"""End-to-end tests for the NiFi flow LM_To_FMS_Locations.flow.json, no NiFi needed.

How it works
------------
The harness reads the real flow.json and follows its connections, so a wiring mistake fails a test.
  * The 4 custom Python processors (ComputeLmWindow, BuildFmsLocationCalls, FinalizeLmLocations,
    AdvanceLmWatermark) run for real from python_extensions/LmFmsLocations.py (nifiapi is stubbed).
  * NiFi's built-in processors are emulated with the properties from flow.json (UpdateAttribute,
    SplitJson, EvaluateJsonPath, AttributesToJSON, MergeContent/Defragment, RouteOnAttribute,
    PutFile, GenerateFlowFile) plus a small Expression Language evaluator for ${..} and #{..}.
  * External systems are fakes: LM (ExecuteSQLRecord) returns canned rows or fails; FMS (ExecuteSQL)
    answers with the SIGNAL text a real DB2 block raises; PutEmail records the mail.
  * Retries follow flow.json (retryCount + retriedRelationships), so "LM down" is retried as configured.
Files (watermark.json, items/, runlog/) are written to a temp folder and checked for real.

Not covered: NiFi's own processor code (real MergeContent binning, the real JDBC driver behaviour).

Run:  python3 nifi/test/test_flow.py        (add -v for test names)
"""
import json
import os
import re
import sys
import tempfile
import types
import unittest
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

NIFI = Path(__file__).resolve().parents[1]
FLOW = NIFI / "LM_To_FMS_Locations.flow.json"
EXT = NIFI / "python_extensions" / "LmFmsLocations.py"

OK = "[SQL0438] P9OK"
REFUSED = "[SQL0438] P9ERR AU DLS0313 Phone number invalid"
LINK_DOWN = "Communication link failure"
UNSIGNALLED = None          # the block ran without SIGNAL: ExecuteSQL routes to success

# ---------------------------------------------------------------- load the Python processors (stub nifiapi)


def _load_extension():
    class Result:
        def __init__(self, relationship, attributes=None, contents=None):
            self.relationship, self.attributes, self.contents = relationship, attributes or {}, contents

    class PD:
        def __init__(self, name, description="", required=False, default_value=None, validators=()):
            self.name, self.default = name, default_value

    for sub in ("flowfiletransform", "properties", "relationship"):
        sys.modules["nifiapi." + sub] = types.ModuleType("nifiapi." + sub)
    sys.modules["nifiapi"] = types.ModuleType("nifiapi")
    sys.modules["nifiapi.flowfiletransform"].FlowFileTransform = object
    sys.modules["nifiapi.flowfiletransform"].FlowFileTransformResult = Result
    sys.modules["nifiapi.properties"].PropertyDescriptor = PD
    sys.modules["nifiapi.properties"].StandardValidators = types.SimpleNamespace(
        NON_EMPTY_VALIDATOR=1, POSITIVE_INTEGER_VALIDATOR=2)
    sys.modules["nifiapi.relationship"].Relationship = lambda name, description: name
    mod = {"__name__": "LmFmsLocations"}
    exec(compile(EXT.read_text(), str(EXT), "exec"), mod)
    return mod


EXTENSION = _load_extension()

# ---------------------------------------------------------------- FlowFile + Expression Language


class FlowFile:
    def __init__(self, attrs=None, content=b""):
        self.attrs = dict(attrs or {})
        self.content = content if isinstance(content, bytes) else content.encode("utf-8")

    def clone(self):
        return FlowFile(self.attrs, self.content)

    # nifiapi FlowFile surface used by the Python processors
    def getAttribute(self, name):
        return self.attrs.get(name)

    def getContentsAsBytes(self):
        return self.content


def evaluate(text, attrs, params):
    """#{param} and ${attr[:equals('x')][:replaceEmpty(arg)]}; arg is a quoted literal or a nested ${..}."""
    if text is None:
        return None
    text = re.sub(r"#\{([^}]+)\}", lambda m: params.get(m.group(1)) or "", text)
    out, i = [], 0
    while i < len(text):
        if text.startswith("${", i):
            value, i = _expr(text, i + 2, attrs)
            out.append(value)
        else:
            out.append(text[i])
            i += 1
    return "".join(out)


def _expr(text, i, attrs):
    j = i
    while text[j] not in ":}":
        j += 1
    value = attrs.get(text[i:j]) or ""
    i = j
    while text[i] == ":":
        name_end = text.index("(", i)
        fn = text[i + 1:name_end]
        arg, i = _arg(text, name_end + 1, attrs)
        if fn == "equals":
            value = "true" if value == arg else "false"
        elif fn == "replaceEmpty":
            value = value if value.strip() else arg
        else:
            raise NotImplementedError("EL function %s" % fn)
        assert text[i] == ")", text[i:]
        i += 1
    assert text[i] == "}", text[i:]
    return value, i + 1


def _arg(text, i, attrs):
    if text[i] == "'":
        end = text.index("'", i + 1)
        return text[i + 1:end], end + 1
    if text.startswith("${", i):
        return _expr(text, i + 2, attrs)
    raise ValueError("unsupported EL argument at %r" % text[i:i + 20])


# ---------------------------------------------------------------- flow runner


class Ctx:
    def __init__(self, props, params):
        self.props, self.params = props, params

    def getProperty(self, pd):
        raw = self.props.get(pd.name, pd.default)
        v = evaluate(raw, {}, self.params) if isinstance(raw, str) else raw
        return types.SimpleNamespace(getValue=lambda: v)


class WiringError(AssertionError):
    pass


class FlowRun:
    """One scheduled run of the flow: Schedule fires once, FlowFiles move along flow.json's connections."""

    def __init__(self, staging, lm_rows, fms_answers, lm_down=False, smtp_down=False, params=None):
        flow = json.loads(FLOW.read_text())
        pg = flow["flowContents"]
        self.procs = {p["name"]: p for p in pg["processors"]}
        self.routes = {}
        for c in pg["connections"]:
            for rel in c["selectedRelationships"]:
                self.routes.setdefault((c["source"]["name"], rel), []).append(c["destination"]["name"])
        ctx_name = pg["parameterContextName"]
        self.params = {p["name"]: p["value"] for p in flow["parameterContexts"][ctx_name]["parameters"]}
        self.params.update({"staging_dir": str(staging), "mail_to": "support@example.test"})
        self.params.update(params or {})
        self.lm_rows, self.fms_answers = lm_rows, fms_answers
        self.lm_down, self.smtp_down = lm_down, smtp_down
        self.calls = {}                   # processor name -> number of times it ran
        self.mails = []                   # (processor, subject, message)
        self.fms_sql = []
        self.bins = {}                    # MergeContent fragments by fragment.identifier

    # -- engine -------------------------------------------------------------------------
    def run(self):
        queue = [("Schedule", FlowFile())]
        while queue:
            name, ff = queue.pop(0)
            proc = self.procs[name]
            attempts = 1 + (proc["retryCount"] if proc["retriedRelationships"] else 0)
            for _ in range(attempts):
                self.calls[name] = self.calls.get(name, 0) + 1
                outputs = self._execute(proc, ff.clone())
                if not any(rel in proc["retriedRelationships"] for rel, _ in outputs):
                    break
            for rel, out in outputs:
                queue.extend(self._route(proc, rel, out))
        for fid, frags in self.bins.items():          # an incomplete bin ages out to failure
            raise AssertionError("MergeContent bin %s never completed (%d fragments)" % (fid, len(frags)))
        return self

    def _route(self, proc, rel, ff):
        dests = self.routes.get((proc["name"], rel))
        if dests:
            return [(d, ff.clone()) for d in dests]
        if rel in proc["autoTerminatedRelationships"]:
            return []
        raise WiringError("%s -> %r is neither connected nor auto-terminated" % (proc["name"], rel))

    def _execute(self, proc, ff):
        kind = proc["type"].rsplit(".", 1)[-1]
        handler = getattr(self, "_p_" + kind, None)
        if handler is None and kind in EXTENSION:
            return self._python(proc, ff)
        if handler is None:
            raise NotImplementedError("no emulation for processor type " + kind)
        return handler(proc, ff)

    def _prop(self, proc, key, ff=None):
        return evaluate(proc["properties"].get(key), ff.attrs if ff else {}, self.params)

    # -- the real Python processors -------------------------------------------------------
    def _python(self, proc, ff):
        kind = proc["type"].rsplit(".", 1)[-1]
        result = EXTENSION[kind]().transform(Ctx(proc["properties"], self.params), ff)
        out = FlowFile(dict(ff.attrs, **result.attributes),
                       ff.content if result.contents is None else result.contents)
        return [(result.relationship, out), ("original", ff)]

    # -- emulated NiFi built-ins ----------------------------------------------------------
    def _p_GenerateFlowFile(self, proc, ff):
        return [("success", FlowFile({"uuid": str(uuid.uuid4())}))]

    def _p_ExecuteSQLRecord(self, proc, ff):          # LookupLocationChanges -> LocationManagement
        assert ff.attrs.get("sql.args.1.value"), "window not bound to the LM query"
        if self.lm_down:
            ff.attrs["executesqlrecord.error.message"] = "Cannot get a connection, pool error"
            return [("failure", ff)]
        return [("success", FlowFile(ff.attrs, json.dumps(self.lm_rows())))]

    def _p_ExecuteSQL(self, proc, ff):                # CallFmsLocation -> FMS DB2
        sql = self._prop(proc, "SQL select query", ff)
        assert sql and sql.strip(), "empty DB2 statement for location %s" % ff.attrs.get("lm.locationId")
        self.fms_sql.append(sql)
        answer = self.fms_answers.get(ff.attrs.get("lm.locationId"), OK)
        if answer is UNSIGNALLED:
            return [("success", ff)]
        ff.attrs["executesql.error.message"] = answer   # a SIGNAL always lands on failure
        return [("failure", ff)]

    def _p_UpdateAttribute(self, proc, ff):
        attrs = dict(ff.attrs)
        for key, raw in proc["properties"].items():
            if key == "Delete Attributes Expression":
                attrs = {k: v for k, v in attrs.items() if not re.fullmatch(raw, k)}
            else:
                attrs[key] = evaluate(raw, ff.attrs, self.params)
        return [("success", FlowFile(attrs, ff.content))]

    def _p_SplitJson(self, proc, ff):
        assert proc["properties"]["JsonPath Expression"] == "$.*"
        items, fid = json.loads(ff.content), str(uuid.uuid4())
        out = [("split", FlowFile(dict(ff.attrs, **{"fragment.identifier": fid, "fragment.index": str(i),
                                                    "fragment.count": str(len(items))}), json.dumps(it)))
               for i, it in enumerate(items)]
        return out + [("original", ff)]

    def _p_EvaluateJsonPath(self, proc, ff):
        doc = json.loads(ff.content)
        for key, path in proc["properties"].items():
            if path.startswith("$."):
                if path[2:] not in doc:
                    return [("unmatched", ff)]
                ff.attrs[key] = str(doc[path[2:]])
        return [("matched", ff)]

    def _p_AttributesToJSON(self, proc, ff):
        p = proc["properties"]
        if "Attributes List" in p:
            null_as_empty = p.get("Null Value") == "false"
            doc = {}
            for k in p["Attributes List"].split(","):
                v = ff.attrs.get(k)
                doc[k] = "" if v is None and null_as_empty else v
        else:
            rx = re.compile(p["Attributes Regular Expression"])
            doc = {k: v for k, v in ff.attrs.items() if rx.fullmatch(k)}
        return [("success", FlowFile(ff.attrs, json.dumps(doc)))]

    def _p_MergeContent(self, proc, ff):
        p = proc["properties"]
        assert p["Merge Strategy"] == "Defragment"
        fid = ff.attrs["fragment.identifier"]
        frags = self.bins.setdefault(fid, [])
        frags.append(ff)
        if len(frags) < int(ff.attrs["fragment.count"]):
            return []
        del self.bins[fid]
        frags.sort(key=lambda f: int(f.attrs["fragment.index"]))
        common = {k: v for k, v in frags[0].attrs.items() if all(f.attrs.get(k) == v for f in frags)}
        body = p["Header File"] + p["Demarcator File"].join(f.content.decode() for f in frags) + p["Footer File"]
        return [("merged", FlowFile(common, body))] + [("original", f) for f in frags]

    def _p_RouteOnAttribute(self, proc, ff):
        out = [(k, ff) for k, v in proc["properties"].items()
               if k != "Routing Strategy" and evaluate(v, ff.attrs, self.params) == "true"]
        return out or [("unmatched", ff)]

    def _p_PutFile(self, proc, ff):
        folder = Path(self._prop(proc, "Directory", ff))
        folder.mkdir(parents=True, exist_ok=True)
        (folder / ff.attrs.get("filename", ff.attrs.get("uuid", "flowfile"))).write_bytes(ff.content)
        return [("success", ff)]

    def _p_PutEmail(self, proc, ff):
        if self.smtp_down:
            return [("failure", ff)]
        self.mails.append((proc["name"], self._prop(proc, "Subject", ff), self._prop(proc, "Message", ff)))
        return [("success", ff)]


# ---------------------------------------------------------------- the tests


def central_now(delta_minutes=0):
    dt = datetime.now(ZoneInfo("America/Chicago")).replace(tzinfo=None) + timedelta(minutes=delta_minutes)
    return dt.strftime("%Y-%m-%d %H:%M:%S.") + "%03d" % (dt.microsecond // 1000)


def location(loc_id, name, changed_ts, requirements=None):
    return {"LocationId": loc_id, "LocationName": name, "StreetAddress1": "100 Main St", "StreetAddress2": None,
            "City": "Chicago", "State": "IL", "Country": "US", "PostalCode": "60601", "Phone": "312-555-0100",
            "ContactName": "Dock Lead", "Fax": "", "Status": "1", "LocationCategoryName": "TERMINAL",
            "LocationCategoryDesc": "Rail terminal", "ParentId": None, "FacilityDropsite": "DS01",
            "FacilityLocaleKey": "LOC" + loc_id, "ChangedTs": changed_ts,
            "RequirementsJson": json.dumps(requirements) if requirements else None}


class FlowTest(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.staging = Path(self._tmp.name)
        self.fms_answers = {}
        self.locations = ["1234", "5678"]

    def tearDown(self):
        self._tmp.cleanup()

    def lm_rows(self):
        """The NiFi LM query: one record per location (requirements nested), then the '-1' clock sentinel."""
        rows = [location(i, "Yard " + i, central_now(-5),
                         [{"LocationId": i, "ReqType": "O", "ReqText": "Gate code:4471", "ReqSeq": "1"}]
                         if i == "1234" else None) for i in self.locations]
        sentinel = {k: "" for k in rows[0]} if rows else {}
        sentinel.update({"LocationId": "-1", "ChangedTs": central_now(), "RequirementsJson": None})
        return rows + [sentinel]

    def run_flow(self, **kw):
        self.run_ = FlowRun(self.staging, self.lm_rows, self.fms_answers, **kw).run()
        return self.run_

    # helpers over the files the flow wrote
    def run_log(self):
        logs = list((self.staging / "runlog").glob("*.json"))
        self.assertEqual(1, len(logs), "one run log per run")
        return json.loads(logs[0].read_text())

    def watermark(self):
        return json.loads((self.staging / "watermark.json").read_text())

    def items(self):
        files = list((self.staging / "items").glob("*/items.json"))
        self.assertEqual(1, len(files))
        return [i["outcome"] for i in json.loads(files[0].read_text())]

    def mails(self, proc):
        return [m for m in self.run_.mails if m[0] == proc]

    def write_watermark(self, text):
        (self.staging / "watermark.json").write_text(text)
        return text

    # -- scenarios, same as the Camel route tests ---------------------------------------
    def test_all_sent_advances_watermark_no_mail(self):
        self.run_flow()
        log = self.run_log()
        self.assertEqual("Succeeded", log["lm.status"], log)
        self.assertEqual("2", log["lm.sent"])
        self.assertEqual(log["lm.endTs"], self.watermark()["nextBeginTs"])
        self.assertEqual(["sent", "sent"], self.items())
        self.assertEqual([], self.run_.mails)

    def test_fms_refusal_advances_watermark_sends_digest(self):
        self.fms_answers["5678"] = REFUSED
        self.run_flow()
        log = self.run_log()
        self.assertEqual("Succeeded", log["lm.status"], log)
        self.assertEqual("1", log["lm.fmsErrors"])
        self.assertEqual(log["lm.endTs"], self.watermark()["nextBeginTs"], "a business refusal still advances")
        digest = self.mails("SendDigestEmail")
        self.assertEqual(1, len(digest))
        self.assertTrue(digest[0][1].startswith("BUSINESS:"), digest[0][1])
        self.assertIn("5678", digest[0][2])
        self.assertEqual([], self.mails("SendUpstreamFailureEmail"))

    def test_technical_failure_holds_watermark_sends_failure_mail(self):
        before = self.write_watermark(json.dumps({"nextBeginTs": central_now(-10), "nextBeginId": -1}))
        self.fms_answers["5678"] = LINK_DOWN
        self.run_flow()
        log = self.run_log()
        self.assertEqual("Failed", log["lm.status"], log)
        self.assertIn("WATERMARK_ADVANCE_FAILED", log["lm.error"])
        self.assertEqual(before, (self.staging / "watermark.json").read_text(), "watermark must not move")
        failure = self.mails("SendUpstreamFailureEmail")
        self.assertEqual(1, len(failure))
        self.assertIn("FAILED (watermark held)", failure[0][1])
        self.assertEqual([], self.mails("SendDigestEmail"), "same gap as Camel: no digest on technical")
        self.assertEqual(2, len(self.run_.fms_sql), "both locations were still attempted")

    def test_bad_watermark_fails_before_reading_lm(self):
        self.write_watermark('{"nextBeginTs":""}')
        self.run_flow()
        log = self.run_log()
        self.assertEqual("Failed", log["lm.status"])
        self.assertIn("WATERMARK_UNUSABLE", log["lm.error"])
        self.assertNotIn("LookupLocationChanges", self.run_.calls, "LM must not be read")
        self.assertEqual([], self.run_.fms_sql)
        self.assertEqual(1, len(self.mails("SendUpstreamFailureEmail")))

    def test_lm_down_is_retried_then_fails(self):
        self.run_flow(lm_down=True)
        log = self.run_log()
        self.assertEqual("Failed", log["lm.status"])
        self.assertEqual(6, self.run_.calls["LookupLocationChanges"], "1 attempt + 5 retries, as Fabric")
        self.assertEqual([], self.run_.fms_sql)
        self.assertFalse((self.staging / "watermark.json").exists())
        self.assertEqual(1, len(self.mails("SendUpstreamFailureEmail")))

    def test_override_replays_window_without_reading_watermark(self):
        self.write_watermark("not json at all")                        # would fail if read
        start = central_now(-120)
        self.run_flow(params={"override_begin_ts": start})
        log = self.run_log()
        self.assertEqual("Succeeded", log["lm.status"], log)
        self.assertEqual(start, log["lm.beginTs"])
        self.assertEqual(2, len(self.run_.fms_sql))

    # -- NiFi-specific paths ------------------------------------------------------------
    def test_no_changes_skips_fms_and_advances(self):
        self.locations = []
        self.run_flow()
        log = self.run_log()
        self.assertEqual("Succeeded", log["lm.status"], log)
        self.assertNotIn("CallFmsLocation", self.run_.calls, "'empty' goes straight to finalize")
        self.assertEqual(log["lm.endTs"], self.watermark()["nextBeginTs"])

    def test_unsignalled_block_is_technical(self):
        self.fms_answers["1234"] = UNSIGNALLED
        self.run_flow()
        self.assertEqual("Failed", self.run_log()["lm.status"])
        self.assertFalse((self.staging / "watermark.json").exists(), "held")

    def test_digest_smtp_down_still_succeeds(self):
        self.fms_answers["5678"] = REFUSED
        self.run_flow(smtp_down=True)
        self.assertEqual("Succeeded", self.run_log()["lm.status"],
                         "PutEmail failure is auto-terminated: the run is not marked failed")
        self.assertTrue((self.staging / "watermark.json").exists())


class FlowDefinitionTest(unittest.TestCase):
    """Static checks on flow.json: schedule, timeouts, retries and wiring the runtime tests rely on."""

    @classmethod
    def setUpClass(cls):
        flow = json.loads(FLOW.read_text())["flowContents"]
        cls.procs = {p["name"]: p for p in flow["processors"]}
        cls.edges = {(c["source"]["name"], r, c["destination"]["name"])
                     for c in flow["connections"] for r in c["selectedRelationships"]}

    def test_schedule_every_15_min_9_to_5(self):
        s = self.procs["Schedule"]
        self.assertEqual(("CRON_DRIVEN", "0 0/15 9-16 * * ?", "PRIMARY"),
                         (s["schedulingStrategy"], s["schedulingPeriod"], s["executionNode"]))

    def test_timeouts_match_fabric(self):
        self.assertEqual("3 min", self.procs["CallFmsLocation"]["properties"]["Max Wait Time"])
        self.assertEqual("20 min", self.procs["LookupLocationChanges"]["properties"]["Max Wait Time"])

    def test_retries_match_fabric(self):
        lookup = self.procs["LookupLocationChanges"]
        self.assertEqual((5, ["failure"]), (lookup["retryCount"], lookup["retriedRelationships"]))
        self.assertEqual([], self.procs["CallFmsLocation"]["retriedRelationships"], "FMS calls: retry 0")

    def test_every_failure_reaches_mark_failed(self):
        for name in ("ReadWatermark", "LookupLocationChanges", "BuildLocationCalls", "SplitPerLocation",
                     "GatherOutcomes", "FinalizeLocations", "WriteItemLog", "AdvanceWatermark"):
            self.assertIn((name, "failure", "MarkFailed"), self.edges, name)

    def test_success_only_after_watermark_advanced(self):
        into = {(s, r) for s, r, d in self.edges if d == "MarkSucceeded"}
        self.assertEqual({("AdvanceWatermark", "success")}, into)

    def test_fms_outcome_recorded_on_both_relationships(self):
        self.assertIn(("CallFmsLocation", "success", "RecordLocationOutcome"), self.edges)
        self.assertIn(("CallFmsLocation", "failure", "RecordLocationOutcome"), self.edges)

    def test_python_processors_exist_in_extension(self):
        for p in self.procs.values():
            kind = p["type"].rsplit(".", 1)[-1]
            if "." not in p["type"]:
                self.assertIn(kind, EXTENSION, "flow uses %s but LmFmsLocations.py has no such class" % kind)


if __name__ == "__main__":
    unittest.main()
