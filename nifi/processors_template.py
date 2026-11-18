

# ============================================================================
# NiFi 2.x Python processors for the LM -> FMS locations flow.
# Everything above this banner is sliced verbatim from the Fabric UDF by
# build_nifi.py, so mapping/assembly/classification cannot drift from p9.
# ============================================================================
import os
import uuid as _uuid
from zoneinfo import ZoneInfo

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators
from nifiapi.relationship import Relationship

_CENTRAL = ZoneInfo("America/Chicago")
_VERSION = "0.1.0"

STAGING_DIR = PropertyDescriptor(
    name="Staging Directory",
    description="Folder holding watermark.json (stand-in for the lakehouse staging folder).",
    required=True, validators=[StandardValidators.NON_EMPTY_VALIDATOR])


def _attr(flowfile, name, default=""):
    v = flowfile.getAttribute(name)
    return default if v is None else v


def _fail(message, **extra):
    attrs = {"lm.error": str(message)[:2000]}
    attrs.update(extra)
    return FlowFileTransformResult(relationship="failure", attributes=attrs)


class ComputeLmWindow(FlowFileTransform):
    """ReadWatermark + CheckWatermarkError: emits the window as attributes and as
    sql.args.N.* so ExecuteSQLRecord binds them to the LM query's ? markers."""

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = _VERSION
        description = "Computes the LocationManagement change window from watermark.json (p9 readWatermark)."
        tags = ["mtc", "lm", "fms", "watermark"]

    OVERRIDE = PropertyDescriptor(
        name="Override Begin Ts",
        description="yyyy-MM-dd HH:mm:ss.SSS; replays from here without reading watermark.json.",
        required=False)
    MAX_LOCATIONS = PropertyDescriptor(
        name="Max Locations Per Run", description="TOP (n) cap on the LM read.",
        required=True, default_value="400", validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR])

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return [STAGING_DIR, self.OVERRIDE, self.MAX_LOCATIONS]

    def transform(self, context, flowfile):
        staging = context.getProperty(STAGING_DIR).getValue()
        override = (context.getProperty(self.OVERRIDE).getValue() or "").strip()
        max_locations = context.getProperty(self.MAX_LOCATIONS).getValue()
        from datetime import datetime as _dt
        end_ts = _fmt_ts(_dt.now(_CENTRAL).replace(tzinfo=None))
        try:
            if override:
                if not _valid_ts(override):
                    return _fail("WATERMARK_UNUSABLE: overrideBeginTs %r is not yyyy-MM-dd HH:mm:ss.SSS" % override)
                window = _window(override, end_ts, False)
            else:
                path = os.path.join(staging, WATERMARK_NAME)
                raw = None
                if os.path.exists(path):
                    with open(path, encoding="utf-8") as fh:
                        raw = fh.read()
                window = _compute_window(raw, end_ts)
        except Exception as exc:
            return _fail("WATERMARK_UNUSABLE: watermark read failed: %s" % str(exc)[:300])
        if window["error"]:
            return _fail("WATERMARK_UNUSABLE: %s. LocationManagement was NOT read." % window["error"])
        return FlowFileTransformResult(relationship="success", contents=b"", attributes={
            "lm.runId": _attr(flowfile, "uuid", str(_uuid.uuid4())),
            "lm.beginTs": window["beginTs"],
            "lm.endTs": window["endTs"],
            "lm.beginId": str(window["beginId"]),
            "lm.seeded": str(window["seeded"]).lower(),
            "sql.args.1.type": "12", "sql.args.1.value": window["beginTs"],
            "sql.args.2.type": "12", "sql.args.2.value": window["endTs"],
            "sql.args.3.type": "4", "sql.args.3.value": str(window["beginId"]),
            "sql.args.4.type": "4", "sql.args.4.value": str(max_locations),
        })


class BuildFmsLocationCalls(FlowFileTransform):
    """BuildLocationCalls + CheckBuildError. Input is the LM record set as a JSON
    array whose RequirementsJson column nests each location's requirements."""

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = _VERSION
        description = "Assembles one DB2 compound statement per LM location (p9 buildLocationCalls)."
        tags = ["mtc", "lm", "fms", "db2"]

    FMS_LIBRARY = PropertyDescriptor(name="FMS Library", description="FMS proc library.",
                                     required=True, default_value="FMSLIB",
                                     validators=[StandardValidators.NON_EMPTY_VALIDATOR])
    DEFAULT_PHONE = PropertyDescriptor(name="Default Contact Phone", description="Phone for blank/zero LM phones.",
                                       required=True, default_value="0000000001")
    MAX_LOCATIONS = ComputeLmWindow.MAX_LOCATIONS
    EMPTY = Relationship(name="empty", description="Window held no sendable locations; go straight to finalize.")

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return [self.FMS_LIBRARY, self.DEFAULT_PHONE, self.MAX_LOCATIONS]

    def getRelationships(self):
        return [self.EMPTY]

    def transform(self, context, flowfile):
        try:
            rows = json.loads(flowfile.getContentsAsBytes().decode("utf-8") or "[]")
            if isinstance(rows, dict):
                rows = [rows]
            locations, requirements = [], []
            for row in rows:
                nested = row.pop("RequirementsJson", None)
                locations.append(row)
                if nested:
                    requirements.extend(json.loads(nested))
            # the nested shape cannot truncate requirements apart from locations
            requirements.append({"LocationId": SENTINEL_ID})
            result = _build_calls(json.dumps(locations), json.dumps(requirements),
                                  context.getProperty(self.FMS_LIBRARY).getValue(),
                                  context.getProperty(self.DEFAULT_PHONE).getValue(),
                                  int(context.getProperty(self.MAX_LOCATIONS).getValue()),
                                  _attr(flowfile, "lm.endTs"))
        except Exception as exc:
            return _fail("BUILD_FAILED: %s" % str(exc)[:500])
        if result["error"]:
            return _fail("BUILD_FAILED: %s. Nothing was sent." % result["error"])
        attrs = {
            "lm.callCount": str(result["callCount"]),
            "lm.locationCount": str(result["locationCount"]),
            "lm.requirementCount": str(result["requirementCount"]),
            "lm.capped": str(result["capped"]).lower(),
            "lm.nextBeginTs": result["nextBeginTs"],
            "lm.nextBeginId": str(result["nextBeginId"]),
            "lm.skippedJson": json.dumps(result["skipped"]),
        }
        rel = "success" if result["calls"] else "empty"
        return FlowFileTransformResult(relationship=rel, attributes=attrs,
                                       contents=json.dumps(result["calls"]).encode("utf-8"))


class FinalizeLmLocations(FlowFileTransform):
    """FinalizeLocations: classifies each DB2 SIGNAL text and writes item-log rows as content."""

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = _VERSION
        description = "Classifies FMS outcomes and builds item log + digest (p9 finalizeLocations)."
        tags = ["mtc", "lm", "fms", "logging"]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return []

    def transform(self, context, flowfile):
        run_id = _attr(flowfile, "lm.runId")
        begin_ts, end_ts = _attr(flowfile, "lm.beginTs"), _attr(flowfile, "lm.endTs")
        try:
            merged = json.loads(flowfile.getContentsAsBytes().decode("utf-8") or "[]")
            entries = [{"locationId": e.get("lm.locationId", ""),
                        "requirementCount": int(e.get("lm.requirementCount") or 0),
                        "message": e.get("executesql.error.message", "")}
                       for e in merged if isinstance(e, dict) and "lm.locationId" in e]
            expected = int(_attr(flowfile, "lm.callCount", "0"))
            if len(entries) != expected:
                return _fail("expected %d location outcome(s), got %d -- watermark NOT advanced"
                             % (expected, len(entries)))
            ts = end_ts if _valid_ts(end_ts) else _fmt_ts(datetime.utcnow())
            outcomes = _parse_outcomes(json.dumps(entries), ts)
            skipped = json.loads(_attr(flowfile, "lm.skippedJson", "[]") or "[]")
        except Exception as exc:
            return _fail("FINALIZE_FAILED: %s" % str(exc)[:500])

        pipeline = "lm-to-fms-locations"
        items = [_item_record(run_id, pipeline, "location", o["locationId"], o["outcome"], o["errorCode"],
                              o["errorMessage"], {"requirementCount": o.get("requirementCount"),
                                                  "windowBegin": begin_ts, "windowEnd": end_ts}, o["ts"])
                 for o in outcomes]
        items += [_item_record(run_id, pipeline, "location", _s(s.get("locationId")), _s(s.get("reason")),
                               _s(s.get("reason")), "", {"windowBegin": begin_ts, "windowEnd": end_ts}, ts)
                  for s in skipped if isinstance(s, dict)]
        counts = _summarise(outcomes)
        technical = counts.get("technical", 0)
        digest = _digest_body(run_id, begin_ts, end_ts, outcomes, skipped)
        return FlowFileTransformResult(relationship="success", contents=json.dumps(items).encode("utf-8"),
                                       attributes={
            "filename": "items.json",
            "lm.sent": str(counts.get("sent", 0)),
            "lm.fmsErrors": str(counts.get("fms_error", 0)),
            "lm.technical": str(technical),
            "lm.skipped": str(len(skipped)),
            "lm.hasFailures": str(bool(digest)).lower(),
            "lm.digestBody": digest,
            "lm.nextBeginTs": "" if technical else _attr(flowfile, "lm.nextBeginTs"),
            "lm.nextBeginId": str(NO_BEGIN_ID if technical else _as_begin_id(_attr(flowfile, "lm.nextBeginId"))),
            "lm.watermarkHeld": str(bool(technical)).lower(),
        })


class AdvanceLmWatermark(FlowFileTransform):
    """AdvanceWatermark: the commit point. Blank nextBeginTs (technical failure) routes to failure."""

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = _VERSION
        description = "Writes watermark.json after a run consumed its window (p9 advanceWatermark)."
        tags = ["mtc", "lm", "fms", "watermark"]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return [STAGING_DIR]

    def transform(self, context, flowfile):
        next_ts = _attr(flowfile, "lm.nextBeginTs")
        if not _valid_ts(next_ts):
            return _fail("WATERMARK_ADVANCE_FAILED: nextBeginTs %r unusable (held by a technical failure?); "
                         "the next run re-reads this window" % next_ts)
        doc = {"nextBeginTs": next_ts, "nextBeginId": _as_begin_id(_attr(flowfile, "lm.nextBeginId")),
               "runId": _attr(flowfile, "lm.runId")}
        staging = context.getProperty(STAGING_DIR).getValue()
        try:
            os.makedirs(staging, exist_ok=True)
            tmp = os.path.join(staging, WATERMARK_NAME + ".tmp")
            with open(tmp, "w", encoding="utf-8") as fh:
                fh.write(json.dumps(doc))
            os.replace(tmp, os.path.join(staging, WATERMARK_NAME))
        except Exception as exc:
            return _fail("WATERMARK_ADVANCE_FAILED: %s" % str(exc)[:300])
        return FlowFileTransformResult(relationship="success")
