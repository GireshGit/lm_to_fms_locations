"""Runs the generated NiFi Python processors through one simulated run (stubbed nifiapi):
window -> build -> fake DB2 SIGNAL texts -> finalize -> advance. No NiFi needed."""
import json
import sys
import tempfile
import types
from pathlib import Path

HERE = Path(__file__).resolve().parent
api = types.ModuleType("nifiapi")
for sub in ("flowfiletransform", "properties", "relationship"):
    sys.modules["nifiapi." + sub] = types.ModuleType("nifiapi." + sub)


class Result:
    def __init__(self, relationship, attributes=None, contents=None):
        self.relationship, self.attributes, self.contents = relationship, attributes or {}, contents


class PD:
    def __init__(self, name, description="", required=False, default_value=None, validators=()):
        self.name, self.default = name, default_value


sys.modules["nifiapi.flowfiletransform"].FlowFileTransform = object
sys.modules["nifiapi.flowfiletransform"].FlowFileTransformResult = Result
sys.modules["nifiapi.properties"].PropertyDescriptor = PD
sys.modules["nifiapi.properties"].StandardValidators = types.SimpleNamespace(
    NON_EMPTY_VALIDATOR=1, POSITIVE_INTEGER_VALIDATOR=2)
sys.modules["nifiapi.relationship"].Relationship = lambda name, description: name
sys.modules["nifiapi"] = api

mod = {}
src = HERE.parent / "nifi" / "python_extensions" / "LmFmsLocations.py"
exec(compile(src.read_text(), str(src), "exec"), mod)


class FF:
    def __init__(self, attrs, content=b""):
        self.attrs, self.content = dict(attrs), content

    def getAttribute(self, k):
        return self.attrs.get(k)

    def getContentsAsBytes(self):
        return self.content


class Ctx:
    def __init__(self, values):
        self.values = values

    def getProperty(self, pd):
        v = self.values.get(pd.name, pd.default)
        return types.SimpleNamespace(getValue=lambda: v)


def step(cls, ctx, ff):
    r = cls().transform(Ctx(ctx), ff)
    attrs = dict(ff.attrs, **r.attributes)
    return r.relationship, FF(attrs, ff.content if r.contents is None else r.contents)


fx = json.loads((HERE / "fixtures.json").read_text())
staging = tempfile.mkdtemp()
props = {"Staging Directory": staging, "Max Locations Per Run": "3", "FMS Library": "FMSLIB",
         "Default Contact Phone": "0000000001", "Override Begin Ts": ""}

rel, ff = step(mod["ComputeLmWindow"], props, FF({"uuid": "run-1"}))
assert rel == "success" and ff.attrs["lm.seeded"] == "true", (rel, ff.attrs)

# reshape the two Fabric result sets into the NiFi query's nested shape; pin endTs so the sentinel clock agrees
reqs = {}
for r in fx["requirements"][:-1]:
    reqs.setdefault(r["LocationId"], []).append(r)
rows = [dict(l, RequirementsJson=json.dumps(reqs[l["LocationId"]]) if l["LocationId"] in reqs else None)
        for l in fx["locations"]]
ff.attrs["lm.endTs"] = fx["endTs"]
rel, ff = step(mod["BuildFmsLocationCalls"], props, FF(ff.attrs, json.dumps(rows).encode()))
assert rel == "success", ff.attrs.get("lm.error")
calls = json.loads(ff.content)

# the Fabric UDF on the same input must produce the identical calls
udf = mod["_build_calls"](json.dumps(fx["locations"]), json.dumps(fx["requirements"]), "FMSLIB",
                          "0000000001", 3, fx["endTs"])
assert calls == udf["calls"], "NiFi calls differ from the UDF"

msgs = ["[SQL0438] P9OK", "[SQL0438] P9ERR AU DLS0313 Phone number invalid"]
merged = [{"lm.locationId": c["locationId"], "lm.requirementCount": str(c["requirementCount"]),
           "executesql.error.message": m} for c, m in zip(calls, msgs)]
rel, fin = step(mod["FinalizeLmLocations"], props, FF(ff.attrs, json.dumps(merged).encode()))
assert rel == "success", fin.attrs.get("lm.error")
items = json.loads(fin.content)
print("items:", [(i["item_id"], i["outcome"], i["error_code"]) for i in items])
print("hasFailures:", fin.attrs["lm.hasFailures"], "| nextBeginTs:", fin.attrs["lm.nextBeginTs"],
      fin.attrs["lm.nextBeginId"])

rel, _ = step(mod["AdvanceLmWatermark"], props, fin)
assert rel == "success"
print("watermark.json:", (Path(staging) / "watermark.json").read_text())

# a technical outcome must hold the watermark
merged[0]["executesql.error.message"] = "Communication link failure"
rel, held = step(mod["FinalizeLmLocations"], props, FF(ff.attrs, json.dumps(merged).encode()))
rel2, bad = step(mod["AdvanceLmWatermark"], props, held)
assert held.attrs["lm.watermarkHeld"] == "true" and rel2 == "failure"
# a lost outcome must fail finalize
rel3, _ = step(mod["FinalizeLmLocations"], props, FF(ff.attrs, json.dumps(merged[:1]).encode()))
assert rel3 == "failure"
print("NiFi processor simulation: OK")
