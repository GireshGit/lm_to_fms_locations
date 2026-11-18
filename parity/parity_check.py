"""Diffs the Kaoto/Camel Java port (LmFmsCore) against the Fabric UDF on fixtures.json.
Usage: python3 parity_check.py <java-output.json>"""
import json
import os
import sys
import types
from pathlib import Path

HERE = Path(__file__).resolve().parent
# The Fabric UDF lives in the MTCIntegrations repo; default assumes it is a sibling folder.
MTC_REPO = Path(os.environ.get("MTC_REPO", HERE.parents[1] / "MTCIntegrations")).resolve()
UDF = MTC_REPO / "Fabric-Mtc-Integrations" / "9 - LM_To_FMS_Locations" / \
    "udf_lm_fms_locations.UserDataFunction" / "function_app.py"

fabric = types.ModuleType("fabric")
fn = types.ModuleType("fabric.functions")


class _UDF:
    def function(self, *a, **k):
        return lambda f: f

    def connection(self, *a, **k):
        return lambda f: f


fn.UserDataFunctions = _UDF
fn.FabricLakehouseClient = object
fabric.functions = fn
sys.modules["fabric"] = fabric
sys.modules["fabric.functions"] = fn
app = {}
exec(compile(UDF.read_text(), str(UDF), "exec"), app)

f = json.loads((HERE / "fixtures.json").read_text())
end = f["endTs"]
b = app["_build_calls"](json.dumps(f["locations"]), json.dumps(f["requirements"]), f["fmsLibrary"],
                        f["defaultPhone"], f["maxLocations"], end)
py = {"build." + k: b[k] for k in ("calls", "skipped", "locationCount", "requirementCount", "maxTs",
                                     "serverTs", "capped", "bytes", "error", "callCount",
                                     "nextBeginTs", "nextBeginId")}
wins = []
for w in f["watermarks"]:
    win = app["_compute_window"](w or None, end)
    win["error"] = "ERR" if win["error"] else ""
    wins.append(win)
py["windows"] = wins
py["classify"] = [list(app["classify_outcome"](m)) for m in f["messages"]]

java = json.loads(Path(sys.argv[1]).read_text())
bad = 0
for k in py:
    if py[k] != java.get(k):
        bad += 1
        print("MISMATCH", k)
        print("  udf :", json.dumps(py[k])[:600])
        print("  java:", json.dumps(java.get(k))[:600])
print("parity: %d/%d keys match" % (len(py) - bad, len(py)))
sys.exit(1 if bad else 0)
