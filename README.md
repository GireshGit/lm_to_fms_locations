# LM → FMS Locations: Kaoto vs NiFi POC

Two ports of the Fabric pipeline `9_pl_LM_To_FMS_Locations` that do the same thing:
- **Kaoto (Apache Camel YAML DSL)**: `kaoto/`
- **Apache NiFi 2.x**: `nifi/`

Both keep the Fabric design: a watermark window, a sentinel-row read of LocationManagement, one DB2
compound statement per location (`ADD_UPDATE_LOCATIONS` followed by the requirements rewrite, ending in
`SIGNAL P9OK/P9ERR`), parallel calls, an item log, a digest mail, and a watermark that is held
whenever a technical failure occurs. Step and processor names reuse the Fabric activity names, so
the three can be compared side by side.

This lives outside `Fabric-Mtc-Integrations/` so Fabric git sync never sees it.

## Layout

| Path | What |
|---|---|
| `kaoto/lm-to-fms-locations.camel.yaml` | The route. Open it in VS Code with the **Kaoto** extension to see the canvas. |
| `kaoto/LmFmsCore.java`, `Json.java` | Java port of the UDF logic (mapping, block assembly, classification, watermark). |
| `kaoto/LmFmsLocations.java` | The Camel bean (`lmFms`). One method per Fabric UDF entry point. |
| `kaoto/sql/*.sql` | The two Fabric LM queries copied verbatim, with `@p` changed to `:#p`. |
| `nifi/LM_To_FMS_Locations.flow.json` | Flow definition. Import it with *Process Group → Upload flow definition*. |
| `nifi/python_extensions/LmFmsLocations.py` | NiFi Python processors. Generated, so do not edit by hand. |
| `nifi/build_nifi.py` | Regenerates both NiFi files. It copies the UDF's core code **verbatim**. |
| `nifi/lm-query.sql` | One LM query that nests the requirements as a `FOR JSON` column. |
| `parity/` | Checks that both ports match the Fabric UDF. |

## Run it

**Kaoto / Camel** needs the Camel JBang CLI (`camel`):
```bash
cd kaoto
export LM_DB_HOST=... LM_DB_USER=... LM_DB_PASSWORD=... FMS_DB_HOST=... FMS_DB_USER=... FMS_DB_PASSWORD=...
export SENDGRID_API_KEY=... MTC_SUPPORT_EMAIL=...
camel run lm-to-fms-locations.camel.yaml *.java application.properties sql/*.sql
```
The watermark, item logs and run logs are written to `kaoto/data/lm2fms/`. To replay a window,
set `lm.overrideBeginTs`. To deploy, use `camel export --runtime=quarkus` (or `spring-boot`) to
get a regular Maven project.

**NiFi**:
1. Put `mssql-jdbc-*.jar` and `jt400-*.jar` in `nifi/drivers/`.
2. Run `docker compose -f nifi/docker-compose.yml up` and open https://localhost:8443/nifi.
3. Drag in a Process Group, choose **Upload flow definition**, and pick `LM_To_FMS_Locations.flow.json`.
4. Fill in the parameter context `lm_to_fms_locations`: JDBC URLs, users, the 3 sensitive passwords, and `mail_to`.
5. Enable the 3 controller services, then start the group.

**End-to-end tests** (everything external is mocked; no databases, NiFi, Java or Maven install needed):
```bash
jbang kaoto/test/LmToFmsRouteTest.java     # Camel route: 8 tests (JUnit 5 + AdviceWith)
python3 nifi/test/test_flow.py -v          # NiFi flow:  16 tests (drives the real flow.json)
```
Demo notes: `kaoto/DEMO_NOTES.md` and `nifi/DEMO_NOTES.md` (same layout, compare side by side).

**Parity checks** (no databases needed):
```bash
python3 nifi/build_nifi.py                 # regenerate after any UDF change
python3 parity/nifi_sim.py                 # NiFi processors over one simulated run
J=~/.jbang/currentjdk/bin; mkdir -p /tmp/p && $J/javac -d /tmp/p kaoto/LmFmsCore.java kaoto/Json.java parity/ParityCheck.java \
  && $J/java -cp /tmp/p ParityCheck parity/fixtures.json > /tmp/p/java.json && python3 parity/parity_check.py /tmp/p/java.json
```

## Status

| | Kaoto/Camel | NiFi |
|---|---|---|
| Logic matches the Fabric UDF | ✅ 14/14 parity keys, including byte-identical DB2 SQL | ✅ same code (copied from the UDF), and the simulated run matches the UDF |
| Started for real | ✅ Camel 4.22 JBang: beans, SQL resources, watermark seed and failure path + run log all ran. Stopped at the LM connection (no DB here). | ❌ Not imported into a live NiFi yet (no Docker on the dev machine). Bundle versions or property names may need fixing on first import. |
| Tested against real LM/FMS | ❌ | ❌ |

## Comparison

| | Kaoto / Camel | NiFi |
|---|---|---|
| **Visual designer** | Kaoto canvas over a YAML file. Diagram and code are the same artifact. | The canvas *is* the runtime. Changes are live, and there is no source file unless you use the Registry or a JSON export. |
| **Source control / PR review** | ✅ Readable YAML and Java diffs, like today's pipeline JSON plus UDF. | ⚠️ Flow JSON is noisy (UUIDs, positions). Needs NiFi Registry or a GitHub flow registry client. |
| **Where the logic lives** | A Java bean, ported from Python (about 450 lines). This is a second copy that must be kept in sync, and `parity/` guards it. | Python processors that reuse the UDF code **verbatim**. Lowest porting cost. |
| **DB2 SIGNAL-as-outcome trick** | `doTry`/`doCatch` around `jdbc:`. Clean. | ExecuteSQL always routes to *failure*, so every location raises an ERROR bulletin. Noisy. |
| **Fan-out and gather** | `split` with a parallel thread pool and grouped aggregation: one step. | SplitJson → EvaluateJsonPath → ExecuteSQL → AttributesToJSON → MergeContent(Defragment): 5 processors. |
| **Error and red-run semantics** | `onException` (mail + fail) and `onCompletion` (run log). This is close to Fabric's Completed/Failed dependencies. | Each failure relationship is wired to `MarkFailed`. More lines on the canvas, but you can see where everything goes. |
| **Watermark** | A file, read in the `readWindow` bean and written with `file:`. Can switch to `azure-storage-datalake` for the lakehouse. | A file written by a Python processor. NiFi state or a map cache would be more idiomatic. |
| **Operations** | A plain JVM app (Quarkus or Spring Boot) for k8s or a container app. Monitored with Hawtio and Micrometer. | A NiFi cluster to run and secure. Provenance, queue inspection and replay are built in, which is very good for support. |
| **Overlapping runs** | `quartz` with `stateful=true`, so runs never overlap. | `GenerateFlowFile` keeps firing, so a run longer than 15 min can overlap the next and read the same window twice. Needs a guard (Wait/Notify or a longer period). |
| **Scale for this job** | ≤400 rows every 15 min. Very small. | Same. NiFi's streaming strengths are not used here. |

**Recommendation:** for request/response-style integrations like this one (poll, transform, call a
stored procedure, log), **Kaoto/Camel** fits better: it is code-reviewable, testable, uses little
infrastructure, and the visual designer is a view of the code, not a replacement for it. Choose
**NiFi** if the team mostly wants operator tools (provenance, replay, queue inspection) and is
willing to run and govern a NiFi cluster. Its Python processors would let most of the existing UDF
code move over unchanged.

## Known gaps vs Fabric (both ports)
- The lakehouse staging folder is a local folder. ETL logging writes JSON files, not the `udf_etl_logging` tables.
- Mail goes through the SendGrid SMTP relay, not the `sendEmail` UDF. Only one subject: there is no warnings mail yet, because `KNOWN_BUSINESS_ERRORS` is empty.
- Camel uses a non-pooled jt400 DataSource, so each call opens a new connection, as the Fabric gateway does. NiFi pools connections.
