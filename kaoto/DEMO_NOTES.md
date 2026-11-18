# Kaoto demo notes — LM → FMS Locations (p9) compared with Fabric

These notes are for presenting the Kaoto/Camel port of `9_pl_LM_To_FMS_Locations`.
Checked on 14 Nov 2026 against the files in this folder.

**What "Fabric" means here.** The Fabric column is the pipeline definition in the repo, plus how PROD behaved in
the November alerts. PROD runs a **newer** UDF than the repo: the PROD traceback pointed at line 1019 of
`function_app.py`, and the repo file has 908 lines. Its alert mails also look different (incident cards). Say this
up front if anyone asks whether this is "the PROD version".

---

## 1. The 30-second pitch

> "This is the same p9 job — LocationManagement changes pushed to FMS every 15 minutes — rebuilt as an Apache Camel
> route. Kaoto is the visual designer. The diagram *is* the YAML file, so what you see on the canvas is exactly what
> runs, and it diffs cleanly in a pull request. The business logic (field mapping, the DB2 statement per location,
> classifying results, the watermark) was ported from the Fabric UDF. A parity test shows it produces
> byte-identical DB2 SQL and the same results on test data. It hasn't run against real LM or FMS yet."

---

## 2. What is in this folder

| File | What it is | Fabric equivalent |
|---|---|---|
| `lm-to-fms-locations.camel.yaml` | The route. Open it in VS Code with the **Kaoto** extension to see the canvas. | The pipeline (`pipeline-content.json`) |
| `LmFmsLocations.java` | The Camel **bean** `lmFms`: one method per step (readWindow, buildCalls, outcome, finalizeLocations, advanceWatermark, failureMail, runLog) | The UDF entry points (`readWatermark`, `buildLocationCalls`, `finalizeLocations`, `advanceWatermark`) |
| `LmFmsCore.java` | The ported logic: mapping, DB2 statement assembly, classification, watermark | The core of `udf_lm_fms_locations/function_app.py` |
| `Json.java` | A small JSON reader/writer, so the bean needs no extra libraries | — (Python has `json` built in) |
| `sql/lm-locations.sql`, `sql/lm-requirements.sql` | The two LM queries, copied from the Fabric Script activity; `@param` became `:#param` | `LookupLocationChanges` (one Script activity, two result sets) |
| `application.properties` | Every setting: schedule, limits, connections, mail | Pipeline parameters + Variable Library + connections |

---

## 3. Where every value comes from

Camel resolves `{{name}}` placeholders from `application.properties` when the app starts.
`{{env:LM_DB_HOST:localhost}}` means: use the environment variable `LM_DB_HOST`, or `localhost` if it isn't set.
A value can also be overridden at start-up with `camel run … --property lm.maxLocationsPerRun=50` or
`-Dlm.maxLocationsPerRun=50`.

| Setting | Property | Value / source | Fabric equivalent |
|---|---|---|---|
| Schedule | `lm.cron` | `0 0/15 9-16 * * ?` (every 15 min, 09:00–16:45) | Schedule trigger, every 15 min |
| Max locations per run | `lm.maxLocationsPerRun` | 400 | parameter `max_locations_per_run` |
| FMS library | `fms.library` | FMSLIB | parameter `fms_library` |
| Phone for blank or zero phones | `fms.defaultContactPhone` | 0000000001 | parameter `default_contact_phone` |
| FMS calls in parallel | `fms.parallelism` | 12 (fixed thread pool `fmsPool`) | ForEach `batchCount` 12 |
| Replay start time | `lm.overrideBeginTs` | blank | parameter `overrideBeginTs` |
| Watermark, item logs, run logs | `lm.staging.dir` | `./data/lm2fms` (local disk) | lakehouse `Files/mtc/staging/lm2fms` |
| LM database | `lm.db.*` | env `LM_DB_HOST`, `LM_DB_PORT` (1433), `LM_DB_NAME` (LocationManagement), `LM_DB_USER`, `LM_DB_PASSWORD`, `LM_DB_TRUST_CERT` | LM SQL Server connection in Fabric |
| FMS DB2 | `fms.db.*` | env `FMS_DB_HOST`, `FMS_DB_USER`, `FMS_DB_PASSWORD` (jt400 driver, `naming=system`) | ODBC connection `0a9acb5b…` through the on-prem gateway |
| Mail | `mail.*` | `smtp.sendgrid.net:587`, user `apikey`, password = env `SENDGRID_API_KEY`, to = env `MTC_SUPPORT_EMAIL` | `sendEmail` UDF + Variable Library (SendGrid key, support list) |
| Driver jars | `camel.jbang.dependencies` | mssql-jdbc 12.8.1, jt400 20.0.7 (downloaded on first run) | The gateway's drivers |

**Beans** (the top of the YAML, in the **Beans** section of Kaoto):
- `lmFms` → the Java class `LmFmsLocations`.
- `lmDataSource` → SQL Server connection, built from `lm.db.*`.
- `fmsDataSource` → DB2 for i connection, built from `fms.db.*`. It does **not** pool connections, so each call
  opens a new one, as the Fabric gateway does.
- `fmsPool` → 12 threads for the parallel FMS calls.

**Replay difference — say this clearly.** In Fabric, `overrideBeginTs` applies to **one run** because it is a
run parameter. In Camel, `lm.overrideBeginTs` is a property, so it applies to **every** run until you clear it
and restart. If you leave it set, every 15 minutes the app re-sends everything from that time onwards.

---

## 4. How data moves inside a Camel run (explain this once)

One run = one **exchange**: a message travelling through the steps. It has three places to keep data:

| Part | What it holds here | Rough Fabric equivalent |
|---|---|---|
| **Body** | The current payload: the LM rows, then the list of calls, then the results, then JSON to write | the activity Output that the next activity reads |
| **Headers** | Values that components read: `beginTs`, `endTs`, `beginId`, `maxLocations` for the SQL; `CamelFileName` for the file writer; `subject` for the mail | dynamic content in an activity's settings |
| **Exchange properties** | Values kept for the whole run: `runId`, `beginTs`, `endTs`, `nextBeginTs`, `callCount`, `digestBody`, `hasFailures`, … | pipeline variables / `activity('X').output` |

`runId` is a random GUID (`${uuid(random)}`), the same format as the Fabric pipeline `RunId`.

---

## 5. Step by step (the Kaoto step IDs match the Fabric activity names)

| # | Kaoto step (id) | Camel building block | What it does | Fabric activity | Difference |
|---|---|---|---|---|---|
| 0 | `Schedule` | `quartz:lm2fms` | Starts a run every 15 min on the clock (:00/:15/:30/:45) from 09:00 to 16:45. `stateful=true` stops runs overlapping; a fire that comes while a run is still going waits for it to finish. | Schedule trigger + `concurrency 1` | Same behaviour. |
| 1 | `setProperty runId` | setProperty | `runId = ${uuid(random)}` | `pipeline().RunId` | Same GUID format. |
| 2 | `ReadWatermark` | bean `lmFms.readWindow` | Reads `watermark.json` from `lm.staging.dir` (skipped when the override is set) and builds the window. A missing file seeds; any other read error fails the run. `endTs` = now in Central time. `beginTs` = the override if set; otherwise the watermark's `nextBeginTs`; with no file, 30 min back. Puts `beginTs`, `endTs`, `beginId` and `maxLocations` into **headers** for the SQL, and everything into properties. | `ReadWatermark` (UDF `readWatermark`, retry 5) | endTs is the current clock time; Fabric uses the trigger time. A failed file read is retried 5 times, 30 s apart. |
| 3 | `CheckWatermarkError` → `FailOnWatermarkError` | choice + throwException | If `watermarkError` is not empty, throws `WATERMARK_UNUSABLE: …` → the failure path (section 6). | `CheckWatermarkError` → `FailOnWatermarkError` | Same. |
| 4 | `LookupLocations` | `sql:classpath:sql/lm-locations.sql`, `outputType=SelectList` | Runs the location query. `:#beginTs`, `:#endTs`, `:#beginId`, `:#maxLocations` come from the headers. The result is a list of rows plus the sentinel row `-1` that carries the LM server clock. Saved to property `lmLocations`. | `LookupLocationChanges`, result set 1 | Fabric runs both queries in **one** Script call; Camel runs two separate calls a moment apart. 20 min timeout (`template.queryTimeout=1200`), 5 retries 30 s apart, as Fabric. |
| 5 | `LookupRequirements` | `sql:classpath:sql/lm-requirements.sql` | Requirements for the same window (same headers), also ending in a `-1` sentinel row. Left in the body. | result set 2 | As above. |
| 6 | `BuildLocationCalls` | bean `lmFms.buildCalls` | Checks both sentinels and the clock difference (≤ 20 min), maps each location to the 20 `ADD_UPDATE_LOCATIONS` parameters, and builds **one DB2 statement per location**. Skips location IDs longer than 7 digits (`location_id_unmappable`). Calculates `nextBeginTs`/`nextBeginId`. Body becomes the list of calls. | `BuildLocationCalls` (UDF) | Same logic: the parity test shows byte-identical SQL. |
| 7 | `CheckBuildError` → `FailOnBuildError` | choice + throwException | If `buildError` is not empty, throws `BUILD_FAILED: …`. | `CheckBuildError` → `FailOnBuildError` | Same. |
| 8 | `CallFmsLocations` | **split** (parallel, `fmsPool`, `GroupedBodyAggregationStrategy`) | One branch per location, up to 12 at once. Waits for all of them, then puts the results back together as a list. | ForEach `CallFmsLocations` (batchCount 12) | Same idea. |
| 9a | `CallFmsLocation` (doTry) → `ExecuteDb2Block` | `jdbc:fmsDataSource` | Sends the location's DB2 statement. It **always** ends with `SIGNAL 75005 'P9OK'` or `'P9ERR …'`, so the driver always throws an SQLException and the result text is inside it. | `CallFmsLocation` Lookup (timeout 3 min, retry 0) | 3 min timeout (`statement.queryTimeout=180`), no retry, as Fabric. A timeout is caught by the doCatch and classed `technical`. |
| 9b | `RecordLocationOutcome` (doCatch) | bean `lmFms.outcome` | Takes the SQLException message as the result: `{locationId, requirementCount, message}`. | `RecordLocationOutcome` (AppendVariable with base64 of the error message) | Same information; no base64 needed. |
| 9c | `RecordUnsignalledOutcome` | bean `lmFms.outcome` | Only reached if the call returned **without** an error, which should never happen. Message is blank, so it counts as `technical`. | (blank message in Fabric) | Same result. |
| 9 | `FinalizeLocations` | bean `lmFms.finalizeLocations` | Checks that results = calls (otherwise throws: an outcome was lost). Classifies each result: `P9OK` → `sent`, `P9ERR` → `fms_error`, anything else → `technical`. Builds the item rows and digest text. **Any technical result blanks `nextBeginTs`**, which holds the watermark. | `FinalizeLocations` (UDF) | Same logic. |
| 10 | `WriteItemLog` | `file:` | Writes `items/<runId>/items.json`, one row per location. | Written inside the Fabric UDF to the lakehouse | Local file; doesn't go to `etl_item_log`. |
| 11 | `AdvanceWatermark` | bean `lmFms.advanceWatermark` | Builds `{"nextBeginTs", "nextBeginId", "runId"}`. If `nextBeginTs` is blank (held), **throws** → failure path. | `AdvanceWatermark` (retry 5) | Same rule. A held watermark is not retried (it would fail the same way); a failed watermark write is. |
| 12 | `WriteWatermark` | `file:` `fileName=watermark.json`, `fileExist=Override` | Saves the new watermark. This is the commit point. | inside `advanceWatermark` | Local file. |
| 13 | `CheckDigestEmail` → `SendDigestEmail` | choice + `smtp:` | If `hasFailures`, mails `BUSINESS: Camel: lm-to-fms-locations - N location(s) refused by FMS - run <id>` with the digest text. | `CheckDigestEmail` → `SendDigestEmail` | Fabric checks the digest and the watermark **in parallel**; Camel advances the watermark first, then mails. PROD now sends incident-style mails, not this subject. |
| — | `InvokeLogging` → `BuildRunLog` → `WriteRunLog` | **onCompletion** (`AfterConsumer`) | After **every** run, success or failure, writes `runlog/<runId>.json` with status, error, counts, window and `watermarkHeld`. | `InvokeLogging` (depends on Completed/Skipped of every step) | Local file; doesn't go to `etl_run_log`, so it isn't in the views or Power BI. |

---

## 6. The failure path — how it is wired

There are no arrows from every step to an error box, as there are in Fabric. Instead, two top-level blocks in the
YAML apply to the whole route. Kaoto shows them as separate flows next to the route.

```
any step throws an exception (outside the split's doTry)
        │
        ▼
onException "RetryTransientFailure"   (java.lang.Exception: retry the failed step 5×, 30 s apart)
onException "NoRetryFailure"          (IllegalState/IllegalArgument/MessagingException: no retry)
        │  both, once retries are used up, handled = false
        ▼
direct:sendUpstreamFailureEmail  (route "SendUpstreamFailureEmail")
   ├─ BuildFailureMail   (bean failureMail: body = reason, window, run id; sets runError)
   ├─ subject = "ERROR: Camel: lm-to-fms-locations FAILED (watermark held) - run <id>"
   └─ doTry MailFailure (smtp)  ── if the mail itself fails → log WARN only
        │
        ▼  handled = false → the run ends as FAILED (like FailRunOnUpstreamError)
onCompletion "InvokeLogging"  → runlog/<id>.json with status "Failed" and the error
```

| What throws | When | Result | Fabric equivalent |
|---|---|---|---|
| `FailOnWatermarkError` | watermark file bad, override malformed, or clock went backwards | failure mail; nothing read; watermark unchanged | `FailOnWatermarkError` (WATERMARK_UNUSABLE) |
| `LookupLocations` / `LookupRequirements` | LM unreachable or SQL error | retried 5×, 30 s apart; then failure mail; nothing sent | Lookup fails after 5 retries → `SendUpstreamFailureEmail` |
| `FailOnBuildError` | sentinel row missing, clocks differ by more than 20 min, calls over 1.5 MB | failure mail; nothing sent | `FailOnBuildError` (BUILD_FAILED) |
| **FMS call error** | any SQLException in `ExecuteDb2Block` | **caught by doTry/doCatch inside the split → it is a result, not a failure** | Lookup failure is recorded by `RecordLocationOutcome` (dependency = Completed) |
| `FinalizeLocations` | results ≠ calls | failure mail; watermark held | Finalize raises → watermark held |
| `AdvanceWatermark` | a location was `technical` → `nextBeginTs` blank | failure mail; **watermark held**; the next run resends the window | `AdvanceWatermark` fails → `PropagateWatermarkAdvanceFailure` (run red) |
| `SendDigestEmail` | SMTP down | failure mail (which wrongly says "watermark held" — it already advanced) | `SendDigestEmail` retry 0 |

**Two differences to be ready for:**
1. When a location is `technical`, Fabric still sends the digest listing every failed location, then fails the run.
   Camel fails at `AdvanceWatermark` **before** the digest, so support gets only the generic failure mail.
   (This is the same situation as the PROD alert of 5 Nov: `nextBeginTs is not 'yyyy-MM-dd HH:mm:ss.SSS': ''`.)
2. PROD labelled that same situation `RUN_PARAMETER_INVALID`, which is wrong. Camel's mail says "watermark held"
   with the exception text, which is at least accurate.

---

### End-to-end tests (mocked)

`test/LmToFmsRouteTest.java` loads this YAML unchanged and mocks only the edges with AdviceWith:
quartz → `direct:start`, the two LM lookups → canned rows, `ExecuteDb2Block` → a fake FMS that throws the
SIGNAL text (`P9OK`, `P9ERR …`, or a link failure), both mails → `mock:` endpoints. The NiFi flow has the same
scenarios in `../nifi/test/test_flow.py` (see `../nifi/DEMO_NOTES.md`). Files go to a temp folder
and are checked for real. No LM, FMS, SMTP, Java or Maven install is needed:

```bash
cd kaoto
jbang test/LmToFmsRouteTest.java
```

| Test | Checks |
|---|---|
| `allSent_advancesWatermark_noMail` | watermark = endTs, run log Succeeded, no mail |
| `fmsRefusal_advancesWatermark_sendsDigest` | watermark still advances, BUSINESS digest lists the location |
| `technicalFailure_holdsWatermark_sendsFailureMail` | watermark.json unchanged, failure mail, run Failed |
| `badWatermark_failsBeforeReadingLm` | WATERMARK_UNUSABLE, LM never read, no FMS calls |
| `lmDown_isRetriedFiveTimes_thenFails` | 6 LM attempts (1 + 5 retries), then failure mail |
| `override_replaysWindow_withoutReadingWatermark` | override used, broken watermark file not touched |
| `digestSmtpDown_failsRun_butWatermarkAlreadyAdvanced` | SMTP error on the digest fails the run, but the watermark has already moved (NiFi drops it and stays Succeeded) |
| `failureMailSmtpDown_isLoggedOnly_runLogStillWritten` | a failing failure-mail only logs a WARN; the run log keeps the original error |

---

## 7. The watermark in one picture

```
run N:  read watermark.json → window (beginTs, endTs] → send → all results sent / fms_error
        → write nextBeginTs = endTs (or the resume point if capped at 400)        ✅ advanced
run N:  … any result technical → nextBeginTs blank → AdvanceWatermark throws       ⛔ held
run N+1: same window again (FMS procedures are idempotent, so the resend is safe)
```

`fms_error` (FMS refused the data) does **not** hold the watermark; that location needs a data fix and a replay.
A held watermark is the **only** automatic retry.

---

## 8. Honest differences from PROD Fabric (have this list ready)

| Area | Fabric PROD | Kaoto/Camel POC |
|---|---|---|
| Logic version | Newer UDF than the repo | Copy of the **repo** UDF |
| Retries | 5 on watermark, LM read, build, finalize, advance | 5× 30 s on any failing step; none on deliberate stops |
| Timeouts | FMS call 3 min, LM read 20 min | same |
| Digest when a location is technical | sent | not sent (generic failure mail only) |
| Alerts | incident cards, Still happening / Recovered, daily summary | plain text mails |
| Logging | `etl_run_log` / `etl_item_log` → views, Power BI | local JSON files |
| Watchdog quiet-path check | yes | no |
| Replay | per-run parameter | property that stays set until cleared |
| Storage | lakehouse | local disk (could switch to the `azure-storage-datalake` component) |
| Tested on real LM/FMS | yes | **no** |

Known data bugs are the **same in both** because the logic was copied: a fax is sent unchecked (location 149471),
and an edit to `Contact` alone doesn't open a window.

---

## 9. Running it in the demo

```bash
cd kaoto
export LM_DB_HOST=… LM_DB_USER=… LM_DB_PASSWORD=… FMS_DB_HOST=… FMS_DB_USER=… FMS_DB_PASSWORD=…
export SENDGRID_API_KEY=… MTC_SUPPORT_EMAIL=you@…
camel run lm-to-fms-locations.camel.yaml *.java application.properties sql/*.sql
```

- **Without database access** (safe demo): the run starts, seeds the window 30 min back, fails at
  `LookupLocations`, goes down the failure path, and writes `data/lm2fms/runlog/<id>.json` with
  `"status":"Failed"`. That is a good live demo of the failure wiring. Leave `SENDGRID_API_KEY` empty: the mail
  attempt fails inside its own doTry and only logs a WARN.
- **Never point it at PROD FMS for a demo**: it writes real location updates.
- Show the logic without databases: the parity check in `../README.md` (14/14 keys match).
- To deploy for real: `camel export --runtime=quarkus` (or `spring-boot`) produces a normal Maven project.

---

## 10. Questions you will probably get, and answers

**Kaoto and Camel basics**
1. *What is Kaoto vs Camel?* — Camel is the integration engine that runs. Kaoto is the visual editor for Camel
   routes. The canvas is a view of the YAML file; edit either one and the other updates.
2. *Is this low-code?* — The route is visual. The p9-specific logic (field mapping, DB2 statement, classification)
   is Java code in a bean, just as Fabric keeps it in a Python UDF.
3. *Where does it run?* — Any JVM: a container, Kubernetes or a VM. For the demo, the Camel JBang CLI. For
   production, `camel export` to Quarkus or Spring Boot. It does not need Fabric or a gateway, but it does need
   network access to the LM SQL Server and to FMS (port 8471 and the other IBM i host-server ports).
4. *How is it monitored?* — Logs plus the JSON run logs in this POC. In production: Micrometer/Prometheus metrics,
   Hawtio or the Camel CLI (`camel get`, `camel trace`) for live routes. There is no Fabric-style Monitor view
   out of the box.
5. *How does it compare to NiFi?* — Camel: code-reviewable YAML and Java, a small runtime, a clean fit for
   "poll → transform → call a procedure". NiFi: a built-in UI with provenance, replay and queue inspection, but a
   cluster to run, and its flow JSON is hard to review.

**Behaviour**
6. *What if a run takes longer than 15 minutes?* — The next one waits; they never overlap, the same as Fabric's
   `concurrency 1`.
7. *What if FMS is down?* — Every location comes back `technical`, the watermark is held, a failure mail goes out,
   and the next run resends the same window. Nothing is lost.
8. *What if FMS rejects one location's data?* — It is recorded as `fms_error` in the item log and the digest mail.
   The watermark still moves on, so it needs an LM fix plus a replay with `lm.overrideBeginTs`.
9. *How do you replay?* — Set `lm.overrideBeginTs`, restart, let one run go, then clear it. Unlike Fabric, it stays
   in effect until cleared.
10. *Can the same location be sent twice?* — Yes, after a held watermark or a replay. That is safe because the FMS
    procedures are add-or-update.
11. *Why does every DB2 call "fail"?* — By design: the statement ends in SIGNAL so FMS's answer comes back in the
    error text. It's the same trick Fabric uses, and that's why FMS steps show "Failed" in Fabric's Monitor view.
12. *Why 12 parallel calls?* — It matches Fabric's ForEach batch count; change `fms.parallelism`.
13. *How does it handle more than 400 changes?* — It reads 400 oldest first, records the last location
    (`nextBeginId`), and the next run continues after it, as Fabric does.
14. *Where are secrets kept?* — Environment variables in the POC. In production: Kubernetes secrets, Azure Key Vault
    (`camel-azure-key-vault`) or Vault, never in the YAML.

**Trust and parity**
15. *How do we know it does the same as Fabric?* — `parity/` runs the Java port and the Fabric Python on the same
    test data: 14/14 outputs match, including the exact DB2 SQL. Live LM/FMS parity has not been tested yet.
16. *Is it the PROD version?* — No. It matches the repo UDF; PROD runs a newer build. Export the deployed UDF to
    re-sync.
17. *What's missing before production?* — The digest on technical results, lakehouse or
    database storage for the watermark and logs, the incident-style alerts, monitoring, the fax check, and a test
    against non-PROD LM/FMS.
18. *Who maintains the logic?* — Today there are two copies (Python in Fabric, Java here) and `parity/` guards
    against drift. Moving for real means retiring the Fabric copy.

**Gotchas to admit if asked**
19. The two LM queries run separately, so the snapshot is not quite atomic. Fabric runs them in one Script call; the
    NiFi port uses one nested query.
20. `fmsDataSource` does not pool connections (one connection per call, like the gateway). A pool is a one-line
    change if needed.

---

## 11. Camel words you will hear

| Term | Meaning |
|---|---|
| Route | One flow from a `from:` (here, the timer) through steps |
| Exchange | One run: the message with body, headers and properties |
| Component / endpoint | A connector addressed by a URI: `timer:`, `sql:`, `jdbc:`, `file:`, `smtp:` |
| Bean | A Java object the route calls (`lmFms`) |
| EIP | Enterprise Integration Pattern: `split`, `choice`, `doTry` |
| `split` + aggregation | Fan out per item, then gather the results (Fabric's ForEach + AppendVariable) |
| `onException` | Route-wide error handler (Fabric's "on Failed" dependencies + the fail activity) |
| `onCompletion` | Runs after the route on every outcome (Fabric's "on Completed" logging step) |
| `{{property}}` | A placeholder resolved from `application.properties` or the environment |
