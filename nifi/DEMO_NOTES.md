# NiFi demo notes — LM → FMS Locations (p9) compared with Fabric

These notes are for presenting the Apache NiFi 2.x port of `9_pl_LM_To_FMS_Locations`.
Checked on 15 Nov 2026 against the files in this folder. The Kaoto/Camel port has its own notes in
`../kaoto/DEMO_NOTES.md`; the two follow the same layout so you can compare them section by section.

**What "Fabric" means here.** The Fabric column is the pipeline definition in the repo, plus how PROD behaved in
the November alerts. PROD runs a **newer** UDF than the repo: the PROD traceback pointed at line 1019 of
`function_app.py`, and the repo file has 908 lines. Its alert mails also look different (incident cards). Say this
up front if anyone asks whether this is "the PROD version".

**Status to say up front.** The flow has **not** been imported into a live NiFi yet (no Docker on the dev
machine). The logic and the wiring are tested without NiFi (section 6). Bundle versions or property names may need
a fix on the first import.

---

## 1. The 30-second pitch

> "This is the same p9 job — LocationManagement changes pushed to FMS every 15 minutes — rebuilt as a NiFi flow.
> Each Fabric activity is a NiFi processor with the same name. The p9 logic (field mapping, the DB2 statement per
> location, classifying results, the watermark) is the Fabric UDF's Python, copied **verbatim** into four NiFi
> Python processors by a build script, so it cannot drift. NiFi adds what Fabric's Monitor view doesn't have:
> every FlowFile can be inspected and replayed (provenance), and queues show exactly where a run is stuck.
> It hasn't run against real LM or FMS yet."

---

## 2. What is in this folder

| File | What it is | Fabric equivalent |
|---|---|---|
| `LM_To_FMS_Locations.flow.json` | The flow definition. Import with *Process Group → Upload flow definition*. **Generated** | The pipeline (`pipeline-content.json`) |
| `python_extensions/LmFmsLocations.py` | Four NiFi Python processors + the UDF core copied verbatim. **Generated** | `udf_lm_fms_locations/function_app.py` |
| `processors_template.py` | The NiFi processor classes before the UDF code is spliced in | The UDF entry points (`readWatermark`, `buildLocationCalls`, `finalizeLocations`, `advanceWatermark`) |
| `build_nifi.py` | Regenerates both generated files. Re-run after any UDF change | — |
| `lm-query.sql` | One LM query; requirements ride along as a `FOR JSON` column | `LookupLocationChanges` (one Script activity, two result sets) |
| `docker-compose.yml` | Local NiFi 2.x for the demo | The Fabric workspace |
| `drivers/` | Put `mssql-jdbc-*.jar` and `jt400-*.jar` here | The gateway's drivers |
| `test/test_flow.py` | End-to-end tests of the flow with every external system faked | — |

**Never edit the two generated files by hand.** Change `build_nifi.py` or `processors_template.py`, then run
`python3 nifi/build_nifi.py`.

---

## 3. Where every value comes from

NiFi reads settings from the **parameter context** `lm_to_fms_locations`. In a property, `#{name}` is a parameter
and `${name}` is a FlowFile attribute (NiFi Expression Language). Sensitive parameters (passwords) are entered in
the NiFi UI after import and are never in the JSON.

| Setting | Where | Value / source | Fabric equivalent |
|---|---|---|---|
| Schedule | `Schedule` processor, CRON driven | `0 0/15 9-16 * * ?` (every 15 min, 09:00–16:45), primary node only | Schedule trigger, every 15 min |
| Max locations per run | `#{max_locations_per_run}` | 400 | parameter `max_locations_per_run` |
| FMS library | `#{fms_library}` | FMSLIB | parameter `fms_library` |
| Phone for blank or zero phones | `#{default_contact_phone}` | 0000000001 | parameter `default_contact_phone` |
| FMS calls in parallel | `CallFmsLocation` concurrent tasks + `#{fms_parallelism}` pool size | 12 | ForEach `batchCount` 12 |
| Replay start time | `#{override_begin_ts}` | blank | parameter `overrideBeginTs` |
| Watermark, item logs, run logs | `#{staging_dir}` | `/opt/nifi/data/lm2fms` (local disk) | lakehouse `Files/mtc/staging/lm2fms` |
| LM database | controller service *LocationManagement DB* | `#{lm_jdbc_url}`, `#{lm_db_user}`, `#{lm_db_password}`, 2 connections | LM SQL Server connection |
| FMS DB2 | controller service *FMS DB2* | `#{fms_jdbc_url}` (`jdbc:as400://HOST;naming=system;prompt=false`), user, password, 12 connections | ODBC connection `0a9acb5b…` through the gateway |
| Mail | both `PutEmail` processors | `#{mail_host}` smtp.sendgrid.net, `#{mail_port}` 587, user `apikey`, `#{mail_password}` = SendGrid key, `#{mail_to}` | `sendEmail` UDF + Variable Library |
| Driver jars | `#{jdbc_driver_dir}` | `/opt/nifi/drivers` | The gateway's drivers |

**Controller services** (shared connections, enable them before starting the group):
- *LocationManagement DB* → a DBCP pool to SQL Server.
- *FMS DB2* → a DBCP pool to DB2 for i. Unlike the Camel port it **does** pool connections (12).
- *JSON array writer* → turns the LM result set into one JSON array.

**Replay difference — say this clearly.** In Fabric, `overrideBeginTs` applies to **one run** because it is a
run parameter. In NiFi, `override_begin_ts` is a parameter, so it applies to **every** run until you clear it.
If you leave it set, every 15 minutes the flow re-sends everything from that time onwards.

---

## 4. How data moves inside a NiFi run (explain this once)

One run = one **FlowFile** created by `Schedule`, then split into one FlowFile per location and merged back.
A FlowFile has two parts:

| Part | What it holds here | Rough Fabric equivalent |
|---|---|---|
| **Content** | The payload: the LM rows, then the calls array, then one call, then the merged outcomes, then the item log | the activity Output that the next activity reads |
| **Attributes** | Key/value strings kept for the run: `lm.runId`, `lm.beginTs`, `lm.endTs`, `lm.nextBeginTs`, `lm.callCount`, `lm.digestBody`, `lm.hasFailures`, `sql.args.N.*`, `executesql.error.message`, … | pipeline variables / `activity('X').output` |

Processors pass FlowFiles along **relationships** (`success`, `failure`, `empty`, `has_failures`, …). Every
relationship is either connected to the next processor or **auto-terminated** (dropped). That is how branching
and error handling are drawn on the canvas.

`lm.runId` is the FlowFile's `uuid`, a GUID like the Fabric pipeline `RunId`. Every attribute is a string, so
counts like `lm.sent` are `"2"`, not `2`.

---

## 5. Step by step (the processor names match the Fabric activity names)

| # | Processor | NiFi type | What it does | Fabric | Difference |
|---|---|---|---|---|---|
| 0 | `Schedule` | GenerateFlowFile (CRON) | Creates one empty FlowFile every 15 min, 09:00–16:45, on the primary node. | Schedule trigger + `concurrency 1` | **Runs can overlap**: NiFi fires on time even if the last run is still going. |
| 1 | `ReadWatermark` | Python `ComputeLmWindow` | Reads `#{staging_dir}/watermark.json` (skipped when the override is set) and builds the window. Missing file → seed 30 min back; bad file → `failure` with `WATERMARK_UNUSABLE`. Sets `lm.runId`, `lm.beginTs`, `lm.endTs`, `lm.beginId` and `sql.args.1..4` for the query. | `ReadWatermark` (UDF `readWatermark`) + `CheckWatermarkError` | endTs is the current clock time; Fabric uses the trigger time. No retry. |
| 2 | `LookupLocationChanges` | ExecuteSQLRecord | Runs `lm-query.sql`. The `?` markers bind from `sql.args.N`. One record per location with its requirements nested in `RequirementsJson`, then the `-1` sentinel row carrying `SYSDATETIME()`. | `LookupLocationChanges` (20 min, retry 5) | One query, so the snapshot is atomic (Camel runs two). Timeout 20 min, 5 retries 30 s apart: same as Fabric. |
| 3 | `DropSqlArgs` | UpdateAttribute | Deletes `sql.args.*`. | — | NiFi only: ExecuteSQL would bind the leftovers into the DB2 statement. |
| 4 | `BuildLocationCalls` | Python `BuildFmsLocationCalls` | Checks the sentinel and the clock (20 min), builds one DB2 compound statement per location, checks 1.5 MB. Content = the calls array. No sendable locations → `empty`, straight to finalize. Error → `failure` with `BUILD_FAILED`. | `BuildLocationCalls` + `CheckBuildError` | Same logic (the UDF code itself). No retry. |
| 5 | `SplitPerLocation` | SplitJson `$.*` | One FlowFile per call, tagged with `fragment.identifier/index/count`. | ForEach | — |
| 6 | `ExtractCall` | EvaluateJsonPath | Copies `locationId`, `requirementCount` and `sql` into attributes `lm.locationId`, `lm.requirementCount`, `lm.sql`. | `item()` inside the ForEach | — |
| 7 | `CallFmsLocation` | ExecuteSQL `${lm.sql}` | Sends the location's DB2 statement, 12 at a time. It **always** ends with `SIGNAL 75005 'P9OK'` or `'P9ERR …'`, so it lands on `failure` with the text in `executesql.error.message`. `success` and `failure` both go on. | `CallFmsLocation` Lookup (3 min, retry 0) | Same timeout and no retry. Every location raises an **ERROR bulletin**, which is noisy. |
| 8 | `RecordLocationOutcome` | AttributesToJSON | Content = `{lm.locationId, lm.requirementCount, executesql.error.message}`. | `RecordLocationOutcome` (AppendVariable) | — |
| 9 | `GatherOutcomes` | MergeContent (Defragment) | Waits for every fragment of the run and joins them into one JSON array. A fragment missing after 20 min → `failure`. | end of the ForEach | Keeps only attributes common to all fragments. |
| 10 | `FinalizeLocations` | Python `FinalizeLmLocations` | Checks outcomes = `lm.callCount`, classifies each (`sent` / `fms_error` / `technical`), builds item-log rows and the digest. A `technical` result blanks `lm.nextBeginTs`. | `FinalizeLocations` | Same logic. No retry. |
| 11 | `WriteItemLog` | PutFile | Writes `items/<runId>/items.json`. Runs **in parallel** with step 12. | Written by the UDF to the lakehouse | Local file; doesn't go to `etl_item_log`. |
| 12 | `AdvanceWatermark` | Python `AdvanceLmWatermark` | Writes `watermark.json` (temp file + rename). Blank `lm.nextBeginTs` → `failure` with `WATERMARK_ADVANCE_FAILED`. | `AdvanceWatermark` (retry 5) | Same rule. No retry. |
| 13 | `CheckDigestEmail` | RouteOnAttribute | `${lm.hasFailures:equals('true')}` → `has_failures`. | `CheckDigestEmail` | Only reached after the watermark advanced. |
| 14 | `SendDigestEmail` | PutEmail | Sends the BUSINESS digest with `lm.digestBody`. `success` and `failure` are auto-terminated. | `SendDigestEmail` (retry 0) | If SMTP is down the mail is lost and the run still counts as Succeeded (bulletin only). |
| 15 | `MarkSucceeded` | UpdateAttribute | `lm.status = Succeeded` → run log. | Pipeline Succeeded | — |

---

## 6. The failure path — how it is wired

NiFi has no route-wide error handler. Each step's `failure` relationship is drawn to one processor, `MarkFailed`:

```
ReadWatermark / LookupLocationChanges (after 5 retries) / BuildLocationCalls / SplitPerLocation /
ExtractCall / RecordLocationOutcome / GatherOutcomes / FinalizeLocations / WriteItemLog / AdvanceWatermark
        │ failure
        ▼
MarkFailed   lm.status = Failed, lm.error = ${lm.error} or ${executesql.error.message} or 'see bulletins'
   ├─► SendUpstreamFailureEmail  "ERROR: NiFi: lm-to-fms-locations FAILED (watermark held) - run <id>"
   └─► BuildRunLog → NameRunLog → WriteRunLog   runlog/<runId>.json with every lm.* attribute

success path:  AdvanceWatermark ─► MarkSucceeded ─► BuildRunLog → NameRunLog → WriteRunLog
```

| What fails | When | Result | Fabric equivalent |
|---|---|---|---|
| `ReadWatermark` | watermark file bad, override malformed, or clock went backwards | failure mail; LM not read; watermark unchanged | `FailOnWatermarkError` (WATERMARK_UNUSABLE) |
| `LookupLocationChanges` | LM unreachable or SQL error | retried 5×, 30 s apart; then failure mail; nothing sent | Lookup fails after 5 retries → `SendUpstreamFailureEmail` |
| `BuildLocationCalls` | sentinel missing, clocks differ by more than 20 min, calls over 1.5 MB | failure mail; nothing sent | `FailOnBuildError` (BUILD_FAILED) |
| **FMS call** | any DB2 answer, including a refusal | **not a failure**: both relationships go to `RecordLocationOutcome` | Lookup failure recorded by `RecordLocationOutcome` |
| `GatherOutcomes` / `FinalizeLocations` | an outcome went missing | failure mail; watermark held | Finalize raises → watermark held |
| `AdvanceWatermark` | a location was `technical` | failure mail; **watermark held**; the next run resends the window | `AdvanceWatermark` fails → run red |
| `SendDigestEmail` | SMTP down | **nothing**: auto-terminated, run stays Succeeded | `SendDigestEmail` retry 0 |

**Two differences to be ready for:**
1. When a location is `technical`, Fabric still sends the digest listing every failed location, then fails the run.
   NiFi fails at `AdvanceWatermark` **before** the digest, so support gets only the generic failure mail (same as Camel).
2. A Python processor that crashes (rather than returning `failure`) is rolled back by NiFi and retried until it
   works, with the FlowFile waiting in the queue. That is safe, but it shows as a stuck queue, not as a failure mail.

### End-to-end tests (mocked)

`test/test_flow.py` drives the **real** `LM_To_FMS_Locations.flow.json`: it follows the flow's connections, runs
the four Python processors for real, and emulates NiFi's built-in processors with the properties in the JSON
(including the `${…}` / `#{…}` expressions and the retry settings). LM, FMS and SMTP are fakes; watermark, item log
and run log are written to a temp folder and checked. A relationship that is neither connected nor
auto-terminated fails the test, so a wiring mistake is caught. No NiFi, Docker or pip install is needed:

```bash
python3 nifi/test/test_flow.py -v
```

| Test | Checks |
|---|---|
| `test_all_sent_advances_watermark_no_mail` | watermark = endTs, run log Succeeded, no mail |
| `test_fms_refusal_advances_watermark_sends_digest` | watermark still advances, BUSINESS digest lists the location |
| `test_technical_failure_holds_watermark_sends_failure_mail` | watermark.json unchanged, failure mail, no digest, run Failed |
| `test_bad_watermark_fails_before_reading_lm` | WATERMARK_UNUSABLE, LM never read, no FMS calls |
| `test_lm_down_is_retried_then_fails` | 6 LM attempts (1 + 5 retries), then failure mail |
| `test_override_replays_window_without_reading_watermark` | override used, broken watermark file not touched |
| `test_no_changes_skips_fms_and_advances` | `empty` path: no FMS calls, watermark advances |
| `test_unsignalled_block_is_technical` | a DB2 block that returns without SIGNAL holds the watermark |
| `test_digest_smtp_down_still_succeeds` | SMTP error on the digest is dropped, run Succeeded |
| `FlowDefinitionTest` (7 checks) | cron, 3/20 min timeouts, retry 5 on the LM read and 0 on FMS, every failure reaches `MarkFailed`, `MarkSucceeded` only after the watermark, processor classes exist |

What the tests can't prove: NiFi's own processor code (real MergeContent binning, how the real DB2 driver reports
a SIGNAL). That needs one run in a real NiFi against non-PROD LM/FMS.

---

## 7. The watermark in one picture

```
run N:  read watermark.json → window (beginTs, endTs] → send → all results sent / fms_error
        → write nextBeginTs = endTs (or the resume point if capped at 400)        ✅ advanced
run N:  … any result technical → nextBeginTs blank → AdvanceWatermark → failure   ⛔ held
run N+1: same window again (FMS procedures are idempotent, so the resend is safe)
```

`fms_error` (FMS refused the data) does **not** hold the watermark; that location needs a data fix and a replay.
A held watermark is the **only** automatic retry of FMS calls.

---

## 8. Honest differences from PROD Fabric (have this list ready)

| Area | Fabric PROD | NiFi POC |
|---|---|---|
| Logic version | Newer UDF than the repo | Copy of the **repo** UDF (verbatim) |
| Retries | 5 on watermark, LM read, build, finalize, advance | LM read only (5× 30 s). Python steps: none |
| Timeouts | FMS call 3 min, LM read 20 min | same |
| Overlapping runs | never (`concurrency 1`) | **possible** if a run takes over 15 min |
| Digest when a location is technical | sent | not sent (generic failure mail only) |
| Digest mail fails | run continues | dropped silently (bulletin) |
| Alerts | incident cards, Still happening / Recovered, daily summary | plain text mails |
| Logging | `etl_run_log` / `etl_item_log` → views, Power BI | local JSON files + NiFi provenance |
| Watchdog quiet-path check | yes | no |
| Replay | per-run parameter | parameter that stays set until cleared |
| Storage | lakehouse | local disk in the NiFi container |
| Imported into a real NiFi | — | **no** |
| Tested on real LM/FMS | yes | **no** |

Known data bugs are the **same in all three** because the logic was copied: a fax is sent unchecked (location
149471), and an edit to `Contact` alone doesn't open a window.

---

## 9. Running it in the demo

```bash
cd lm_to_fms_locations
cp ~/Downloads/mssql-jdbc-*.jar ~/Downloads/jt400-*.jar nifi/drivers/
docker compose -f nifi/docker-compose.yml up        # then open https://localhost:8443/nifi
```

1. Drag in a Process Group → **Upload flow definition** → `nifi/LM_To_FMS_Locations.flow.json`.
2. Fill in the parameter context `lm_to_fms_locations`: JDBC URLs, users, the 3 sensitive passwords, `mail_to`.
3. Enable the 3 controller services, then start the group.

- **Without database access** (safe demo): wait for the next quarter hour inside 09:00–16:45 (or right-click
  `Schedule` → *Run Once*). `LookupLocationChanges` retries 5 times, then the FlowFile goes down `MarkFailed` and
  `runlog/<id>.json` shows `"lm.status":"Failed"`. Good for showing queues, bulletins and provenance.
- **Never point it at PROD FMS for a demo**: it writes real location updates.
- Show the logic and wiring without NiFi: `python3 nifi/test/test_flow.py -v` (16 tests) and `parity/nifi_sim.py`.
- After any change to the Fabric UDF: `python3 nifi/build_nifi.py`, re-run the tests, re-upload the flow.

---

## 10. Questions you will probably get, and answers

**NiFi basics**
1. *What is NiFi?* — A dataflow engine with a web canvas. The canvas *is* the running system: you drag
   processors, connect relationships, and start/stop them live.
2. *Is this low-code?* — The flow is visual and mostly built-in processors. The p9-specific logic is Python in four
   custom processors — the UDF code itself, copied by `build_nifi.py`.
3. *Where does it run?* — A NiFi server or cluster (container, VM, Kubernetes). It needs network access to the LM
   SQL Server and to FMS (port 8471 and the other IBM i host-server ports). No Fabric or gateway.
4. *How is it monitored?* — Built in: queue sizes, bulletins (errors) per processor, and **provenance**, which keeps
   every FlowFile's attributes and content at every step and can replay it. Plus the JSON run logs.
5. *How does it compare to Camel?* — NiFi: best-in-class run inspection and replay, logic reused verbatim, but a
   cluster to operate and a flow JSON that is hard to review in a PR. Camel: reviewable YAML + Java, small runtime,
   but the logic is a second (Java) copy.

**Behaviour**
6. *What if a run takes longer than 15 minutes?* — The next one starts anyway and can read the same window. That's
   harmless for FMS (idempotent procedures) but it double-sends. A Wait/Notify guard would fix it.
7. *What if FMS is down?* — Every location comes back `technical`, the watermark is held, a failure mail goes out,
   and the next run resends the same window. Nothing is lost.
8. *What if FMS rejects one location's data?* — It's an `fms_error` in the item log and the digest mail. The
   watermark still moves on, so it needs an LM fix plus a replay with `override_begin_ts`.
9. *How do you replay?* — Set `override_begin_ts`, let one run go, then clear it. Or replay one FlowFile from
   provenance. Unlike Fabric, the parameter stays in effect until cleared.
10. *Can the same location be sent twice?* — Yes, after a held watermark, a replay or an overlap. Safe, because the
    FMS procedures are add-or-update.
11. *Why is every FMS call red/an ERROR bulletin?* — By design: the statement ends in SIGNAL so FMS's answer comes
    back as the error text. It's the same trick Fabric uses. Lower the bulletin level on `CallFmsLocation` to hide it.
12. *Why 12 parallel calls?* — It matches Fabric's ForEach batch count; change the concurrent tasks and
    `fms_parallelism` together.
13. *How does it handle more than 400 changes?* — It reads 400 oldest first, records the last location
    (`nextBeginId`), and the next run continues after it, as Fabric does.
14. *Where are secrets kept?* — Sensitive parameters, encrypted by NiFi and never in the exported JSON. In production:
    a parameter provider (Azure Key Vault, HashiCorp Vault).

**Trust and parity**
15. *How do we know it does the same as Fabric?* — The Python processors run the UDF's own code. `parity/nifi_sim.py`
    and `test/test_flow.py` check the result, including byte-identical DB2 SQL. Live parity is not tested yet.
16. *Is it the PROD version?* — No. It matches the repo UDF; PROD runs a newer build. Export the deployed UDF and
    re-run `build_nifi.py` to re-sync.
17. *What's missing before production?* — A first import into real NiFi, an overlap guard, retries on the Python
    steps, the digest on technical results, lakehouse or database storage, the incident-style alerts, monitoring,
    the fax check, and a test against non-PROD LM/FMS.
18. *Who maintains the logic?* — There is only one copy: the Fabric UDF. `build_nifi.py` regenerates the NiFi side.

**Gotchas to admit if asked**
19. Every FMS call shows as a failure in NiFi's UI (see 10.11).
20. `WriteItemLog` runs in parallel with `AdvanceWatermark`. If only the item-log write fails, the watermark still
    moves, a failure mail says "watermark held" (not true here), and the run log is written twice under the same
    name (Succeeded and Failed; the last write wins).

---

## 11. NiFi words you will hear

| Term | Meaning |
|---|---|
| FlowFile | One piece of data moving through the flow: content + attributes |
| Processor | One step on the canvas (`ExecuteSQL`, `PutFile`, a Python processor, …) |
| Relationship | A named exit of a processor (`success`, `failure`, `empty`, …) |
| Connection / queue | The arrow between processors; FlowFiles wait in it |
| Auto-terminate | Drop FlowFiles that leave on that relationship |
| Controller service | A shared resource such as a DB connection pool or a record writer |
| Parameter context | The flow's settings (`#{name}`); some are sensitive |
| Expression Language | `${attribute}` with functions like `:equals()`, `:replaceEmpty()` |
| Provenance | The record of every FlowFile at every step; can be inspected and replayed |
| Bulletin | An error/warning badge on a processor |
| Penalty / retry | How long a FlowFile waits before a retry; `retryCount` on a relationship |
| Defragment | MergeContent mode that rebuilds the pieces of one SplitJson |
