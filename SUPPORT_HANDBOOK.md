# LM_To_FMS_Locations (p9) — support handbook: Fabric, NiFi and Kaoto/Camel

Last checked: 13 Nov 2026, against branch `giresh-dev`.

The same job runs in three runtimes. **Fabric** (`9_pl_LM_To_FMS_Locations`) is the PROD pipeline. **NiFi** and
**Kaoto/Camel** are POC ports in this repo (`lm_to_fms_locations`), separate from MTCIntegrations. Neither port has run against real LM or FMS.

| | Fabric (PROD) | NiFi (POC) | Kaoto / Camel (POC) |
|---|---|---|---|
| Where | workspace `Enru-Operations-MTC-Integrations-prod` | `nifi/LM_To_FMS_Locations.flow.json` + Python processors | `kaoto/lm-to-fms-locations.camel.yaml` + Java bean |
| Trigger | schedule, every 15 min, one run at a time (`concurrency 1`) | `Schedule` (GenerateFlowFile), cron `0 0/15 9-16 * * ?` (09:00–16:45) | `quartz:lm2fms`, `lm.cron=0 0/15 9-16 * * ?` (09:00–16:45) |
| Logic | UDF `udf_lm_fms_locations` | same Python code as the UDF, copied by `build_nifi.py` | Java port (`LmFmsCore.java`) |
| Status | live | not imported into a live NiFi yet | started once with Camel JBang; stopped at the LM connection |

Times are US Central. A **window** is the LM change interval `(beginTs, endTs]` that one run reads. The
**watermark** (`watermark.json`) stores where the next window starts.

---

## What it does

Every 15 minutes the job does six things:

1. **ReadWatermark** — reads `watermark.json` and builds the window: `beginTs` (with `beginId`) up to now in
   Central time.
   - No file yet → it starts 30 min back (`SEED_LOOKBACK_MINUTES`).
   - Bad file → the run fails and nothing is read.
2. **LookupLocationChanges** — reads LocationManagement (SQL Server) for up to `max_locations_per_run` (400)
   locations changed in the window, oldest first, plus their requirements.
   - Each result set ends in a sentinel row (`LocationId -1`) that carries the LM server clock.
   - No sentinel means the read was truncated, so the run fails.
   - A clock difference of more than 20 min between the LM server and the window also fails the run.
3. **BuildLocationCalls** — builds one DB2 compound statement per location. Each statement runs:
   - `FMSLIB.ADD_UPDATE_LOCATIONS` (20 parameters);
   - if the location has requirements, `FMSLIB.LOCATION_REQUIREMENTS_UPDATE` once with `D` (delete), then
     with `A` (add) in groups of 50;
   - finally `SIGNAL SQLSTATE '75005'` with `P9OK` or `P9ERR <step> <code> <text>`.

   A location ID longer than 7 digits is skipped (`location_id_unmappable`).
4. **CallFmsLocations** — runs the statements against FMS (DB2 for i), 12 at a time. Every call "fails",
   because the block always ends in SIGNAL; the error text is the result.
5. **FinalizeLocations** — sorts each result:

   | Result | Message contains | Meaning |
   |---|---|---|
   | `sent` | `P9OK` | FMS took the location. |
   | `fms_error` | `P9ERR` | FMS refused the data; the code is `<step>_<code>`, for example `AU_DLS0313`. |
   | `technical` | anything else | DB2, the connection or the gateway failed, or the call timed out. |

   It also writes one item log row per location and builds the digest mail.
6. **AdvanceWatermark** — saves the next window start, but only when there is no `technical` result.
   - Any `technical` result → the watermark is **held** and the next run reads the same window again. The
     FMS procedures are idempotent, so the resend is safe.
   - An `fms_error` does **not** hold the watermark; that location is not retried.

FMS step codes: `AU` = ADD_UPDATE_LOCATIONS, `RD` = requirements delete, `RA` = requirements add.

---

## Parameters

| Meaning | Fabric parameter | NiFi parameter (context `lm_to_fms_locations`) | Camel property |
|---|---|---|---|
| Max locations per run | `max_locations_per_run` = 400 | `max_locations_per_run` | `lm.maxLocationsPerRun` |
| FMS library | `fms_library` = FMSLIB | `fms_library` | `fms.library` |
| Phone used for a blank or zero phone | `default_contact_phone` = 0000000001 | `default_contact_phone` | `fms.defaultContactPhone` |
| Staging folder | `staging_folder` = `mtc/staging/lm2fms` | `staging_dir` = `/opt/nifi/data/lm2fms` | `lm.staging.dir` = `./data/lm2fms` |
| **Replay from a time** | `overrideBeginTs` | `override_begin_ts` | `lm.overrideBeginTs` |
| FMS calls in parallel | ForEach `batchCount` 12 | `CallFmsLocation` concurrent tasks 12 | `fms.parallelism` = 12 |

The replay value is a Central time in the form `yyyy-MM-dd HH:mm:ss.SSS`. It replaces the watermark for one run
only. That run then advances the watermark as usual.

---

## Connections

| Used for | Fabric | NiFi | Camel |
|---|---|---|---|
| LocationManagement (read) | Script activity on the LM SQL Server connection | DBCP service, `lm_jdbc_url` | `lmDataSource` (mssql-jdbc) |
| FMS DB2 (calls) | ODBC connection `0a9acb5b…` through the gateway (DSN FMS-FABRIC-EAI-PROD) | DBCP service, `fms_jdbc_url` (jt400, pooled) | `fmsDataSource` (jt400, not pooled) |
| Mail | `sendEmail` UDF (SendGrid API) | PutEmail through the SendGrid SMTP relay | `smtp://smtp.sendgrid.net:587` |
| Staging and logs | lakehouse `lh_mtc_integration` | local disk | local disk |

---

## Where the data lives

| What | Fabric | NiFi | Camel |
|---|---|---|---|
| Watermark | `Files/mtc/staging/lm2fms/watermark.json` | `#{staging_dir}/watermark.json` | `./data/lm2fms/watermark.json` |
| Item log (one row per location) | `Files/mtc/staging/lm2fms/items/<run id>/items.json` | `#{staging_dir}/items/<run id>/` | `./data/lm2fms/items/<run id>/items.json` |
| Run log | `…/runlog/<run id>.json`, then `etl_run_log` after `pl_log_sweep` | `#{staging_dir}/runlog/<run id>.json` | `./data/lm2fms/runlog/<run id>.json` |
| Warehouse views | `vw_etl_runs`, `vw_etl_items` (after the sweep) | none | none |

The watermark file looks like `{"nextBeginTs": "2026-11-05 06:15:00.123", "nextBeginId": -1, "runId": "…"}`.
A `nextBeginId` of 0 or more means the last run hit the 400 cap partway through a group of locations with the
same timestamp. The next run continues after that location ID.

---

## Failures and what to do

### Fabric (PROD)

| What you see | What it means | What to do |
|---|---|---|
| Mail **"FMS refused the location change"** (BUSINESS, amber) with a table of location, outcome and FMS reason. Run is green. | `fms_error`: FMS turned the data down. The watermark advanced, so nothing retries it. | Send the table to the LM data owners and ask them to fix the field in LocationManagement. Once fixed, an IT (Fabric pipelines) person replays with `overrideBeginTs` set just before that location's change time. See [Replay](#replay-a-window). |
| `Fax number contains invalid numeric data` | The LM fax holds characters FMS rejects, such as `(`, `)`, `.`, `+`, `x` or letters. The pipeline sends the fax without checking it ([code gap](#known-code-gaps)). | As above. In LM, check the **oldest** primary contact row of the location, because that is the one the query reads (`ORDER BY c.DateModified`). |
| `DLS0313` (phone) | FMS rejected the phone. The pipeline already swaps a blank or zero phone for `default_contact_phone`. | Ask the LM data owners to fix the phone. |
| Mail **"run started with an invalid parameter"** (`RUN_PARAMETER_INVALID`) whose detail says `advanceWatermark … nextBeginTs is not 'yyyy-MM-dd HH:mm:ss.SSS': ''` | **Wrong label.** At least one location ended `technical`, so the pipeline held the watermark on purpose. | 1. Open the run → `FinalizeLocations` output and check that `technical` is above 0 and `watermarkHeld` is true. 2. In the `outcomesJson` input, decode `messageB64` (base64) for each location. 3. Read the error texts in the table below. 4. Do **not** set `overrideBeginTs`, and do not edit `watermark.json`; the next run resends the window. 5. If the next run fails the same way, follow the "same location keeps failing" row. |
| Decoded message: `[28000] … CWBSY1008 … rc=8007` / `Communication link failure` | The IBM i sign-in or connection failed for that call. | One run: nothing. If it repeats: IT (FMS) for the service account sign-in, and IT (Fabric platform) for the gateway. |
| Decoded message is blank | The call returned no message (dropped or timed out). Counted as `technical`. | Same as above. If many blanks appear together, it was a short outage. |
| Decoded message: `HYT00`, `CWBCO1049`, `08004` | FMS was down. | Sunday 21:00 to midnight Central: nothing. Otherwise, escalate to IT (FMS) after 30 min. |
| The same location keeps failing `technical` on every run | One bad record is blocking the window. Every run fails, and nothing behind it moves. | Escalate to IT (Fabric pipelines) with the location ID and the decoded text. They decide whether to skip it with `overrideBeginTs`. |
| `WATERMARK_UNUSABLE` (`FailOnWatermarkError`) | `watermark.json` is malformed or ahead of the clock. Nothing was read. | Escalate to IT (Fabric pipelines). They write a valid `nextBeginTs` by hand. Deleting the file only seeds 30 min back, so changes older than that would be skipped. |
| `BUILD_FAILED` | The read was cut off (no sentinel row), the batch was too big (over 1.5 MB), or the clocks differ by more than 20 min. Nothing was sent. | Over 1.5 MB: IT lowers `max_locations_per_run`. Clock difference: IT (Fabric pipelines). Otherwise the next run retries. |
| `ERROR: … FAILED (watermark held)` (`SendUpstreamFailureEmail`) | The run failed before outcomes were logged. Some locations may already have reached FMS. | Nothing for one run; the window is resent. If it happens twice in a row, IT (Fabric pipelines). |
| Watchdog `Quiet path: 9_pl_LM_To_FMS_Locations has not advanced its watermark … 2880 min` | No green p9 run for 48 h. | Open the latest p9 run and follow its row above. If there has been no run at all, check the schedule (IT (Fabric pipelines)). |

### NiFi (POC)

| What you see | What it means | What to do |
|---|---|---|
| ERROR bulletin on `CallFmsLocation` for **every** location | Normal. ExecuteSQL routes a SIGNAL to `failure`, and the result is read from `executesql.error.message`. | Nothing. |
| Mail `ERROR: NiFi: lm-to-fms-locations FAILED (watermark held)` | A step routed to `MarkFailed`. Its `lm.error` attribute says which one. If it came from `AdvanceWatermark`, a location was `technical`. | Read `lm.error` in Data Provenance for that FlowFile, then follow the matching Fabric row. |
| Mail `BUSINESS: NiFi: … N location(s) refused by FMS` | `fms_error` rows. | Same as the Fabric BUSINESS row. |
| FlowFiles waiting in `GatherOutcomes` | A location never came back, for example a hung DB2 call. MergeContent waits up to 20 min, and then `FinalizeLocations` fails because the count is wrong. | Wait 20 min. Never empty the queue: the watermark is held, and the next run resends the window. |
| Two runs reading the same window | `GenerateFlowFile` fires every 15 min even while a run is still going. | Lengthen the schedule, or add a Wait/Notify guard ([gaps](#parity-what-matches-and-what-does-not)). |
| Replay | — | Set the `override_begin_ts` parameter and let one run go, then **clear it again**. |

### Kaoto / Camel (POC)

| What you see | What it means | What to do |
|---|---|---|
| Mail `ERROR: Camel: lm-to-fms-locations FAILED (watermark held)` | An exception reached `onException`. The body's `Reason:` line names it: `WATERMARK_UNUSABLE`, `BUILD_FAILED`, `expected N location outcome(s)`, or `nextBeginTs is not …` (a technical result held the watermark). | Follow the matching Fabric row. The run log `runlog/<run id>.json` has `status: Failed` and `error`. |
| Mail `BUSINESS: Camel: … N location(s) refused by FMS` | `fms_error` rows. | Same as the Fabric BUSINESS row. |
| Run appears to hang | A DB2 call is stuck. The `jdbc:` endpoint has no query timeout ([gaps](#parity-what-matches-and-what-does-not)). | Check FMS. Restarting the app is safe: the watermark advances only at the end. |
| Replay | — | Set `lm.overrideBeginTs` in `application.properties` (or `-Dlm.overrideBeginTs=…`), restart, let one run go, then **clear it again**. |

---

## Replay a window

Who: IT (Fabric pipelines). Use this only when the latest run is **green** and you need locations from an
earlier window sent again, for example after LM data was corrected.

1. **Find the time.** Find when the location changed in LM:
   ```sql
   SELECT LocationId, COALESCE(DateModified, DateCreated) AS ChangedTs
   FROM dbo.Location WITH (NOLOCK)
   WHERE LocationId IN (<ids>);
   ```
2. **Correct the data.** In LM, fix the field on the primary contact the pipeline reads. That is the **oldest**
   primary contact row:
   ```sql
   SELECT ContactId, Phone, Fax, IsPrimaryContact, DateModified
   FROM dbo.Contact WITH (NOLOCK)
   WHERE LocationId = <id> AND IsPrimaryContact = 1
   ORDER BY DateModified;
   ```
   Editing only `Contact` does not change `Location.DateModified`, so the location will **not** appear in a
   new window by itself. That is why the replay is needed.
3. **Run once with the override.** Set the replay parameter to just before the earliest `ChangedTs`, in the
   form `yyyy-MM-dd HH:mm:ss.SSS`, and start one run:
   - Fabric: **Run** with the parameter set, without saving the pipeline.
   - NiFi: set the parameter, let one run go, then clear it.
   - Camel: set the property, restart, let one run go, then clear it.

What a replay sends: every location changed from that time to now, up to 400 per run. FMS procedures are
idempotent. A replay never skips anything.

**How you know it worked:** in the item log of the replay run, the location shows `sent`.

---

## Parity: what matches and what does not

**Matches (checked 13 Nov 2026)**
- **NiFi Python code:** byte-identical to the current UDF. `build_nifi.py` regenerated it with no diff.
- **NiFi end-to-end:** the simulated run (`parity/nifi_sim.py`) gives the same results as the UDF.
- **Kaoto/Camel Java:** 14 of 14 parity keys match the UDF, including byte-identical DB2 SQL for each location,
  window and watermark handling, and result classification.
- **LM queries:** Camel uses Fabric's two queries verbatim. NiFi folds the requirements into one query
  (`FOR JSON`) with the same filters, mapping and order.

**Does not match yet**

| # | Gap | Fabric | NiFi | Camel | Impact |
|---|---|---|---|---|---|
| 1 | Digest mail when a location is `technical` | Digest still sent; the run then goes red at AdvanceWatermark | Only the generic "FAILED (watermark held)" mail | Same as NiFi | Support loses the per-location detail in exactly the case that needs it. |
| 2 | Timeouts | FMS call 3 min, then `technical`; LM lookup 20 min | Same (`Max Wait Time` 3 min / 20 min) | Same (`statement.queryTimeout=180` on the FMS call, `template.queryTimeout=1200` on the LM lookups) | Matches now. Bean steps (watermark, build, finalize) run in memory, so they have no timeout. |
| 3 | Retries on read, build, finalize and watermark steps | 5 retries each, 30 s apart | LM lookup only: 5 retries, 30 s penalty. Python processors: none | Any failing step: 5 retries, 30 s apart (`onException RetryTransientFailure`). Deliberate stops (`FailOn*`, held watermark) and mail errors: none | NiFi still fails the whole run on a transient watermark-file or finalize error; the next run recovers, 15 min later. |
| 4 | Overlapping runs | never (`concurrency 1`) | possible | never (`quartz` with `stateful=true`) | NiFi can read the same window twice. |
| 5 | Alert format | PROD sends incident cards ("FMS refused the location change", Recovered mails, daily summary). The repo pipeline still sends the old BUSINESS subject. | old subject | old subject | The ports copy the **repo** version. PROD runs newer code: its traceback points at line 1019 of a 908-line repo file. |
| 6 | WARNING mail for known FMS refusals | yes (the list is currently empty) | no | no | No effect until the list gets entries. |
| 7 | Logging to `etl_run_log`, the views and Power BI | yes | JSON files only | JSON files only | The ports don't appear in the warehouse views or the Power BI report. |
| 8 | Watchdog quiet-path check | yes | no | no | Nothing alerts if a port stops. |
| 9 | Digest mail fails after the watermark has advanced | — | — | `onException` sends "watermark held", which is untrue at that point | A misleading mail. |

**Conclusion:** the business logic is a true like-for-like copy, but **of the repo version**, not PROD. The
orchestration around it (gaps 1–4, 9) is **not yet like-for-like**. Close those gaps before any side-by-side
run against PROD, and get the deployed PROD UDF so the ports can copy that version.

---

## Known code gaps (all three runtimes, inherited from the UDF)

- **Fax not checked:** `_fax` strips only spaces and dashes, then cuts to 10 characters. A fax such as
  `(555) 123-4567` reaches FMS as `(555)12345`, and FMS refuses the location (location 149471, 4 Nov 2026).
  Phone is checked; fax is not.
- **Fax cut from the wrong end:** the fax keeps its first 10 characters, while the phone keeps its last 10.
- **Misleading digest text:** the digest always says "The watermark advanced", even when it was held.
- **Removed requirements stay in FMS:** a location whose LM requirements were all removed never gets the `D`
  call. This copies the legacy behaviour. The UDF comment points to a `KNOWN_GAPS.md` that is not in this repo.
- **Contact edits are invisible:** only `Location.DateModified` opens a window, so an edit to `Contact` alone is
  never picked up.

---

## Diagnose in 3 steps

1. **Run:**
   - Fabric: Monitor → `9_pl_LM_To_FMS_Locations` → the run → the first red step.
   - NiFi: bulletins, and Data Provenance on `MarkFailed`.
   - Camel: the log and `runlog/<run id>.json`.
2. **Results:** the `FinalizeLocations` output (Fabric) or `items/<run id>/items.json` (all three) gives each
   location's result, code and text. For Fabric `technical` rows, decode `messageB64` in the `outcomesJson`
   input.
3. **State:** `watermark.json`. Its `nextBeginTs` should be close to the last green run's end. If it is stuck
   while runs fail, the window is held and is being resent on each run.

## Who to call

| Problem | Who |
|---|---|
| Wrong LM data (fax, phone, state) | LocationManagement data owners |
| FMS down, or FMS sign-in errors | IT (FMS) |
| Watermark repair, replay, schedule, a location blocking every run | IT (Fabric pipelines) |
| Gateway, connection tokens, UDF `InternalServerError` on 3 runs in a row | IT (Fabric platform) |
| NiFi host | IT (NiFi host) |
