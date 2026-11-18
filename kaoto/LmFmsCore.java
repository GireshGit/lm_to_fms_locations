import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Camel-free port of udf_lm_fms_locations (Fabric) for the Kaoto/Camel POC.
 * Same field mapping, DB2 compound-block assembly, outcome classification and
 * watermark rules; ParityCheck.java + parity_check.py diff it against the UDF.
 */
public final class LmFmsCore {

    private LmFmsCore() {
    }

    public static final String FMS_USER = "TMEAISERV ";
    static final int REQS_PER_CALL = 50;
    static final int REQ_TEXT_WIDTH = 100;
    static final int REQ_TYPE_PARAM_WIDTH = REQS_PER_CALL;
    static final int REQ_TEXT_PARAM_WIDTH = REQS_PER_CALL * REQ_TEXT_WIDTH;
    static final int LOCATION_NUMBER_WIDTH = 7;
    static final String FALLBACK_PHONE = "0000000001";
    static final String SIGNAL_SQLSTATE = "75005";
    static final String OK_MARKER = "P9OK";
    static final String ERR_MARKER = "P9ERR";
    static final int MESSAGE_TEXT_MAX = 70;
    static final String OK_SQL = "'P9' || 'OK'";
    static final String ERR_SQL = "'P9' || 'ERR '";
    static final int MAX_CALLS_BYTES = 1_500_000;
    static final String SENTINEL_ID = "-1";
    public static final int NO_BEGIN_ID = -1;
    static final int MAX_CLOCK_SKEW_MINUTES = 20;
    static final int SEED_LOOKBACK_MINUTES = 30;
    static final ZoneId CENTRAL = ZoneId.of("America/Chicago");

    static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS").withResolverStyle(ResolverStyle.STRICT);
    static final Pattern TS_RE = Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}");
    static final Pattern CTRL_RE = Pattern.compile("[\\r\\n\\t\\x00-\\x1f]");
    static final Pattern NUMBER_RE = Pattern.compile(
            "[+-]?((\\d+\\.?\\d*|\\.\\d+)([eE][+-]?\\d+)?|inf|infinity|nan)", Pattern.CASE_INSENSITIVE);

    static final String FMS_LIBL_PREFIX =
            "BEGIN DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN END; "
            + "CALL QSYS2.QCMDEXC('CHGJOB INQMSGRPY(*DFT)'); "
            + "CALL QSYS2.QCMDEXC('ADDLIBLE LIB(FMSSEC) POSITION(*FIRST)'); END; ";

    // ------------------------------------------------------------ timestamps

    public static boolean validTs(Object s) {
        if (!(s instanceof String) || !TS_RE.matcher((String) s).matches()) {
            return false;
        }
        try {
            LocalDateTime.parse((String) s, TS_FMT);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    static LocalDateTime parseTs(String s) {
        return LocalDateTime.parse(s, TS_FMT);
    }

    static String fmtTs(LocalDateTime dt) {
        return dt.format(TS_FMT);
    }

    /** Fabric's convertFromUtc(TriggerTime, 'Central Standard Time'), DST-aware. */
    public static String nowCentral() {
        return fmtTs(LocalDateTime.now(CENTRAL));
    }

    public static String nowUtc() {
        return fmtTs(LocalDateTime.now(ZoneOffset.UTC));
    }

    // ------------------------------------------------------------ watermark

    static Map<String, Object> window(String beginTs, String endTs, boolean seeded, String error, int beginId) {
        Map<String, Object> w = new LinkedHashMap<>();
        w.put("beginTs", beginTs);
        w.put("endTs", endTs);
        w.put("seeded", seeded);
        w.put("error", error);
        w.put("beginId", beginId);
        return w;
    }

    static Map<String, Object> windowError(String error) {
        return window("", "", false, error, NO_BEGIN_ID);
    }

    /** readWatermark: override wins and touches no file; absent file seeds 30 min back. */
    public static Map<String, Object> readWindow(String rawJson, String endTs, String overrideBeginTs) {
        String override = s(overrideBeginTs).strip();
        if (!override.isEmpty()) {
            if (!validTs(override)) {
                return windowError("overrideBeginTs is not 'yyyy-MM-dd HH:mm:ss.SSS': '" + overrideBeginTs + "'");
            }
            if (!validTs(endTs)) {
                return windowError("endTs is not 'yyyy-MM-dd HH:mm:ss.SSS': '" + endTs + "'");
            }
            return window(override, endTs, false, "", NO_BEGIN_ID);
        }
        try {
            return computeWindow(rawJson, endTs);
        } catch (Exception e) {
            return windowError("watermark window computation failed: " + cut(e.getMessage(), 300));
        }
    }

    static Map<String, Object> computeWindow(String rawJson, String endTs) {
        if (!validTs(endTs)) {
            return windowError("endTs is not 'yyyy-MM-dd HH:mm:ss.SSS': '" + endTs + "'");
        }
        LocalDateTime end = parseTs(endTs);
        if (rawJson == null || rawJson.strip().isEmpty()) {
            return window(fmtTs(end.minusMinutes(SEED_LOOKBACK_MINUTES)), endTs, true, "", NO_BEGIN_ID);
        }
        Object doc;
        try {
            doc = Json.parse(rawJson);
        } catch (Exception e) {
            return windowError("watermark unparseable: " + cut(e.getMessage(), 200));
        }
        if (!(doc instanceof Map)) {
            return windowError("watermark is not a JSON object");
        }
        Object begin = ((Map<?, ?>) doc).get("nextBeginTs");
        if (!validTs(begin)) {
            return windowError("watermark nextBeginTs invalid: '" + begin + "'");
        }
        if (parseTs((String) begin).isAfter(end)) {
            return windowError("window begin " + begin + " is after end " + endTs
                    + " (clock moved backwards or the watermark is ahead of the trigger time)");
        }
        return window((String) begin, endTs, false, "", asBeginId(((Map<?, ?>) doc).get("nextBeginId")));
    }

    public static int asBeginId(Object value) {
        if (value == null || "".equals(value)) {
            return NO_BEGIN_ID;
        }
        long parsed;
        try {
            if (value instanceof Number) {
                parsed = ((Number) value).longValue();
            } else if (value instanceof Boolean) {
                parsed = ((Boolean) value) ? 1 : 0;
            } else {
                parsed = Long.parseLong(value.toString().strip());
            }
        } catch (Exception e) {
            return NO_BEGIN_ID;
        }
        return parsed >= 0 && parsed <= Integer.MAX_VALUE ? (int) parsed : NO_BEGIN_ID;
    }

    /** advanceWatermark: the commit point; a blank nextBeginTs (technical failure) refuses to advance. */
    public static String watermarkDoc(String nextBeginTs, Object nextBeginId, String runId) {
        if (!validTs(nextBeginTs)) {
            throw new IllegalArgumentException("nextBeginTs is not 'yyyy-MM-dd HH:mm:ss.SSS': '" + nextBeginTs + "'");
        }
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("nextBeginTs", nextBeginTs);
        doc.put("nextBeginId", asBeginId(nextBeginId));
        doc.put("runId", s(runId));
        return Json.write(doc);
    }

    // --------------------------------------------------------- field mapping

    static String s(Object value) {
        return value == null ? "" : value.toString();
    }

    static String cut(String value, int n) {
        String v = s(value);
        return v.length() > n ? v.substring(0, n) : v;
    }

    static String ljust(String value, int width) {
        StringBuilder b = new StringBuilder(value);
        while (b.length() < width) {
            b.append(' ');
        }
        return b.toString();
    }

    static String fit(Object value, int width) {
        return ljust(cut(s(value), width), width);
    }

    static String locationNumber(Object locationId) {
        String raw = s(locationId).strip();
        if (raw.isEmpty() || !raw.chars().allMatch(Character::isDigit) || raw.length() > LOCATION_NUMBER_WIDTH) {
            return null;
        }
        return "0".repeat(LOCATION_NUMBER_WIDTH - raw.length()) + raw;
    }

    static String country(Object value) {
        String raw = s(value);
        if (raw.strip().toUpperCase().equals("US")) {
            raw = "USA";
        }
        return fit(raw, 3);
    }

    static String postal(Object value) {
        String raw = s(value).strip();
        return raw.isEmpty() ? "000000000" : fit(raw.replace("-", ""), 9);
    }

    static boolean isNumber(String raw) {
        return NUMBER_RE.matcher(raw).matches();
    }

    static boolean isAllZero(String raw) {
        return !raw.isEmpty() && raw.chars().allMatch(c -> c == '0');
    }

    static String digitsOnly(Object value) {
        return s(value).strip().replace(" ", "").replace("-", "");
    }

    static String phone(Object value, String defaultPhone) {
        String raw = digitsOnly(value);
        if (raw.isEmpty() || !isNumber(raw) || isAllZero(raw)) {
            raw = digitsOnly(defaultPhone);
        }
        if (raw.isEmpty() || isAllZero(raw)) {
            raw = FALLBACK_PHONE;
        }
        if (raw.length() > 10) {
            raw = raw.substring(raw.length() - 10);
        }
        return ljust(raw, 10);
    }

    static String fax(Object value) {
        String raw = digitsOnly(value);
        return raw.isEmpty() ? "0000000000" : fit(raw, 10);
    }

    static String status(Object value) {
        String raw = s(value).strip();
        if (raw.isEmpty()) {
            return "A";
        }
        return raw.charAt(0) == '0' ? "I" : "A";
    }

    static String parent(Object value) {
        String raw = s(value).strip();
        return raw.isEmpty() ? "0000000" : fit(raw, LOCATION_NUMBER_WIDTH);
    }

    static String facilityKey(Object value, int width) {
        String raw = s(value).strip();
        return raw.isEmpty() ? ljust("", width) : fit(raw, width);
    }

    /** The 20 input parameters of Fmslib.Add_Update_Locations in call order; null when the id won't fit 7 digits. */
    static List<String> mapLocation(Map<String, Object> row, String defaultPhone) {
        String number = locationNumber(row.get("LocationId"));
        if (number == null) {
            return null;
        }
        String name = s(row.get("LocationName"));
        String city = s(row.get("City"));
        String state = s(row.get("State")).strip();
        List<String> p = new ArrayList<>();
        p.add(number);
        p.add(fit(name, 30));
        p.add(fit(row.get("StreetAddress1"), 30));
        p.add(fit(row.get("StreetAddress2"), 30));
        p.add(fit(city, 18));
        p.add(fit(state, 2));
        p.add(country(row.get("Country")));
        p.add(postal(row.get("PostalCode")));
        p.add(phone(row.get("Phone"), defaultPhone));
        p.add(fit(row.get("ContactName"), 30));
        p.add(fax(row.get("Fax")));
        p.add(status(row.get("Status")));
        p.add(fit(row.get("LocationCategoryName"), 10));
        p.add(fit(row.get("LocationCategoryDesc"), 30));
        p.add(parent(row.get("ParentId")));
        p.add(fit(name, 28));
        p.add(fit(city, 28));
        p.add(facilityKey(row.get("FacilityDropsite"), 7));
        p.add(facilityKey(row.get("FacilityLocaleKey"), 9));
        p.add(FMS_USER);
        return p;
    }

    // ------------------------------------------------------ DB2 block assembly

    static String lit(String value) {
        String v = CTRL_RE.matcher(value).replaceAll(" ");
        int end = v.length();
        while (end > 0 && v.charAt(end - 1) == ' ') {
            end--;
        }
        return "'" + v.substring(0, end).replace("'", "''") + "'";
    }

    static String call(String proc, List<String> params) {
        List<String> lits = new ArrayList<>();
        for (String p : params) {
            lits.add(p == null ? "NULL" : lit(p));
        }
        return "SET V_ERRCODE = ''; SET V_ERRSTR = ''; CALL " + proc + "(" + String.join(", ", lits)
                + ", V_ERRCODE, V_ERRSTR);";
    }

    static String capture(String step) {
        return "IF V_MSG = " + OK_SQL + " AND (COALESCE(TRIM(V_ERRCODE), '') <> '' "
                + "OR COALESCE(TRIM(V_ERRSTR), '') <> '') THEN "
                + "SET V_MSG = LEFT(" + ERR_SQL + " || '" + step + " ' || TRIM(V_ERRCODE) || ' ' || TRIM(V_ERRSTR), "
                + MESSAGE_TEXT_MAX + "); END IF;";
    }

    static List<String[]> requirementBatches(List<Map<String, Object>> requirements) {
        List<String[]> batches = new ArrayList<>();
        for (int start = 0; start < requirements.size(); start += REQS_PER_CALL) {
            StringBuilder types = new StringBuilder();
            StringBuilder texts = new StringBuilder();
            for (Map<String, Object> req : requirements.subList(start, Math.min(start + REQS_PER_CALL, requirements.size()))) {
                String t = s(req.get("ReqType"));
                types.append(t.length() == 1 ? t : " ");
                texts.append(fit(req.get("ReqText"), REQ_TEXT_WIDTH));
            }
            batches.add(new String[] {ljust(types.toString(), REQ_TYPE_PARAM_WIDTH),
                ljust(texts.toString(), REQ_TEXT_PARAM_WIDTH)});
        }
        return batches;
    }

    /** {sql, skipReason}: one location's FMS work as one dynamic compound statement. */
    public static String[] buildLocationBlock(Map<String, Object> row, List<Map<String, Object>> requirements,
            String fmsLibrary, String defaultPhone) {
        List<String> params = mapLocation(row, defaultPhone);
        if (params == null) {
            return new String[] {"", "location_id_unmappable"};
        }
        String number = params.get(0);
        String addUpdate = fmsLibrary + ".ADD_UPDATE_LOCATIONS";
        String reqUpdate = fmsLibrary + ".LOCATION_REQUIREMENTS_UPDATE";

        List<String> body = new ArrayList<>();
        body.add(call(addUpdate, params));
        body.add(capture("AU"));
        if (!requirements.isEmpty()) {
            body.add(call(reqUpdate, List.of("D", number, " ".repeat(REQ_TYPE_PARAM_WIDTH),
                    " ".repeat(REQ_TEXT_PARAM_WIDTH), FMS_USER)));
            body.add(capture("RD"));
            for (String[] batch : requirementBatches(requirements)) {
                body.add(call(reqUpdate, List.of("A", number, batch[0], batch[1], FMS_USER)));
                body.add(capture("RA"));
            }
        }
        String sql = "BEGIN "
                + "DECLARE V_ERRCODE VARCHAR(256) DEFAULT ''; "
                + "DECLARE V_ERRSTR VARCHAR(1024) DEFAULT ''; "
                + "DECLARE V_MSG VARCHAR(" + MESSAGE_TEXT_MAX + ") DEFAULT ''; "
                + FMS_LIBL_PREFIX
                + "SET V_MSG = " + OK_SQL + "; "
                + String.join(" ", body) + " "
                + "SIGNAL SQLSTATE '" + SIGNAL_SQLSTATE + "' SET MESSAGE_TEXT = V_MSG; "
                + "END";
        return new String[] {sql, ""};
    }

    // ---------------------------------------------------------- LM result sets

    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> rows(Object raw) {
        List<Map<String, Object>> out = new ArrayList<>();
        if (raw == null) {
            return out;
        }
        if (!(raw instanceof List)) {
            throw new IllegalArgumentException("result set is not a list");
        }
        for (Object r : (List<?>) raw) {
            if (r instanceof Map) {
                out.add((Map<String, Object>) r);
            }
        }
        return out;
    }

    static Map<String, Object> splitSentinel(List<Map<String, Object>> rows, List<Map<String, Object>> data,
            String what) {
        Map<String, Object> sentinel = null;
        for (Map<String, Object> row : rows) {
            if (SENTINEL_ID.equals(s(row.get("LocationId")).strip())) {
                sentinel = row;
            } else {
                data.add(row);
            }
        }
        if (sentinel == null) {
            throw new IllegalArgumentException(what + " result set has no sentinel row -- the payload was "
                    + "truncated or silently emptied");
        }
        return sentinel;
    }

    static String clockSkewError(String serverTs, String endTs) {
        if (!validTs(endTs)) {
            return "";
        }
        if (!validTs(serverTs)) {
            return "the locations sentinel carried no usable server clock ('" + serverTs
                    + "'), so a timezone mismatch cannot be ruled out";
        }
        double skew = Math.abs(java.time.Duration.between(parseTs(endTs), parseTs(serverTs)).toMillis()) / 60000.0;
        if (skew > MAX_CLOCK_SKEW_MINUTES) {
            return "LocationManagement's clock reads " + serverTs + " while this run's window ends at " + endTs
                    + ", " + Math.round(skew) + " minutes apart -- the window is being built in a different "
                    + "timezone from the columns it filters, so it can match nothing";
        }
        return "";
    }

    static Map<String, Object> buildResult() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("calls", new ArrayList<>());
        out.put("skipped", new ArrayList<>());
        out.put("locationCount", 0);
        out.put("requirementCount", 0);
        out.put("maxTs", "");
        out.put("serverTs", "");
        out.put("capped", false);
        out.put("bytes", 0);
        out.put("error", "");
        out.put("callCount", 0);
        out.put("nextBeginTs", "");
        out.put("nextBeginId", NO_BEGIN_ID);
        return out;
    }

    /** buildLocationCalls: error comes back as data so the route's CheckBuildError gate shows the reason. */
    public static Map<String, Object> buildCalls(Object locationRows, Object requirementRows, String fmsLibrary,
            String defaultPhone, int maxLocations, String endTs) {
        Map<String, Object> result = buildResult();
        try {
            if (s(fmsLibrary).strip().isEmpty()) {
                result.put("error", "fmsLibrary is blank");
                return result;
            }
            List<Map<String, Object>> locations = new ArrayList<>();
            Map<String, Object> locSentinel = splitSentinel(rows(locationRows), locations, "locations");
            List<Map<String, Object>> requirements = new ArrayList<>();
            splitSentinel(rows(requirementRows), requirements, "requirements");

            String serverTs = s(locSentinel.get("ChangedTs")).strip();
            String skew = clockSkewError(serverTs, endTs);
            if (!skew.isEmpty()) {
                result.put("error", skew);
                result.put("serverTs", serverTs);
                return result;
            }

            Map<String, List<Map<String, Object>>> grouped = new LinkedHashMap<>();
            for (Map<String, Object> r : requirements) {
                grouped.computeIfAbsent(s(r.get("LocationId")).strip(), k -> new ArrayList<>()).add(r);
            }
            List<Map<String, Object>> calls = new ArrayList<>();
            List<Map<String, Object>> skipped = new ArrayList<>();
            String maxTs = "";
            String resumeTs = null;
            long resumeId = 0;
            for (Map<String, Object> row : locations) {
                String id = s(row.get("LocationId"));
                List<Map<String, Object>> reqs = grouped.getOrDefault(id.strip(), List.of());
                String[] block = buildLocationBlock(row, reqs, fmsLibrary, defaultPhone);
                if (!block[1].isEmpty()) {
                    Map<String, Object> sk = new LinkedHashMap<>();
                    sk.put("locationId", id);
                    sk.put("reason", block[1]);
                    skipped.add(sk);
                } else {
                    Map<String, Object> c = new LinkedHashMap<>();
                    c.put("locationId", id);
                    c.put("locationName", cut(s(row.get("LocationName")), 60));
                    c.put("requirementCount", reqs.size());
                    c.put("sql", block[0]);
                    calls.add(c);
                }
                String ts = s(row.get("ChangedTs")).strip();
                if (validTs(ts)) {
                    if (ts.compareTo(maxTs) > 0) {
                        maxTs = ts;
                    }
                    try {
                        long loc = Long.parseLong(id.strip());
                        int cmp = resumeTs == null ? 1 : ts.compareTo(resumeTs);
                        if (cmp > 0 || (cmp == 0 && loc > resumeId)) {
                            resumeTs = ts;
                            resumeId = loc;
                        }
                    } catch (NumberFormatException ignored) {
                        // no resume candidate, as in the UDF
                    }
                }
            }
            int size = Json.write(calls).length();
            if (size > MAX_CALLS_BYTES) {
                result.put("error", "assembled calls are " + size + " bytes, over the " + MAX_CALLS_BYTES
                        + " limit -- lower max_locations_per_run");
                return result;
            }
            boolean capped = maxLocations > 0 && locations.size() >= maxLocations;
            result.put("calls", calls);
            result.put("skipped", skipped);
            result.put("locationCount", locations.size());
            result.put("requirementCount", requirements.size());
            result.put("maxTs", maxTs);
            result.put("serverTs", serverTs);
            result.put("capped", capped);
            result.put("bytes", size);
            result.put("callCount", calls.size());
            if (capped && resumeTs != null) {
                result.put("nextBeginTs", resumeTs);
                result.put("nextBeginId", (int) resumeId);
            } else {
                result.put("nextBeginTs", endTs);
                result.put("nextBeginId", NO_BEGIN_ID);
            }
            return result;
        } catch (Exception e) {
            Map<String, Object> err = buildResult();
            err.put("error", "build failed: " + cut(e.getMessage(), 500));
            return err;
        }
    }

    // ------------------------------------------------------------- outcomes

    /** {outcome, errorCode, errorMessage}; the SIGNAL text is the only channel. */
    public static String[] classifyOutcome(String rawMessage) {
        String msg = s(rawMessage);
        if (msg.contains(OK_MARKER) && !msg.contains(ERR_MARKER)) {
            return new String[] {"sent", "", ""};
        }
        if (msg.contains(ERR_MARKER)) {
            int start = msg.indexOf(ERR_MARKER);
            String detail = msg.substring(start, Math.min(msg.length(), start + MESSAGE_TEXT_MAX)).strip();
            String[] parts = detail.split("\\s+", 3);
            String step = parts.length > 1 ? parts[1] : "";
            String code = parts.length > 2 ? parts[2].split("\\s+", 2)[0] : "";
            return new String[] {"fms_error", stripUnderscore(step + "_" + code), detail};
        }
        return new String[] {"technical", "db2_call_failed", cut(msg, 1000)};
    }

    static String stripUnderscore(String v) {
        int a = 0;
        int b = v.length();
        while (a < b && v.charAt(a) == '_') {
            a++;
        }
        while (b > a && v.charAt(b - 1) == '_') {
            b--;
        }
        return v.substring(a, b);
    }

    static Map<String, Object> itemRecord(String runId, String pipeline, String itemId, String outcome,
            String errorCode, String errorMessage, Map<String, Object> details, String ts) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("run_id", s(runId));
        r.put("pipeline", pipeline);
        r.put("item_type", "location");
        r.put("item_id", s(itemId));
        r.put("ts_utc", ts);
        r.put("outcome", s(outcome));
        r.put("target", "");
        r.put("file_path", "");
        r.put("error_code", s(errorCode));
        r.put("error_message", s(errorMessage));
        r.put("step_statuses", "{}");
        r.put("details", Json.write(details));
        r.put("debug", "{}");
        r.put("log_day", ts.length() >= 10 ? ts.substring(0, 10) : "");
        r.put("log_hour", ts.length() >= 13 ? ts.substring(11, 13) : "");
        return r;
    }

    static List<String> outcomeLines(List<Map<String, Object>> rows) {
        List<String> lines = new ArrayList<>();
        for (Map<String, Object> o : rows) {
            lines.add("  " + s(o.get("locationId")) + "  " + o.get("outcome") + "  " + s(o.get("errorCode")) + "  "
                    + cut(s(o.get("errorMessage")), 200));
        }
        return lines;
    }

    /**
     * finalizeLocations: classify, build item-log rows and digests. A technical outcome blanks
     * nextBeginTs so the window replays; a business (FMS) error still advances.
     */
    public static Map<String, Object> finalizeLocations(List<Map<String, Object>> rawOutcomes,
            List<Map<String, Object>> skipped, String runId, String pipeline, String beginTs, String endTs,
            int expectedCount, String nextBeginTs, Object nextBeginId) {
        String ts = validTs(endTs) ? endTs : nowUtc();
        if (expectedCount >= 0 && rawOutcomes.size() != expectedCount) {
            throw new IllegalStateException("expected " + expectedCount + " location outcome(s), got "
                    + rawOutcomes.size() + " -- an outcome was lost between the FMS call and this call. "
                    + "The watermark is NOT advanced; the window replays.");
        }
        List<Map<String, Object>> outcomes = new ArrayList<>();
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (Map<String, Object> e : rawOutcomes) {
            String[] c = classifyOutcome(s(e.get("message")));
            Map<String, Object> o = new LinkedHashMap<>();
            o.put("locationId", s(e.get("locationId")));
            o.put("requirementCount", e.get("requirementCount"));
            o.put("outcome", c[0]);
            o.put("errorCode", c[1]);
            o.put("errorMessage", c[2]);
            o.put("ts", s(e.get("ts")).isEmpty() ? ts : s(e.get("ts")));
            outcomes.add(o);
            counts.merge(c[0], 1, Integer::sum);
        }

        List<Map<String, Object>> items = new ArrayList<>();
        for (Map<String, Object> o : outcomes) {
            Map<String, Object> d = new LinkedHashMap<>();
            d.put("requirementCount", o.get("requirementCount"));
            d.put("windowBegin", beginTs);
            d.put("windowEnd", endTs);
            items.add(itemRecord(runId, pipeline, s(o.get("locationId")), s(o.get("outcome")),
                    s(o.get("errorCode")), s(o.get("errorMessage")), d, s(o.get("ts"))));
        }
        for (Map<String, Object> sk : skipped) {
            Map<String, Object> d = new LinkedHashMap<>();
            d.put("windowBegin", beginTs);
            d.put("windowEnd", endTs);
            items.add(itemRecord(runId, pipeline, s(sk.get("locationId")), s(sk.get("reason")),
                    s(sk.get("reason")), "", d, ts));
        }

        List<Map<String, Object>> bad = new ArrayList<>();
        for (Map<String, Object> o : outcomes) {
            if (!"sent".equals(o.get("outcome"))) {
                bad.add(o);
            }
        }
        String digest = "";
        if (!bad.isEmpty() || !skipped.isEmpty()) {
            List<String> lines = new ArrayList<>(List.of(pipeline + " reported location failures.", "",
                    "Run: " + runId, "Window: " + beginTs + " .. " + endTs,
                    "Locations attempted: " + outcomes.size() + ", failed: " + bad.size() + ", skipped: "
                            + skipped.size(), ""));
            lines.addAll(outcomeLines(bad));
            for (Map<String, Object> sk : skipped) {
                lines.add("  " + s(sk.get("locationId")) + "  " + s(sk.get("reason")));
            }
            lines.add("");
            lines.add("The watermark advanced: these locations are NOT retried automatically. "
                    + "Re-run with lm.overrideBeginTs to replay the window.");
            digest = String.join("\n", lines);
        }

        int technical = counts.getOrDefault("technical", 0);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("items", items);
        out.put("itemCount", items.size());
        out.put("sent", counts.getOrDefault("sent", 0));
        out.put("fmsErrors", counts.getOrDefault("fms_error", 0));
        out.put("technical", technical);
        out.put("skipped", skipped.size());
        out.put("digestBody", digest);
        out.put("hasFailures", !digest.isEmpty());
        out.put("nextBeginTs", technical > 0 ? "" : s(nextBeginTs));
        out.put("nextBeginId", technical > 0 ? NO_BEGIN_ID : asBeginId(nextBeginId));
        out.put("watermarkHeld", technical > 0);
        return out;
    }
}
