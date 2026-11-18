import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;

/**
 * Camel bean ("lmFms" in the route) wrapping LmFmsCore. Each method mirrors one
 * Fabric UDF entry point and moves its results onto exchange properties so the
 * Kaoto canvas can branch on them (CheckWatermarkError, CheckBuildError, digest).
 */
public class LmFmsLocations {

    public static final String PIPELINE = "lm-to-fms-locations";

    private static String prop(Exchange ex, String key) {
        return ex.getContext().resolvePropertyPlaceholders("{{" + key + "}}");
    }

    /**
     * ReadWatermark: reads {lm.staging.dir}/watermark.json like the Fabric UDF does.
     * Only a missing file seeds; any other read error fails the run (watermark held).
     * An override skips the file entirely.
     */
    public void readWindow(Exchange ex) {
        String override = prop(ex, "lm.overrideBeginTs");
        Map<String, Object> w;
        try {
            String raw = null;
            if (override.isBlank()) {
                Path file = Path.of(prop(ex, "lm.staging.dir"), "watermark.json");
                raw = Files.exists(file) ? Files.readString(file) : null;
            }
            w = LmFmsCore.readWindow(raw, LmFmsCore.nowCentral(), override);
        } catch (Exception e) {
            w = LmFmsCore.windowError("watermark read failed: " + LmFmsCore.cut(e.getMessage(), 300));
        }
        w.forEach(ex::setProperty);
        ex.setProperty("watermarkError", w.get("error"));
        // camel-sql binds :#name from headers
        ex.getIn().setHeader("beginTs", w.get("beginTs"));
        ex.getIn().setHeader("endTs", w.get("endTs"));
        ex.getIn().setHeader("beginId", w.get("beginId"));
        ex.getIn().setHeader("maxLocations", Integer.parseInt(prop(ex, "lm.maxLocationsPerRun")));
        ex.getIn().setBody(null);
    }

    /** BuildLocationCalls: locations from property lmLocations, requirements in the body. */
    @SuppressWarnings("unchecked")
    public void buildCalls(Exchange ex) {
        Map<String, Object> r = LmFmsCore.buildCalls(
                ex.getProperty("lmLocations"), ex.getIn().getBody(),
                prop(ex, "fms.library"), prop(ex, "fms.defaultContactPhone"),
                Integer.parseInt(prop(ex, "lm.maxLocationsPerRun")),
                ex.getProperty("endTs", String.class));
        ex.setProperty("buildError", r.get("error"));
        ex.setProperty("callCount", r.get("callCount"));
        ex.setProperty("locationCount", r.get("locationCount"));
        ex.setProperty("requirementCount", r.get("requirementCount"));
        ex.setProperty("capped", r.get("capped"));
        ex.setProperty("skipped", r.get("skipped"));
        ex.setProperty("nextBeginTs", r.get("nextBeginTs"));
        ex.setProperty("nextBeginId", r.get("nextBeginId"));
        ex.removeProperty("lmLocations");
        ex.getIn().removeHeaders("*");
        ex.getIn().setBody(r.get("calls"));
    }

    /**
     * RecordLocationOutcome: runs after the DB2 call in both the try and the catch.
     * The compound block always ends in SIGNAL, so the SQLException text is the outcome.
     */
    public void outcome(Exchange ex) {
        Throwable t = ex.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
        String message = "";
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof SQLException) {
                message = c.getMessage();
                break;
            }
        }
        if (message.isEmpty() && t != null) {
            message = String.valueOf(t.getMessage());
        }
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("locationId", ex.getProperty("locationId", String.class));
        o.put("requirementCount", ex.getProperty("requirementCount"));
        o.put("message", message);
        ex.getIn().setBody(o);
    }

    /** FinalizeLocations: body is the split's grouped outcome list. */
    @SuppressWarnings("unchecked")
    public void finalizeLocations(Exchange ex) {
        List<Map<String, Object>> outcomes = new ArrayList<>();
        Object body = ex.getIn().getBody();
        if (body instanceof List) {
            for (Object o : (List<?>) body) {
                if (o instanceof Map && ((Map<?, ?>) o).containsKey("message")) {
                    outcomes.add((Map<String, Object>) o);
                }
            }
        }
        List<Map<String, Object>> skipped = (List<Map<String, Object>>) ex.getProperty("skipped", List.class);
        Map<String, Object> r = LmFmsCore.finalizeLocations(outcomes, skipped == null ? List.of() : skipped,
                ex.getProperty("runId", String.class), PIPELINE,
                ex.getProperty("beginTs", String.class), ex.getProperty("endTs", String.class),
                ex.getProperty("callCount", Integer.class), ex.getProperty("nextBeginTs", String.class),
                ex.getProperty("nextBeginId"));
        for (String k : List.of("sent", "fmsErrors", "technical", "digestBody", "hasFailures",
                "nextBeginTs", "nextBeginId", "watermarkHeld")) {
            ex.setProperty(k, r.get(k));
        }
        ex.setProperty("skippedCount", r.get("skipped"));
        ex.getIn().setBody(Json.write(r.get("items")));
    }

    /** AdvanceWatermark: throws on a held (blank) nextBeginTs so the run reds and the window replays. */
    public void advanceWatermark(Exchange ex) {
        ex.getIn().setBody(LmFmsCore.watermarkDoc(ex.getProperty("nextBeginTs", String.class),
                ex.getProperty("nextBeginId"), ex.getProperty("runId", String.class)));
    }

    /** SendUpstreamFailureEmail body; also flags the run for the run log. */
    public void failureMail(Exchange ex) {
        Throwable t = ex.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
        String reason = t == null ? "unknown" : t.getClass().getSimpleName() + ": " + t.getMessage();
        ex.setProperty("runError", reason);
        ex.getIn().removeHeaders("*");
        ex.getIn().setBody(PIPELINE + " failed before the watermark was advanced. "
                + "Some locations may already have reached FMS.\n\n"
                + "Run: " + ex.getProperty("runId") + "\n"
                + "Window: " + ex.getProperty("beginTs") + " .. " + ex.getProperty("endTs") + "\n"
                + "Reason: " + reason + "\n\n"
                + "The watermark was NOT advanced -- this window will be re-read on the next run.");
    }

    /** InvokeLogging: one run-log row on every path (onCompletion). */
    public void runLog(Exchange ex) {
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("run_id", ex.getProperty("runId"));
        d.put("pipeline", PIPELINE);
        d.put("ts_utc", LmFmsCore.nowUtc());
        String err = ex.getProperty("runError", String.class);
        d.put("status", err == null ? "Succeeded" : "Failed");
        d.put("error", err == null ? "" : err);
        for (String k : List.of("locationCount", "requirementCount", "capped", "sent", "fmsErrors",
                "technical", "skippedCount", "beginTs", "endTs", "nextBeginTs", "watermarkHeld")) {
            d.put(k, ex.getProperty(k));
        }
        ex.getIn().setBody(Json.write(d));
    }
}
