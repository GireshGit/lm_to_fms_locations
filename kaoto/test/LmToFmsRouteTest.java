///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 21+
//DEPS org.apache.camel:camel-bom:4.22.0@pom
//DEPS org.apache.camel:camel-core
//DEPS org.apache.camel:camel-yaml-dsl
//DEPS org.apache.camel:camel-quartz
//DEPS org.apache.camel:camel-sql
//DEPS org.apache.camel:camel-jdbc
//DEPS org.apache.camel:camel-mail
//DEPS com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11
//DEPS net.sf.jt400:jt400:20.0.7
//DEPS org.junit.jupiter:junit-jupiter:5.11.4
//DEPS org.junit.platform:junit-platform-launcher:1.11.4
//DEPS org.slf4j:slf4j-simple:2.0.16
// Camel logs are off (the failure tests log expected stack traces). To see them:
//   jbang -Dorg.slf4j.simpleLogger.defaultLogLevel=info test/LmToFmsRouteTest.java
//JAVA_OPTIONS -Dorg.slf4j.simpleLogger.defaultLogLevel=off
//SOURCES ../LmFmsLocations.java
//SOURCES ../LmFmsCore.java
//SOURCES ../Json.java

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.builder.AdviceWith;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.PluginHelper;
import org.apache.camel.support.ResourceHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.engine.discovery.DiscoverySelectors;

/**
 * End-to-end tests for lm-to-fms-locations.camel.yaml with every external system mocked.
 *
 * The real YAML route is loaded unchanged; AdviceWith swaps only the edges:
 *   quartz:lm2fms       -> direct:start (the test fires one run)
 *   LookupLocations /
 *   LookupRequirements  -> canned LM rows (or an exception for "LM down")
 *   ExecuteDb2Block     -> fake FMS that throws the SIGNAL text a real DB2 block would
 *   SendDigestEmail /
 *   MailFailure         -> mock:digest / mock:failureMail
 * Files (watermark.json, items/, runlog/) go to a temp folder and are asserted for real.
 *
 * Run from the kaoto folder:  jbang test/LmToFmsRouteTest.java
 */
public class LmToFmsRouteTest {

    /** The kaoto folder: -Dkaoto.dir, else found from the working dir (kaoto/, kaoto/test/, or the repo root). */
    static final Path KAOTO = Stream.of(System.getProperty("kaoto.dir", "."), ".", "..", "kaoto",
                    "lm_to_fms_locations/kaoto", "poc/lm_to_fms_locations/kaoto")
            .map(Path::of)
            .filter(p -> Files.exists(p.resolve("lm-to-fms-locations.camel.yaml")))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("cannot find lm-to-fms-locations.camel.yaml from "
                    + Path.of("").toAbsolutePath() + "; run from the kaoto folder or pass -Dkaoto.dir=<path>"))
            .toAbsolutePath().normalize();

    static final String OK = "[SQL0438] P9OK";
    static final String REFUSED = "[SQL0438] P9ERR AU DLS0313 Phone number invalid";
    static final String LINK_DOWN = "Communication link failure";

    @TempDir
    Path staging;

    CamelContext ctx;
    MockEndpoint digest;
    MockEndpoint failureMail;

    /** FMS answer per location id; anything not listed answers P9OK. */
    final Map<String, String> fmsAnswers = new ConcurrentHashMap<>();
    final AtomicInteger fmsCalls = new AtomicInteger();
    final AtomicInteger lmAttempts = new AtomicInteger();
    boolean lmDown;
    /** names of the mail mocks that throw MessagingException, as an SMTP outage would. */
    Set<String> smtpDown = Set.of();
    List<Map<String, Object>> locations;
    List<Map<String, Object>> requirements;

    // ------------------------------------------------------------------ setup

    @BeforeEach
    void setUp() {
        String now = LmFmsCore.nowCentral();
        String fiveMinAgo = LmFmsCore.fmtTs(LmFmsCore.parseTs(now).minusMinutes(5));
        locations = new ArrayList<>(List.of(
                location("1234", "North Yard", fiveMinAgo),
                location("5678", "South Yard", fiveMinAgo),
                row("LocationId", "-1", "ChangedTs", now)));      // sentinel carries the LM server clock
        requirements = new ArrayList<>(List.of(
                row("LocationId", "1234", "ReqType", "O", "ReqText", "Gate code:4471", "ReqSeq", "1"),
                row("LocationId", "-1", "ReqType", "", "ReqText", "", "ReqSeq", "")));
    }

    @AfterEach
    void tearDown() {
        if (ctx != null) {
            ctx.stop();
        }
    }

    /** Loads the real YAML, mocks the edges, starts Camel and fires one run. */
    Exchange runOnce(Map<String, String> extraProps) throws Exception {
        ctx = new DefaultCamelContext();
        ctx.getPropertiesComponent().setLocation("file:" + KAOTO.resolve("application.properties"));
        Properties p = new Properties();
        p.setProperty("lm.staging.dir", staging.toString());
        p.setProperty("lm.retryDelayMs", "10");
        p.setProperty("mail.to", "support@example.test");
        p.putAll(extraProps);
        ctx.getPropertiesComponent().setOverrideProperties(p);

        PluginHelper.getRoutesLoader(ctx).loadRoutes(
                ResourceHelper.resolveResource(ctx, "file:" + KAOTO.resolve("lm-to-fms-locations.camel.yaml")));

        AdviceWith.adviceWith(ctx, "lm-to-fms-locations", a -> {
            a.replaceFromWith("direct:start");
            a.weaveById("LookupLocations").replace().process(ex -> {
                lmAttempts.incrementAndGet();
                if (lmDown) {
                    throw new SQLException("Failed to obtain JDBC Connection", "08001");
                }
                ex.getIn().setBody(copy(locations));
            });
            a.weaveById("LookupRequirements").replace().process(ex -> ex.getIn().setBody(copy(requirements)));
            a.weaveById("ExecuteDb2Block").replace().process(this::fakeFms);
            a.weaveById("SendDigestEmail").replace().to("mock:digest");
        });
        AdviceWith.adviceWith(ctx, "SendUpstreamFailureEmail",
                a -> a.weaveById("MailFailure").replace().to("mock:failureMail"));

        ctx.start();
        digest = ctx.getEndpoint("mock:digest", MockEndpoint.class);
        failureMail = ctx.getEndpoint("mock:failureMail", MockEndpoint.class);
        for (MockEndpoint m : List.of(digest, failureMail)) {
            if (smtpDown.contains(m.getName())) {
                m.whenAnyExchangeReceived(ex -> {
                    throw new jakarta.mail.MessagingException("SMTP down");
                });
            }
        }
        return ctx.createProducerTemplate().send("direct:start", ex -> { });
    }

    Exchange runOnce() throws Exception {
        return runOnce(Map.of());
    }

    /** A real DB2 block always ends in SIGNAL, so the driver always throws; the text is the outcome. */
    void fakeFms(Exchange ex) throws SQLException {
        fmsCalls.incrementAndGet();
        String id = ex.getProperty("locationId", String.class);
        String sql = ex.getIn().getBody(String.class);
        assertTrue(sql != null && !sql.isBlank(), "FMS got an empty statement for " + id);
        String answer = fmsAnswers.getOrDefault(id, OK);
        throw new SQLException(answer, answer.contains("P9") ? "75005" : "08S01");
    }

    // ------------------------------------------------------------------ tests

    @Test
    void allSent_advancesWatermark_noMail() throws Exception {
        runOnce();

        Map<String, Object> log = runLog();
        assertEquals("Succeeded", log.get("status"), "run log: " + log);
        assertEquals(2, num(log.get("sent")));
        assertEquals(2, fmsCalls.get());
        assertEquals(log.get("endTs"), watermark().get("nextBeginTs"), "watermark moves to this run's endTs");
        assertEquals(0, digest.getReceivedCounter());
        assertEquals(0, failureMail.getReceivedCounter());
        assertEquals(List.of("sent", "sent"), itemOutcomes());
    }

    @Test
    void fmsRefusal_advancesWatermark_sendsDigest() throws Exception {
        fmsAnswers.put("5678", REFUSED);
        runOnce();

        Map<String, Object> log = runLog();
        assertEquals("Succeeded", log.get("status"), "run log: " + log);
        assertEquals(1, num(log.get("fmsErrors")));
        assertEquals(log.get("endTs"), watermark().get("nextBeginTs"), "a business refusal still advances");
        assertEquals(1, digest.getReceivedCounter());
        String body = digest.getReceivedExchanges().get(0).getIn().getBody(String.class);
        assertTrue(body.contains("5678"), "digest lists the refused location:\n" + body);
        assertTrue(digest.getReceivedExchanges().get(0).getIn().getHeader("subject", String.class)
                .startsWith("BUSINESS:"));
        assertEquals(0, failureMail.getReceivedCounter());
    }

    @Test
    void technicalFailure_holdsWatermark_sendsFailureMail() throws Exception {
        String before = writeWatermark(LmFmsCore.fmtTs(LmFmsCore.parseTs(LmFmsCore.nowCentral()).minusMinutes(10)));
        fmsAnswers.put("5678", LINK_DOWN);
        runOnce();

        Map<String, Object> log = runLog();
        assertEquals("Failed", log.get("status"), "run log: " + log);
        assertTrue(String.valueOf(log.get("error")).contains("nextBeginTs"), "error: " + log.get("error"));
        assertEquals(before, Files.readString(staging.resolve("watermark.json")), "watermark must not move");
        assertEquals(1, failureMail.getReceivedCounter());
        assertTrue(failureMail.getReceivedExchanges().get(0).getIn().getHeader("subject", String.class)
                .contains("FAILED (watermark held)"));
        assertEquals(2, fmsCalls.get(), "both locations were still attempted");
    }

    @Test
    void badWatermark_failsBeforeReadingLm() throws Exception {
        Files.writeString(staging.resolve("watermark.json"), "{\"nextBeginTs\":\"\"}");
        runOnce();

        Map<String, Object> log = runLog();
        assertEquals("Failed", log.get("status"));
        assertTrue(String.valueOf(log.get("error")).contains("WATERMARK_UNUSABLE"), "error: " + log.get("error"));
        assertEquals(0, lmAttempts.get(), "LM must not be read");
        assertEquals(0, fmsCalls.get());
        assertEquals(1, failureMail.getReceivedCounter());
    }

    @Test
    void lmDown_isRetriedFiveTimes_thenFails() throws Exception {
        lmDown = true;
        runOnce();

        Map<String, Object> log = runLog();
        assertEquals("Failed", log.get("status"));
        assertEquals(6, lmAttempts.get(), "1 attempt + 5 retries, as Fabric's retry 5");
        assertEquals(0, fmsCalls.get());
        assertFalse(Files.exists(staging.resolve("watermark.json")), "no watermark written");
        assertEquals(1, failureMail.getReceivedCounter());
    }

    @Test
    void override_replaysWindow_withoutReadingWatermark() throws Exception {
        Files.writeString(staging.resolve("watermark.json"), "not json at all");   // would fail if read
        String from = LmFmsCore.fmtTs(LmFmsCore.parseTs(LmFmsCore.nowCentral()).minusHours(2));
        runOnce(Map.of("lm.overrideBeginTs", from));

        Map<String, Object> log = runLog();
        assertEquals("Succeeded", log.get("status"), "run log: " + log);
        assertEquals(from, log.get("beginTs"));
        assertEquals(2, fmsCalls.get());
    }

    @Test
    void digestSmtpDown_failsRun_butWatermarkAlreadyAdvanced() throws Exception {
        fmsAnswers.put("5678", REFUSED);
        smtpDown = Set.of("digest");
        runOnce();

        Map<String, Object> log = runLog();
        assertEquals("Failed", log.get("status"), "an SMTP error on the digest fails the run");
        assertTrue(String.valueOf(log.get("error")).contains("SMTP down"), "error: " + log.get("error"));
        assertEquals(log.get("endTs"), watermark().get("nextBeginTs"),
                "WriteWatermark ran before the digest, so the watermark has already moved");
        assertEquals(1, failureMail.getReceivedCounter(), "failure mail says 'watermark held', which is wrong here");
    }

    @Test
    void failureMailSmtpDown_isLoggedOnly_runLogStillWritten() throws Exception {
        fmsAnswers.put("5678", LINK_DOWN);
        smtpDown = Set.of("failureMail");
        runOnce();

        Map<String, Object> log = runLog();
        assertEquals("Failed", log.get("status"));
        assertTrue(String.valueOf(log.get("error")).contains("nextBeginTs"),
                "the run log keeps the original failure, not the mail error: " + log.get("error"));
        assertFalse(Files.exists(staging.resolve("watermark.json")), "held");
    }

    // ------------------------------------------------------------------ helpers

    static Map<String, Object> location(String id, String name, String changedTs) {
        return row("LocationId", id, "LocationName", name, "StreetAddress1", "100 Main St",
                "StreetAddress2", null, "City", "Chicago", "State", "IL", "Country", "US",
                "PostalCode", "60601", "Phone", "312-555-0100", "ContactName", "Dock Lead", "Fax", "",
                "Status", "1", "LocationCategoryName", "TERMINAL", "LocationCategoryDesc", "Rail terminal",
                "ParentId", null, "FacilityDropsite", "DS01", "FacilityLocaleKey", "LOC" + id,
                "ChangedTs", changedTs);
    }

    static Map<String, Object> row(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    static List<Map<String, Object>> copy(List<Map<String, Object>> rows) {
        List<Map<String, Object>> out = new ArrayList<>();
        rows.forEach(r -> out.add(new LinkedHashMap<>(r)));
        return out;
    }

    String writeWatermark(String nextBeginTs) throws Exception {
        String doc = "{\"nextBeginTs\":\"" + nextBeginTs + "\",\"nextBeginId\":-1,\"runId\":\"seed\"}";
        Files.writeString(staging.resolve("watermark.json"), doc);
        return doc;
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> watermark() throws Exception {
        return (Map<String, Object>) Json.parse(Files.readString(staging.resolve("watermark.json")));
    }

    /** onCompletion writes the run log after the consumer is done; give it a moment. */
    @SuppressWarnings("unchecked")
    Map<String, Object> runLog() throws Exception {
        Path dir = staging.resolve("runlog");
        for (int i = 0; i < 100; i++) {
            if (Files.isDirectory(dir)) {
                try (Stream<Path> s = Files.list(dir)) {
                    List<Path> files = s.toList();
                    if (!files.isEmpty()) {
                        assertEquals(1, files.size(), "one run log per run");
                        return (Map<String, Object>) Json.parse(Files.readString(files.get(0)));
                    }
                }
            }
            Thread.sleep(50);
        }
        throw new AssertionError("no run log written");
    }

    @SuppressWarnings("unchecked")
    List<String> itemOutcomes() throws Exception {
        try (Stream<Path> s = Files.walk(staging.resolve("items"))) {
            Path f = s.filter(p -> p.getFileName().toString().equals("items.json")).findFirst().orElseThrow();
            List<String> out = new ArrayList<>();
            for (Object o : (List<Object>) Json.parse(Files.readString(f))) {
                out.add(String.valueOf(((Map<String, Object>) o).get("outcome")));
            }
            return out;
        }
    }

    static int num(Object o) {
        return o == null ? -1 : ((Number) o).intValue();
    }

    // ------------------------------------------------------------------ runner (jbang)

    public static void main(String[] args) {
        LauncherDiscoveryRequest req = LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(LmToFmsRouteTest.class)).build();
        Launcher launcher = LauncherFactory.create();
        SummaryGeneratingListener summary = new SummaryGeneratingListener();
        TestExecutionListener printer = new TestExecutionListener() {
            @Override
            public void executionFinished(TestIdentifier id, TestExecutionResult r) {
                if (id.isTest()) {
                    System.out.println((r.getStatus() == TestExecutionResult.Status.SUCCESSFUL ? "PASS  " : "FAIL  ")
                            + id.getDisplayName());
                }
            }
        };
        launcher.execute(req, summary, printer);
        PrintWriter out = new PrintWriter(System.out);
        summary.getSummary().printFailuresTo(out, 15);
        summary.getSummary().printTo(out);
        out.flush();
        System.exit(summary.getSummary().getTotalFailureCount() == 0 ? 0 : 1);
    }
}
