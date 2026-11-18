import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Prints the Java port's results for fixtures.json; parity_check.py diffs them against the Fabric UDF. */
public class ParityCheck {
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Map<String, Object> f = (Map<String, Object>) Json.parse(Files.readString(Path.of(args[0])));
        String endTs = (String) f.get("endTs");
        Map<String, Object> out = new LinkedHashMap<>();
        Map<String, Object> build = LmFmsCore.buildCalls(f.get("locations"), f.get("requirements"),
                (String) f.get("fmsLibrary"), (String) f.get("defaultPhone"),
                ((Number) f.get("maxLocations")).intValue(), endTs);
        for (String k : List.of("calls", "skipped", "locationCount", "requirementCount", "maxTs", "serverTs",
                "capped", "bytes", "error", "callCount", "nextBeginTs", "nextBeginId")) {
            out.put("build." + k, build.get(k));
        }
        List<Object> windows = new ArrayList<>();
        for (Object w : (List<Object>) f.get("watermarks")) {
            Map<String, Object> win = LmFmsCore.readWindow((String) w, endTs, "");
            win.put("error", ((String) win.get("error")).isEmpty() ? "" : "ERR");
            windows.add(win);
        }
        out.put("windows", windows);
        List<Object> classes = new ArrayList<>();
        for (Object m : (List<Object>) f.get("messages")) {
            classes.add(List.of(LmFmsCore.classifyOutcome((String) m)));
        }
        out.put("classify", classes);
        System.out.println(Json.write(out));
    }
}
