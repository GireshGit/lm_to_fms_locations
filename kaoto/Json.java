import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal JSON reader/writer so the POC needs no Jackson dependency. The writer
 * mimics Python's json.dumps defaults (", " / ": " separators, ensure_ascii), so
 * sizes and item-log rows match what the Fabric UDF writes.
 */
public final class Json {

    private Json() {
    }

    public static String write(Object v) {
        StringBuilder b = new StringBuilder();
        write(v, b);
        return b.toString();
    }

    private static void write(Object v, StringBuilder b) {
        if (v == null) {
            b.append("null");
        } else if (v instanceof String) {
            quote((String) v, b);
        } else if (v instanceof Boolean || v instanceof Integer || v instanceof Long) {
            b.append(v);
        } else if (v instanceof Number) {
            b.append(((Number) v).doubleValue());
        } else if (v instanceof Map) {
            b.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : ((Map<?, ?>) v).entrySet()) {
                if (!first) {
                    b.append(", ");
                }
                first = false;
                quote(String.valueOf(e.getKey()), b);
                b.append(": ");
                write(e.getValue(), b);
            }
            b.append('}');
        } else if (v instanceof Iterable) {
            b.append('[');
            boolean first = true;
            for (Object o : (Iterable<?>) v) {
                if (!first) {
                    b.append(", ");
                }
                first = false;
                write(o, b);
            }
            b.append(']');
        } else {
            quote(v.toString(), b);
        }
    }

    private static void quote(String s, StringBuilder b) {
        b.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': b.append("\\\""); break;
                case '\\': b.append("\\\\"); break;
                case '\n': b.append("\\n"); break;
                case '\r': b.append("\\r"); break;
                case '\t': b.append("\\t"); break;
                case '\b': b.append("\\b"); break;
                case '\f': b.append("\\f"); break;
                default:
                    if (c < 0x20 || c > 0x7f) {
                        b.append(String.format("\\u%04x", (int) c));
                    } else {
                        b.append(c);
                    }
            }
        }
        b.append('"');
    }

    public static Object parse(String text) {
        Parser p = new Parser(text);
        p.ws();
        Object v = p.value();
        p.ws();
        if (p.i != text.length()) {
            throw p.err("trailing data");
        }
        return v;
    }

    private static final class Parser {
        final String s;
        int i;

        Parser(String s) {
            this.s = s;
        }

        IllegalArgumentException err(String m) {
            return new IllegalArgumentException(m + " at char " + i);
        }

        void ws() {
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
                i++;
            }
        }

        boolean lit(String w) {
            if (s.startsWith(w, i)) {
                i += w.length();
                return true;
            }
            return false;
        }

        Object value() {
            if (i >= s.length()) {
                throw err("unexpected end");
            }
            char c = s.charAt(i);
            if (c == '{') {
                i++;
                Map<String, Object> m = new LinkedHashMap<>();
                ws();
                if (lit("}")) {
                    return m;
                }
                do {
                    ws();
                    if (i >= s.length() || s.charAt(i) != '"') {
                        throw err("expected key");
                    }
                    String k = string();
                    ws();
                    if (!lit(":")) {
                        throw err("expected ':'");
                    }
                    ws();
                    m.put(k, value());
                    ws();
                } while (lit(","));
                if (!lit("}")) {
                    throw err("expected '}'");
                }
                return m;
            }
            if (c == '[') {
                i++;
                List<Object> l = new ArrayList<>();
                ws();
                if (lit("]")) {
                    return l;
                }
                do {
                    ws();
                    l.add(value());
                    ws();
                } while (lit(","));
                if (!lit("]")) {
                    throw err("expected ']'");
                }
                return l;
            }
            if (c == '"') {
                return string();
            }
            if (lit("true")) {
                return Boolean.TRUE;
            }
            if (lit("false")) {
                return Boolean.FALSE;
            }
            if (lit("null")) {
                return null;
            }
            int start = i;
            while (i < s.length() && "+-0123456789.eE".indexOf(s.charAt(i)) >= 0) {
                i++;
            }
            if (start == i) {
                throw err("unexpected character");
            }
            String n = s.substring(start, i);
            try {
                return n.matches("-?\\d+") ? (Object) Long.parseLong(n) : (Object) Double.parseDouble(n);
            } catch (NumberFormatException e) {
                throw err("bad number");
            }
        }

        String string() {
            i++;
            StringBuilder b = new StringBuilder();
            while (i < s.length()) {
                char c = s.charAt(i++);
                if (c == '"') {
                    return b.toString();
                }
                if (c == '\\') {
                    if (i >= s.length()) {
                        break;
                    }
                    char e = s.charAt(i++);
                    switch (e) {
                        case 'n': b.append('\n'); break;
                        case 'r': b.append('\r'); break;
                        case 't': b.append('\t'); break;
                        case 'b': b.append('\b'); break;
                        case 'f': b.append('\f'); break;
                        case 'u':
                            if (i + 4 > s.length()) {
                                throw err("bad escape");
                            }
                            b.append((char) Integer.parseInt(s.substring(i, i + 4), 16));
                            i += 4;
                            break;
                        default: b.append(e);
                    }
                } else {
                    b.append(c);
                }
            }
            throw err("unterminated string");
        }
    }
}
