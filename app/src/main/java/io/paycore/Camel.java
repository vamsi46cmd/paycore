package io.paycore;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Normalizes database column names (snake_case) to the API's camelCase so every endpoint uses one naming style. */
final class Camel {
    private Camel() {}

    static String camel(String s) {
        StringBuilder sb = new StringBuilder();
        boolean up = false;
        for (char c : s.toCharArray()) {
            if (c == '_') {
                up = true;
            } else {
                sb.append(up ? Character.toUpperCase(c) : c);
                up = false;
            }
        }
        return sb.toString();
    }

    static Map<String, Object> map(Map<String, Object> m) {
        Map<String, Object> out = new LinkedHashMap<>();
        m.forEach((k, v) -> out.put(camel(k), v));
        return out;
    }

    static List<Map<String, Object>> list(List<Map<String, Object>> rows) {
        return rows.stream().map(Camel::map).toList();
    }
}
