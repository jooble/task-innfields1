package taks1;

import java.util.HashMap;
import java.util.Map;

public class GlobalCache {

    private final static Map<String, Object> MAP = new HashMap<String, Object>();

    private GlobalCache() {
    }

    public static String intern(String t) {
        String exist = (String) MAP.putIfAbsent(t, t);
        return (exist == null) ? t : exist;
    }
}
