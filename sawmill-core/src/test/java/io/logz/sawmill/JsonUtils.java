package io.logz.sawmill;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.logz.sawmill.utilities.JsonUtils.toJsonString;

public class JsonUtils {
    public static String createJson(Map<String, Object> map) {
        return toJsonString(map);
    }

    public static List<Object> createList(Object... maps) {
        return Arrays.asList(maps);
    }

    public static Map<String, Object> createMap(Object... objects) {
        Map<String, Object> map = new LinkedHashMap<>();
        if (objects != null) {
            for (int i = 0; i < objects.length; i++) {
                map.put((String) objects[i], objects[++i]);
            }
        }
        return map;
    }
}
