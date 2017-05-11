package io.logz.sawmill.executor.utils;

import io.logz.sawmill.Doc;

import java.util.LinkedHashMap;

public class DocUtils {
    public static Doc createDoc(Object... objects) {
        LinkedHashMap map = new LinkedHashMap<>();
        if (objects != null) {
            for (int i = 0; i < objects.length; i++) {
                map.put(objects[i], objects[++i]);
            }
        }
        return new Doc(map);
    }
}
