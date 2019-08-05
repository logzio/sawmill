package io.logz.sawmill.utils;

import io.logz.sawmill.Doc;

import java.util.LinkedHashMap;

public class DocUtils {
    public static Doc createDoc(Object... objects) {
        if (objects.length % 2 != 0) {
            throw new RuntimeException("Can't construct map out of uneven number of elements");
        }

        LinkedHashMap map = new LinkedHashMap<>();
        if (objects != null) {
            for (int i = 0; i < objects.length; i++) {
                map.put(objects[i], objects[++i]);
            }
        }
        return new Doc(map);
    }
}
