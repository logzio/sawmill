package io.logz.sawmill;

import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TemplateService {
    private static Mustache.Compiler mustache = Mustache.compiler();

    public static Template compileTemplate(String value) {
        return value != null ? mustache.compile(value) : null;
    }

    public static TemplatedValue compileValue(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Map) {
            Map<String, Object> mapValue = (Map) value;
            Map<Template, TemplatedValue> valueTypeMap = new HashMap<>(mapValue.size());
            for (Map.Entry<String, Object> entry : mapValue.entrySet()) {
                valueTypeMap.put(compileTemplate(String.valueOf(entry.getKey())), compileValue(entry.getValue()));
            }
            return new TemplatedValue.MapValue(valueTypeMap);
        } else if (value instanceof List) {
            List<Object> listValue = (List) value;
            List<TemplatedValue> valueSourceList = new ArrayList<>(listValue.size());
            for (Object item : listValue) {
                valueSourceList.add(compileValue(item));
            }
            return new TemplatedValue.ListValue(valueSourceList);
        } else if (value instanceof String) {
            return new TemplatedValue.StringValue((String) value);
        } else {
            return new TemplatedValue.ObjectValue(value);
        }
    }
}
