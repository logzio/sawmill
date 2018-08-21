package io.logz.sawmill.utilities;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import io.logz.sawmill.parser.PipelineDefinitionJsonParser;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JsonUtils {

    private static final ObjectMapper mapper;

    static {
        mapper = new ObjectMapper();

        mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        mapper.enable(DeserializationFeature.READ_ENUMS_USING_TO_STRING);
        mapper.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL);
        mapper.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true);
        mapper.registerModule(new AfterburnerModule());

    }

    public static <T> T fromJsonString(Class<T> type, String json) {
        if (json == null || json.isEmpty()) {
            throw new RuntimeException("json is either null or empty (json = "+json+")");
        }

        try {
            return mapper.readValue(json, type);
        }
        catch (Exception e) {
            throw new RuntimeException("failed to deserialize object type="+type+" from json="+json, e);
        }
    }

    public static <T> T fromJsonString(TypeReference<T> typeReference, String json) {
        if (json == null || json.isEmpty()) {
            throw new RuntimeException("json is either null or empty (json = "+json+")");
        }

        try {
            return mapper.readValue(json, typeReference);
        }
        catch (Exception e) {
            throw new RuntimeException("failed to deserialize object type="+typeReference.getType()+" from json="+json, e);
        }
    }

    public static <T> T fromJsonMap(Class<T> type, Map json) {
        if (json == null) {
            throw new RuntimeException("json map is null");
        }

        try {
            return mapper.convertValue(json, type);
        }
        catch (Exception e) {
            throw new RuntimeException("failed to deserialize object type="+type+" from json="+json, e);
        }
    }

    public static String toJsonString(Object jsonObject) {
        if (jsonObject == null) { return null; }

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            mapper.writeValue(stream, jsonObject);
            return stream.toString();
        } catch (Exception e) {
            throw new RuntimeException("failed to serialize object ="+
                    org.apache.commons.lang3.StringUtils.abbreviate(jsonObject.toString(), 100)
                    +" to json. Error = "+e.getMessage(), e);
        }
    }

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

    public static String getTheOnlyKeyFrom(Map<String, Object> map) {
        Set<String> keys = map.keySet();
        if (keys.size() != 1) {
            throw new RuntimeException("JSON should contain only one key: " + toJsonString(map));
        }
        return keys.iterator().next();
    }

    public static Boolean getBoolean(PipelineDefinitionJsonParser pipelineDefinitionJsonParser, Map<String, Object> map, String key, boolean requiredField) {
        return getValueAs(map, key, Boolean.class, requiredField);
    }

    public static Map<String, Object> getMap(Map<String, Object> map, String key, boolean requiredField) {
        return getValueAs(map, key, Map.class, requiredField);
    }

    public static String getString(Map<String, Object> map, String key, boolean requiredField) {
        return getValueAs(map, key, String.class, requiredField);
    }

    public static List<Map<String, Object>> getList(Map<String, Object> map, String key, boolean requiredField) {
        return getValueAs(map, key, List.class, requiredField);
    }

    private static <T> T getValueAs(Map<String, Object> map, String key, Class<T> clazz, boolean requiredField) {
        Object value = map.get(key);
        if (value == null) {
            if (!requiredField) return null;
            throw new RuntimeException("\"" + key + "\"" + " is a required field which does not exists in " + map);
        }
        if (!clazz.isInstance(value)) {
            throw new RuntimeException("Value of field \"" + key + "\"" + " is: " + value  + " with type: " + value.getClass().getSimpleName() +
                    " , while it should be of type " + clazz.getSimpleName());
        }
        return clazz.cast(value);
    }

    public static boolean isValueMap(Map<String, Object> map, String key) {
        return map.get(key) instanceof Map;
    }

    public static boolean isValueList(Map<String, Object> map, String key) {
        return map.get(key) instanceof List;
    }
}
