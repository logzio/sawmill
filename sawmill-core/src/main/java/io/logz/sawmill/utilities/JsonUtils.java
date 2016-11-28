package io.logz.sawmill.utilities;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.Optional;

public class JsonUtils {

    private static final ObjectMapper mapper;

    static {
        mapper = new ObjectMapper();

        mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        mapper.enable(DeserializationFeature.READ_ENUMS_USING_TO_STRING);
        mapper.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL);
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

    /**
     * json OGNL (Object Graph Navigation Language) getter.
     * <p>for example:
     * <pre>
     * JsonUtils.getByPath(json, "x.y.z")
     * </pre>
     *
     * @return Optional of the value in paths
     * @throws Exception on any error
     **/
    public static <T> Optional<T> getByPath(Map json, String... paths) {
        Object cursor = json;
        for (String path : paths) {
            for (String node : path.split("\\.")) {
                cursor = ((Map) cursor).get(node);
                if (cursor == null) return Optional.empty();
            }
        }
        return Optional.of((T) cursor);
    }
}
