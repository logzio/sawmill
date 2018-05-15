package io.logz.sawmill;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.MustacheException;
import com.github.mustachejava.reflect.ReflectionObjectHandler;
import io.logz.sawmill.utilities.JsonUtils;

import java.io.IOException;
import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.logz.sawmill.FieldType.STRING;

public class UnescapedMustacheFactory extends DefaultMustacheFactory {
    public UnescapedMustacheFactory() {
        super();

        ReflectionObjectHandler oh = new ReflectionObjectHandler() {
            @Override
            public Object coerce(final Object object) {
                if (object != null && object instanceof Map) {
                    Map<String, Object> map = new LinkedHashMap<>();
                    flatten(map, "", (Map) object);
                    return map;
                }

                return super.coerce(object);
            }

            private void flattenList(Map<String, Object> map, String pathContext, List object) {
                List<Object> list = object;
                map.putAll(IntStream.range(0, list.size())
                        .boxed()
                        .collect(Collectors.toMap(i -> pathContext + i.toString(), list::get)));
                map.put(pathContext + "first", map.get(pathContext + "0"));
                map.put(pathContext + "last", map.get(pathContext + String.valueOf(list.size() - 1)));
            }

            private void flatten(Map<String, Object> map, String pathContext, Map<String, Object> context) {
                context.entrySet().stream().forEach(entry -> {
                    String key = pathContext + escape(entry.getKey());
                    Object value = entry.getValue();
                    map.put(key, STRING.convertFrom(value));
                    if (value instanceof List) flattenList(map, key + ".", (List) value);
                    else if (value instanceof Map) {
                        map.put(key + "_logzio_json", JsonUtils.toJsonString(value));
                        flatten(map, key + ".", (Map)value);
                    }
                });
            }

            private String escape(String s) {
                return s.replaceAll("\\.", "\\\\.");
            }
        };

        this.setObjectHandler(oh);
    }

    @Override
    public void encode(String value, Writer writer) {
        try {
            writer.write(value);
        } catch (IOException e) {
            throw new MustacheException("Failed to encode value: " + value);
        }
    }
}
