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
                    return flatten((Map) object);
                }

                if (object != null && object instanceof List) {
                    return flattenList((List) object);
                }

                return super.coerce(object);
            }

            private Map<String, Object> flattenList(List object) {
                Map<String, Object> map = new LinkedHashMap<>();
                List<Object> list = object;
                map.putAll(IntStream.range(0, list.size())
                        .boxed()
                        .collect(Collectors.toMap(i -> i.toString(), list::get)));
                map.put("first", map.get("0"));
                map.put("last", map.get(String.valueOf(list.size() - 1)));

                return map;
            }

            private Map<String, Object> flatten(Map<String, Object> context) {
                Map<String, Object> map = new LinkedHashMap<>();
                context.entrySet().stream().forEach(entry -> {
                    String key = escape(entry.getKey());
                    Object value = entry.getValue();
                    map.put(key, value);
                    if (value instanceof Map) {
                        map.put(key + "_logzio_json", JsonUtils.toJsonString(value));
                    }
                });

                return map;
            }

            private String escape(String s) {
                return s.replaceAll("\\.", "_");
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
