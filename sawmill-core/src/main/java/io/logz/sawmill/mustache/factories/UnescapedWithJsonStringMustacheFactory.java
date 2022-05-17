package io.logz.sawmill.mustache.factories;

import com.github.mustachejava.MustacheException;
import com.github.mustachejava.reflect.ReflectionObjectHandler;
import io.logz.sawmill.utilities.JsonUtils;
import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class UnescapedWithJsonStringMustacheFactory extends SafeMustacheCustomVisitorFactory {
    public UnescapedWithJsonStringMustacheFactory() {
        this.setObjectHandler(new ListTransformObjectHandler());
    }

    @Override
    public void encode(String value, Writer writer) {
        try {
            writer.write(value);
        } catch (IOException e) {
            throw new MustacheException("Failed to encode value: " + value);
        }
    }

    public class ListTransformObjectHandler extends ReflectionObjectHandler {
        @Override
        public String stringify(Object object) {
            if (object instanceof Map) {
                return JsonUtils.toJsonString(object);
            }

            return super.stringify(object);
        }

        @Override
        public Object coerce(final Object object) {
            if (object != null && object instanceof List) {
                return transformListToMap((List) object);
            }
            return super.coerce(object);
        }

        private Map<String, Object> transformListToMap(List<Object> list) {
            Map<String, Object> map = IntStream.range(0, list.size())
                    .boxed()
                    .collect(Collectors.toMap(i -> i.toString(), list::get));
            map.put("first", map.get("0"));
            map.put("last", map.get(String.valueOf(list.size() - 1)));

            return map;
        }
    }
}
