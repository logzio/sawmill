package io.logz.sawmill;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.MustacheException;
import com.github.mustachejava.reflect.ReflectionObjectHandler;

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class UnescapedMustacheFactory extends DefaultMustacheFactory {
    public UnescapedMustacheFactory() {
        super();

        ReflectionObjectHandler oh = new ReflectionObjectHandler() {
            @Override
            public Object coerce(final Object object) {
                if (object != null && object instanceof List) {
                    List<Object> list = (List) object;
                    Map<String, Object> map = IntStream.range(0, list.size())
                            .boxed()
                            .collect(Collectors.toMap(i -> i.toString(), list::get));
                    map.put("last", map.get(String.valueOf(list.size() - 1)));
                    return map;
                }
                return super.coerce(object);
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
