package io.logz.sawmill;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheException;
import com.github.mustachejava.reflect.ReflectionObjectHandler;
import io.logz.sawmill.exceptions.SawmillException;

import java.io.IOException;
import java.io.StringReader;
import java.io.Writer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TemplateService {
    private final DefaultMustacheFactory mustacheFactory;
    private final DateTemplateHandler dateTemplateHandler;

    public TemplateService() {
        this.mustacheFactory = new DefaultMustacheFactory() {
            @Override
            public void encode(String value, Writer writer) {
                try {
                    writer.write(value);
                } catch (IOException e) {
                    throw new MustacheException("Failed to encode value: " + value);
                }
            }
        };
        ReflectionObjectHandler oh = new ReflectionObjectHandler() {
            @Override
            public Object coerce(final Object object) {
                if (object != null && object instanceof List) {
                    List<Object> list = (List) object;
                    return IntStream.range(0, list.size())
                            .boxed()
                            .collect(Collectors.toMap(i -> i.toString(), list::get));
                }
                return super.coerce(object);
            }
        };

        this.mustacheFactory.setObjectHandler(oh);
        this.dateTemplateHandler = new DateTemplateHandler();
    }

    public Template createTemplate(String template) {
        if (template == null) {
            throw new SawmillException("template cannot be with null value");
        }
        StringReader stringReader = new StringReader(template);

        Mustache mustache = mustacheFactory.compile(stringReader, "");

        return new Template(mustache, dateTemplateHandler);
    }
}
