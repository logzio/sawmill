package io.logz.sawmill;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheException;
import com.github.mustachejava.MustacheFactory;
import io.logz.sawmill.exceptions.SawmillException;

import java.io.IOException;
import java.io.StringReader;
import java.io.Writer;

public class TemplateService {
    private final MustacheFactory mustacheFactory;
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
