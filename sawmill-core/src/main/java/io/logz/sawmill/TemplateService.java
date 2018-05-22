package io.logz.sawmill;

import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import io.logz.sawmill.exceptions.SawmillException;

import java.io.StringReader;


public class TemplateService {
    private final MustacheFactory mustacheFactory;
    private final DateTemplateHandler dateTemplateHandler;

    public TemplateService() {

        this.mustacheFactory = new UnescapedMustacheFactory();
        this.dateTemplateHandler = new DateTemplateHandler();
    }

    public Template createTemplate(String template) {
        if (template == null) {
            throw new SawmillException("template cannot be with null value");
        }

        Object value = template;

        if (template.contains("{{") && template.contains("}}")) {
            StringReader stringReader = new StringReader(template);
            value = mustacheFactory.compile(stringReader, "");
        }

        return new Template(value, dateTemplateHandler);
    }
}
