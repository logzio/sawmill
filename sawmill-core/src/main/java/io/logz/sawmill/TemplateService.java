package io.logz.sawmill;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import io.logz.sawmill.exceptions.SawmillException;

import java.io.StringReader;

public class TemplateService implements Service {
    private final DefaultMustacheFactory mustacheFactory;

    public TemplateService() {
        this.mustacheFactory = new DefaultMustacheFactory();
    }

    public Template createTemplate(String template) {
        if (template == null) {
            throw new SawmillException("template cannot be with null value");
        }
        StringReader stringReader = new StringReader(template);

        Mustache mustache = mustacheFactory.compile(stringReader, "");

        return new Template(mustache);
    }
}
