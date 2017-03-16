package io.logz.sawmill;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;

import java.io.StringReader;
import java.util.Optional;

public class TemplateFactory {
    public static final String MUSTACHE_START = "{{";
    public static final String MUSTACHE_END = "}}";
    private static DefaultMustacheFactory mustacheFactory = new DefaultMustacheFactory();

    public static Optional<Template> compileTemplate(Object template) {
        if (template instanceof String) {
            String stringTemplate = (String) template;

            boolean hasMustache = stringTemplate.contains(MUSTACHE_START) && stringTemplate.contains(MUSTACHE_END);
            if (!hasMustache) {
                return Optional.empty();
            }

            StringReader stringReader = new StringReader(stringTemplate);

            Mustache mustache = mustacheFactory.compile(stringReader, "");

            return Optional.of(new Template(mustache));
        }
        return Optional.empty();
    }
}
