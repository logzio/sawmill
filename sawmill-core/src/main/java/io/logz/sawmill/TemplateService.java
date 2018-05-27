package io.logz.sawmill;

import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import io.logz.sawmill.exceptions.SawmillException;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.LinkedHashMap;


public class TemplateService {
    //TODO: remove backward compatibility mustache and support only json string implementation
    public static final String JSON_STRING_SUFFIX = "_sawmill_json";
    private final MustacheFactory mustacheFactory;
    private final UnescapedWithJsonStringMustacheFactory jsonStringMustacheFactory;

    public TemplateService() {
        this.mustacheFactory = new UnescapedMustacheFactory();
        this.jsonStringMustacheFactory = new UnescapedWithJsonStringMustacheFactory();
    }

    public Template createTemplate(String template) {
        if (template == null) {
            throw new SawmillException("template cannot be with null value");
        }

        boolean containsMustache = template.contains("{{") && template.contains("}}");
        if (!containsMustache) {
            return new StringTemplate(template);
        }

        Mustache mustache = template.contains(JSON_STRING_SUFFIX) ?
                    jsonStringMustacheFactory.compile(new StringReader(template.replaceAll(JSON_STRING_SUFFIX, "")), "") :
                    mustacheFactory.compile(new StringReader(template), "");

        return new MustacheTemplate(mustache);
    }

    public static class StringTemplate implements Template {
        private final String value;
        private StringTemplate(String value) {
            this.value = value;
        }

        @Override
        public String render(Doc doc) {
            return value;
        }
    }

    public static class MustacheTemplate implements Template {
        private final Mustache mustache;
        private final static DateTemplateHandler dateTemplateHandler = new DateTemplateHandler();

        private MustacheTemplate(Mustache value) {
            this.mustache = value;
        }
        @Override
        public String render(Doc doc) {
            Object docContext;
            if (doc == null) {
                docContext = new LinkedHashMap<>();
            } else {
                docContext = doc.getSource();
            }

            StringWriter writer = new StringWriter();
            mustache.execute(writer, Arrays.asList(docContext, dateTemplateHandler));

            writer.flush();

            return writer.toString();
        }

    }
}
