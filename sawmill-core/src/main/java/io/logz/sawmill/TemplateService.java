package io.logz.sawmill;

import com.github.mustachejava.MustacheFactory;
import io.logz.sawmill.exceptions.SawmillException;

import java.io.StringReader;


public class TemplateService {
    //TODO: remove backward compatibility mustache and support only json string implementation
    public static final String JSON_STRING_SUFFIX = "_sawmill_json";
    private final MustacheFactory mustacheFactory;
    private final DateTemplateHandler dateTemplateHandler;
    private final UnescapedWithJsonStringMustacheFactory jsonStringMustacheFactory;

    public TemplateService() {
        this.mustacheFactory = new UnescapedMustacheFactory();
        this.jsonStringMustacheFactory = new UnescapedWithJsonStringMustacheFactory();
        this.dateTemplateHandler = new DateTemplateHandler();
    }

    public Template createTemplate(String template) {
        if (template == null) {
            throw new SawmillException("template cannot be with null value");
        }

        Object value = template;

        boolean containsMustache = template.contains("{{") && template.contains("}}");
        if (containsMustache) {
            value = template.contains(JSON_STRING_SUFFIX) ?
                        jsonStringMustacheFactory.compile(new StringReader(template.replaceAll(JSON_STRING_SUFFIX, "")), "") :
                        mustacheFactory.compile(new StringReader(template), "");
        }

        return new Template(value, dateTemplateHandler);
    }
}
