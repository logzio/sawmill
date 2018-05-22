package io.logz.sawmill;

import com.github.mustachejava.Mustache;

import java.io.StringWriter;
import java.util.Arrays;
import java.util.LinkedHashMap;

public class Template {
    private final Object value;
    private final DateTemplateHandler dateTemplateHandler;

    public Template(Object value, DateTemplateHandler dateTemplateHandler) {
        this.value = value;
        this.dateTemplateHandler = dateTemplateHandler;
    }

    public String render(Doc doc) {
        if (value instanceof String) {
            return (String) value;
        }

        Mustache mustache = (Mustache) value;
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
