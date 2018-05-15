package io.logz.sawmill;

import com.github.mustachejava.Mustache;

import java.io.StringWriter;
import java.util.Arrays;
import java.util.LinkedHashMap;

public class Template {
    private final Mustache mustache;
    private final DateTemplateHandler dateTemplateHandler;

    public Template(Mustache mustache, DateTemplateHandler dateTemplateHandler) {
        this.mustache = mustache;
        this.dateTemplateHandler = dateTemplateHandler;
    }

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
