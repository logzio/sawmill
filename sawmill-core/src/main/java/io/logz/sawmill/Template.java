package io.logz.sawmill;

import com.github.mustachejava.Mustache;

import java.io.StringWriter;
import java.util.Collections;

public class Template {
    private final Mustache mustache;

    public Template(Mustache mustache) {
        this.mustache = mustache;
    }

    public String render(Doc doc) {
        Object context;
        if (doc == null) {
            context = Collections.EMPTY_MAP;
        } else {
            context = doc.getSource();
        }

        StringWriter writer = new StringWriter();
        mustache.execute(writer, context);

        writer.flush();

        return writer.toString();
    }
}
