package io.logz.sawmill;

import com.github.mustachejava.Mustache;

import java.io.StringWriter;
import java.util.Collections;

public class Template {
    private final Mustache mustache;

    public Template(Mustache mustache) {
        this.mustache = mustache;
    }

    public String render(Object context) {
        if (context == null) {
            context = Collections.EMPTY_MAP;
        }

        StringWriter writer = new StringWriter();
        mustache.execute(writer, context);

        writer.flush();

        return writer.toString();
    }
}
