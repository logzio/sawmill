package io.logz.sawmill.mustache.factories;

import com.github.mustachejava.DefaultMustacheVisitor;
import com.github.mustachejava.MustacheException;
import com.github.mustachejava.MustacheVisitor;
import com.github.mustachejava.SafeMustacheFactory;
import com.github.mustachejava.TemplateContext;
import java.util.Collections;

public class SafeMustacheCustomVisitorFactory extends SafeMustacheFactory {

    public SafeMustacheCustomVisitorFactory() {
        super(Collections.emptySet(), "."); // disallow any resource reference
    }

    @Override
    public MustacheVisitor createMustacheVisitor() {
        return new DefaultMustacheVisitor(this) {
            public void pragma(TemplateContext tc, String pragma, String args) {
                throw new MustacheException("Disallowed: pragmas in templates");
            }
        };
    }
}
