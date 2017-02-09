package io.logz.sawmill;

import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.logz.sawmill.TemplateService.compileTemplate;

public interface TemplatedValue {
    Mustache.Compiler mustache = Mustache.compiler();

    Object execute(Object context);

    final class MapValue implements TemplatedValue {
        private final Map<Template, TemplatedValue> map;

        public MapValue(Map<Template, TemplatedValue> map) {
            this.map = map;
        }

        @Override
        public Object execute(Object context) {
            Map<String, Object> compiled = new HashMap<>();
            for (Map.Entry<Template, TemplatedValue> entry : map.entrySet()) {
                compiled.put(entry.getKey().execute(context), entry.getValue().execute(context));
            }
            return compiled;
        }
    }

    final class ListValue implements TemplatedValue {
        private final List<TemplatedValue> list;

        public ListValue(List<TemplatedValue> list) {
            this.list = list;
        }

        @Override
        public Object execute(Object context) {
            List<Object> compiled = new ArrayList<>();
            list.stream().map(item -> item.execute(context)).forEach(compiled::add);
            return compiled;
        }
    }

    final class ObjectValue implements TemplatedValue {
        private final Object object;

        public ObjectValue(Object object) {
            this.object = object;
        }

        @Override
        public Object execute(Object context) {
            return object;
        }
    }

    final class StringValue implements TemplatedValue {
        private final Template template;

        public StringValue(String str) {
            this.template = compileTemplate(str);
        }

        @Override
        public Object execute(Object context) {
            return template.execute(context);
        }
    }
}
