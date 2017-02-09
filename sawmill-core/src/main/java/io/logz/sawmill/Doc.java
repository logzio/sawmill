package io.logz.sawmill;

import com.samskivert.mustache.Template;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.MapUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

public class Doc {

    private final Map<String, Object> source;
    private final Map<String, Object> metadata;

    public Doc(Map<String, Object> source, Map<String, Object> metadata) {
        checkState(MapUtils.isNotEmpty(source), "source cannot be empty");
        this.source = source;
        this.metadata = metadata;
    }

    public Doc(Map<String, Object> source) {
        this(source, new HashMap<>());
    }

    public Map<String, Object> getSource() { return source; }

    public Map<String, Object> getMetadata() { return metadata; }

    public boolean hasField(Template path) {
        return hasField(renderTemplate(path));
    }

    public boolean hasField(String path) {
        Optional<Object> field = JsonUtils.getByPath(source, path);
        return field.isPresent();
    }

    public boolean hasField(Template path, Class clazz) {
        return hasField(renderTemplate(path), clazz);
    }

    public boolean hasField(String path, Class clazz) {
        Optional<Object> field = JsonUtils.getByPath(source, path);
        return field.isPresent() && clazz.isInstance(field.get());
    }

    public <T> T getField(Template path) {
        return getField(renderTemplate(path));
    }

    public <T> T getField(String path) {
        Optional<Object> field = JsonUtils.getByPath(source, path);
        checkState(field.isPresent(), String.format("Couldn't resolve field in path [%s]", path));
        return (T) field.get();
    }

    public void addField(Template path, Object value) {
        addField(renderTemplate(path), value);
    }

    public void addField(Template path, TemplatedValue value) {
        addField(renderTemplate(path), renderTemplate(value));
    }

    public void addField(String path, Object value) {
        Map<String, Object> context = source;
        String[] pathElements = path.split("\\.");

        String leafKey = pathElements[pathElements.length - 1];

        for (int i=0; i< pathElements.length - 1; i++) {
            String pathElement = pathElements[i];
            if (context.containsKey(pathElement)) {
                context = (Map) context.get(pathElement);
            } else {
                Map<String, Object> newMap = new HashMap<>();
                context.put(pathElement, newMap);
                context = newMap;
            }
        }

        context.put(leafKey, value);
    }

    /**
     * removes field from source
     * @param path
     * @return {@code true} if field has been removed
     *         {@code false} if field wasn't exist
     */

    public boolean removeField(Template path) {
        return removeField(renderTemplate(path));
    }

    public boolean removeField(String path) {
        if (!hasField(path)) {
            return false;
        }
        Map<String, Object> context = source;
        String[] pathElements = path.split("\\.");

        String leafKey = pathElements[pathElements.length - 1];

        if (pathElements.length > 1) {
            String pathWithoutLeaf = path.substring(0, path.lastIndexOf("."));
            context = getField(pathWithoutLeaf);
        }

        context.remove(leafKey);

        return true;
    }

    public void appendList(Template path, TemplatedValue value) {
        appendList(renderTemplate(path), renderTemplate(value));
    }

    public void appendList(String path, TemplatedValue value) {
        appendList(path, renderTemplate(value));
    }

    public void appendList(String path, Object value) {
        List<Object> list;
        if (!hasField(path)) {
            addField(path, new ArrayList<>());
        }

        Object field = getField(path);
        if (field instanceof List) {
            list = (List) field;
        } else {
            list = new ArrayList<>();
            list.add(field);
            removeField(path);
            addField(path, list);
        }
        if (value instanceof List) {
            list.addAll((List)value);
        } else {
            list.add(value);
        }
    }

    /**
     * removes value from a list
     * @param path
     * @param value
     * @return {@code true} if value removed from list
     *         {@code false} otherwise
     */
    public boolean removeFromList(Template path, TemplatedValue value) {
        return removeFromList(renderTemplate(path), renderTemplate(value));
    }

    public boolean removeFromList(String path, TemplatedValue value) {
        return removeFromList(path, renderTemplate(value));
    }

    public boolean removeFromList(String path, Object value) {
        if (!hasField(path)) {
            return false;
        }

        Object field = getField(path);
        if (field instanceof List) {
            List<Object> list = (List) field;

            if (value instanceof List) {
                list.removeAll((List) value);
            } else {
                list.remove(value);
            }

            return true;
        }

        return false;
    }

    private String renderTemplate(Template template) {
        return template.execute(source);
    }

    private Object renderTemplate(TemplatedValue value) {
        return value.execute(source);
    }

    @Override
    public String toString() {
        return "Doc{" +
                "source=" + source +
                ", metadata=" + metadata +
                '}';
    }
}
