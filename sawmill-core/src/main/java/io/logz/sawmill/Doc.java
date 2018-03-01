package io.logz.sawmill;

import org.apache.commons.collections4.MapUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

public class Doc {

    private final Map<String, Object> source;

    public Doc(Map<String, Object> source) {
        checkState(MapUtils.isNotEmpty(source), "source cannot be empty");
        this.source = source;
    }

    public Map<String, Object> getSource() { return source; }

    public boolean hasField(String path) {
        Optional<Object> field = getByPath(source, path);
        return field.isPresent();
    }

    public boolean hasField(String path, Class clazz) {
        Optional<Object> field = getByPath(source, path);
        return field.isPresent() && clazz.isInstance(field.get());
    }

    public <T> T getField(String path) {
        Optional<Object> field = getByPath(source, path);
        checkState(field.isPresent(), "Couldn't resolve field in path [%s]", path);
        return (T) field.get();
    }

    public void addField(String path, Object value) {
        Map<String, Object> context = source;
        List<String> pathElements = tokenizeString(path);

        String leafKey = pathElements.get(pathElements.size() - 1);

        for (String pathElement : pathElements.subList(0, pathElements.size() - 1)) {
            Object pathValue = context.get(pathElement);
            if (pathValue != null && pathValue instanceof Map) {
                context = (Map) pathValue;
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
    public boolean removeField(String path) {
        if (!hasField(path)) {
            return false;
        }
        Map<String, Object> context = source;
        List<String> pathElements = tokenizeString(path);

        String leafKey = pathElements.get(pathElements.size() - 1);

        if (pathElements.size() > 1) {
            String pathWithoutLeaf = path.substring(0, path.lastIndexOf("."));
            context = getField(pathWithoutLeaf);
        }

        context.remove(leafKey);

        return true;
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

    /**
     * json OGNL (Object Graph Navigation Language) getter.
     * <p>for example:
     * <pre>
     * JsonUtils.getByPath(json, "x.y.z")
     * </pre>
     *
     * @return Optional of the value in paths
     * @throws Exception on any error
     **/
    public static <T> Optional<T> getByPath(Map json, String... paths) {
        Object cursor = json;
        for (String path : paths) {
            for (String node : tokenizeString(path)) {
                cursor = ((Map) cursor).get(node);
                if (cursor == null) return Optional.empty();
            }
        }
        return Optional.of((T) cursor);
    }

    public static List<String> tokenizeString(String s) {
        List<String> tokens = new ArrayList<>();
        StringBuilder sb = new StringBuilder();

        char[] charArray = s.toCharArray();
        int i = 0;
        while (i < charArray.length) {
            // Handle escaped character \.
            if (charArray[i] == '\\' && i < (charArray.length - 1) && charArray[i + 1] == '.') {
                sb.append('.');
                i++;

            } else if (charArray[i] == '.') {
                tokens.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.append(charArray[i]);
            }

            i++;
        }

        tokens.add(sb.toString());

        return tokens;
    }

    @Override
    public String toString() {
        return "Doc{" +
                "source=" + source +
                '}';
    }
}
