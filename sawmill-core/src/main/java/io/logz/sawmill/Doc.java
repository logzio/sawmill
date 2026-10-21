package io.logz.sawmill;

import org.apache.commons.collections4.MapUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

public class Doc {

    private final Map<String, Object> source;

    public Doc(Map<String, Object> source) {
        checkState(MapUtils.isNotEmpty(source), "source cannot be empty");
        this.source = source;
    }

    public Map<String, Object> getSource() { return source; }

    public boolean hasField(String path) {
        return lookup(source, path) != null;
    }

    public boolean hasField(String path, Class clazz) {
        return clazz.isInstance(lookup(source, path));
    }

    public <T> T getField(String path) {
        Object field = lookup(source, path);
        checkState(field != null, "Couldn't resolve field in path [%s]", path);
        return (T) field;
    }

    public void addField(String path, Object value) {
        if (isSingleField(path)) {
            source.put(path, value);
            return;
        }
        Map<String, Object> context = source;
        List<String> pathElements = tokenizePath(path);

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
        if (isSingleField(path)) {
            source.remove(path);
            return true;
        }
        Map<String, Object> context = source;
        List<String> pathElements = tokenizePath(path);

        List<String> pathElementsWithoutLeaf = pathElements.subList(0, pathElements.size() - 1);
        String leafKey = pathElements.get(pathElements.size() - 1);

        for (String currElement : pathElementsWithoutLeaf) {
            context = (Map<String, Object>) context.get(currElement);
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
     * lookup(json, "x.y.z")
     * </pre>
     *
     * @return the value in path, or null if it is missing or null
     **/
    private static Object lookup(Map json, String path) {
        if (isSingleField(path)) {
            return json.get(path);
        }
        Object cursor = json;
        for (String pathElement : tokenizePath(path)) {
            if (!(cursor instanceof Map)) return null;

            cursor = ((Map) cursor).get(pathElement);
            if (cursor == null) return null;
        }
        return cursor;
    }

    /** A path with nothing to split or unescape: one key of the source map. */
    private static boolean isSingleField(String path) {
        return path.indexOf('.') < 0 && path.indexOf('\\') < 0;
    }

    private static List<String> tokenizePath(String s) {
        List<String> pathTokens = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inEscape = false;

        for (char c : s.toCharArray()) {
            if (inEscape) {
                inEscape = false;
                sb.append(c);
            } else if (c == '\\') {
                inEscape = true;
            } else if (c == '.') {
                pathTokens.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        pathTokens.add(sb.toString());

        return pathTokens;
    }
    public boolean replaceFieldValue(String path,Object newValue){
        if (hasField(path)) {
            removeField(path);
            addField(path,newValue);
            return true;
        }
        return false;
    }

    public void replace(Map<String,Object> otherMap){
        source.clear();
        source.putAll(otherMap);
    }

    @Override
    public String toString() {
        return "Doc{" +
                "source=" + source +
                '}';
    }
}
