package io.logz.sawmill.executor.inputs;

import io.logz.sawmill.Doc;
import io.logz.sawmill.executor.Input;
import io.logz.sawmill.executor.annotations.InputProvider;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.EMPTY_LIST;

@InputProvider(type = "file", factory = FileInput.Factory.class)
public class FileInput implements Input{
    private final String path;

    public FileInput(String path) {
        checkState(StringUtils.isNotEmpty(path), "path cannot be empty");
        this.path = path;
    }

    @Override
    public List<Doc> listen() {
        try {
            Path path = Paths.get(new URI("file:///" + this.path));
            return Files.lines(path).map(line -> {
                Map<String, Object> source = new HashMap<>();
                source.put("message", line);
                return new Doc(source);
            }).collect(Collectors.toList());
        } catch (Exception e) {
            return EMPTY_LIST;
        }

    }

    public static class Factory implements Input.Factory {
        @Override
        public FileInput create(Map<String, Object> config) {
            FileInput.Configuration fileConfig = JsonUtils.fromJsonMap(FileInput.Configuration.class, config);

            return new FileInput(fileConfig.getPath());
        }
    }

    public static class Configuration implements Input.Configuration {
        private String path;

        public Configuration() {
        }

        public String getPath() {
            return path;
        }

    }
}
