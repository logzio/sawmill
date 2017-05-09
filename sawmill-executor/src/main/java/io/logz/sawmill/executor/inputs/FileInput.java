package io.logz.sawmill.executor.inputs;

import io.logz.sawmill.Doc;
import io.logz.sawmill.executor.Input;
import io.logz.sawmill.executor.annotations.InputProvider;
import io.logz.sawmill.utilities.JsonUtils;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.EMPTY_LIST;

@InputProvider(type = "file", factory = FileInput.Factory.class)
public class FileInput implements Input{
    private final String codec;
    private final String delimiter;
    private final String path;

    public FileInput(String path,
                     String delimiter,
                     String codec) {
        this.path = checkNotNull(path, "path cannot be empty");
        this.delimiter = delimiter;
        this.codec = codec;
    }

    @Override
    public List<Doc> listen() {
        try {
            Path path = Paths.get(new URI(this.path));
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

            return new FileInput(fileConfig.getPath(),
                    fileConfig.getDelimiter(),
                    fileConfig.getCodec());
        }
    }

    public static class Configuration implements Input.Configuration {
        private String codec = "plain";
        private String delimiter = "\n";
        private String path;

        public Configuration() {
        }

        public String getCodec() {
            return codec;
        }

        public String getDelimiter() {
            return delimiter;
        }

        public String getPath() {
            return path;
        }

    }
}
