package io.logz.sawmill.executor.inputs;

import io.logz.sawmill.Doc;
import io.logz.sawmill.executor.Input;
import io.logz.sawmill.executor.annotations.InputProvider;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkNotNull;

@InputProvider(type = "file", factory = FileInput.Factory.class)
public class FileInput implements Input{
    private final String codec;
    private final String delimiter;
    private final String path;
    private final String startPosition;
    private final List<String> exclude;

    public FileInput(String path,
                     String startPosition,
                     String delimiter,
                     String codec,
                     List<String> exclude) {
        this.path = checkNotNull(path, "path cannot be empty");
        this.startPosition = startPosition;
        this.delimiter = delimiter;
        this.codec = codec;
        this.exclude = exclude;
    }

    @Override
    public List<Doc> listen() {
        return null;
    }

    public static class Factory implements Input.Factory {
        @Override
        public FileInput create(Map<String, Object> config) {
            FileInput.Configuration fileConfig = JsonUtils.fromJsonMap(FileInput.Configuration.class, config);

            return new FileInput(fileConfig.getPath(),
                    fileConfig.getStartPosition(),
                    fileConfig.getDelimiter(),
                    fileConfig.getCodec(),
                    fileConfig.getExclude());
        }
    }

    public static class Configuration implements Input.Configuration {
        private String codec = "plain";
        private String delimiter = "\n";
        private String path;
        private String startPosition = "end";
        private List<String> exclude;

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

        public String getStartPosition() {
            return startPosition;
        }

        public List<String> getExclude() {
            return exclude;
        }
    }
}
