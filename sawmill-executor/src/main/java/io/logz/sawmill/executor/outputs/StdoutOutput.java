package io.logz.sawmill.executor.outputs;

import io.logz.sawmill.Doc;
import io.logz.sawmill.executor.Output;
import io.logz.sawmill.executor.annotations.OutputProvider;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.Map;

@OutputProvider(type = "stdout", factory = StdoutOutput.Factory.class)
public class StdoutOutput implements Output {

    private final String codec;

    public StdoutOutput(String codec) {
        this.codec = codec;
    }

    @Override
    public void send(Doc doc) {

    }

    public static class Factory implements Output.Factory {

        @Override
        public StdoutOutput create(Map<String, Object> config) {
            StdoutOutput.Configuration stdoutConfig = JsonUtils.fromJsonMap(StdoutOutput.Configuration.class, config);

            return new StdoutOutput(stdoutConfig.getCodec());
        }
    }

    public static class Configuration implements Output.Configuration {
        private String codec = "plain";

        public Configuration() {
        }

        public String getCodec() {
            return codec;
        }
    }
}
