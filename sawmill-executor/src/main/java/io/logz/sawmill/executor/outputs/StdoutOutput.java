package io.logz.sawmill.executor.outputs;

import io.logz.sawmill.Doc;
import io.logz.sawmill.executor.Output;
import io.logz.sawmill.executor.annotations.OutputProvider;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.List;
import java.util.Map;

@OutputProvider(type = "stdout", factory = StdoutOutput.Factory.class)
public class StdoutOutput implements Output {

    public StdoutOutput() {
    }

    @Override
    public void send(List<Doc> docs) {
        docs.forEach(doc -> System.out.println(doc.getSource().toString()));
    }

    public static class Factory implements Output.Factory {

        @Override
        public StdoutOutput create(Map<String, Object> config) {
            StdoutOutput.Configuration stdoutConfig = JsonUtils.fromJsonMap(StdoutOutput.Configuration.class, config);

            return new StdoutOutput();
        }
    }

    public static class Configuration implements Output.Configuration {
    }
}
