package io.logz.sawmill.processors;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import io.logz.sawmill.Log;
import io.logz.sawmill.Processor;

import java.util.Map;

public class TestProcessor implements Processor {
    public static final String TYPE = "test";

    public final String value;

    public TestProcessor(String value) {
        this.value = value;
    }

    @Override
    public void execute(Log log) {

    }

    @Override
    public String getType() { return TYPE; }

    public String getValue() { return value; }

    public static final class Factory implements Processor.Factory {
        public Factory() {

        }

        @Override
        public Processor create(Map<String,Object> config) {
            Gson gson = new Gson();
            JsonElement jsonConfig = gson.toJsonTree(config);
            Configuration testConfiguration = gson.fromJson(jsonConfig, Configuration.class);

            return new TestProcessor(testConfiguration.getValue());
        }
    }

    public class Configuration implements Processor.Configuration {
        private String value;

        public Configuration() { }

        public Configuration(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
}
