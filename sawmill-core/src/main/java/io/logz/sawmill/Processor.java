package io.logz.sawmill;

import java.util.List;
import java.util.Map;

public interface Processor {
    ProcessResult process(Doc doc);

    String getType();

    interface Factory {
        Processor create(String config);
    }

    interface Configuration {
    }

    class Definition {
        private String type;
        private String name;
        private Map<String,Object> config;
        private List<Definition> onFailure;

        public Definition() { }

        public String getType() {
            return type;
        }

        public String getName() {
            return name;
        }

        public Map<String,Object> getConfig() {
            return config;
        }

        public List<Definition> getOnFailure() {
            return onFailure;
        }
    }
}
