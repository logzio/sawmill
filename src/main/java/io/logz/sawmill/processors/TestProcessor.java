package io.logz.sawmill.processors;

import io.logz.sawmill.Log;
import io.logz.sawmill.Processor;

import java.util.Map;

public class TestProcessor implements Processor {
    public static final String TYPE = "sample";

    public TestProcessor() {
    }

    @Override
    public void execute(Log log) {

    }

    @Override
    public String getType() { return TYPE; }

    public static final class Factory implements Processor.Factory {
        public Factory() {

        }

        @Override
        public Processor create(Map<String, Object> config) {
            return new TestProcessor();
        }
    }
}
