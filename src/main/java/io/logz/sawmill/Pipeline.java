package io.logz.sawmill;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Pipeline {
    private final List<Processor> processors;

    public Pipeline(List<Processor> processors) {
        if (processors == null || processors.isEmpty()) throw new IllegalArgumentException("processors cannot be empty");
        this.processors = processors;
    }

    public List<Processor> getProcessors() {
        return processors;
    }

    public void execute(Log log) {
        for (Processor processor : processors) {
            try {
                processor.execute(log);
            } catch (Exception e) {
                throw new RuntimeException(String.format("failed to execute processor %s on log %s", processor.getType(), log.toString()), e);
            }
        }
    }

    public static final class Factory {

        public Pipeline create(Map<String, Object> config, Map<String, Processor.Factory> proccesorFactories) {
            List<Processor> processors = new ArrayList<>();

            // TODO: parsing mechanism should extract all the relevant processors
            processors.add(proccesorFactories.get("sample").create(config));

            return new Pipeline(processors);
        }
    }
}
