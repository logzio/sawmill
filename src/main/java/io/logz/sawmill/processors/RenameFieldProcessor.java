package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RenameFieldProcessor implements Processor {
    public static final String NAME = "renameField";
    private static final Logger logger = LoggerFactory.getLogger(RenameFieldProcessor.class);

    private final String from;
    private final String to;

    public RenameFieldProcessor(String from, String to) {
        this.from = from;
        this.to = to;
    }

    @Override
    public String getName() { return NAME; }

    @Override
    public void process(Doc doc) {
        try {
            Object fieldValue = doc.getField(from);
            doc.removeField(from);
            doc.addField(to, fieldValue);
        } catch (Exception e) {
            logger.trace("failed to rename field [{}] to [{}]", from, to, e);
        }
    }

    @ProcessorProvider(name = NAME)
    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public Processor create(String config) {
            RenameFieldProcessor.Configuration renameFieldConfig = JsonUtils.fromJsonString(RenameFieldProcessor.Configuration.class, config);

            return new RenameFieldProcessor(renameFieldConfig.getFrom(), renameFieldConfig.getTo());
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String from;
        private String to;

        public Configuration() { }

        public Configuration(String from, String to) {
            this.from = from;
            this.to = to;
        }

        public String getFrom() { return from; }

        public String getTo() { return to; }
    }
}
