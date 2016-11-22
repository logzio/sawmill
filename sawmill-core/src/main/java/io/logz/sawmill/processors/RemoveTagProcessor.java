package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RemoveTagProcessor implements Processor {
    private static final String NAME = "removeTag";
    private static final Logger logger = LoggerFactory.getLogger(RemoveTagProcessor.class);


    private final List<String> tags;

    public RemoveTagProcessor(List<String> tags) {
        this.tags = tags;
    }

    @Override
    public String getName() { return NAME; }

    @Override
    public void process(Doc doc) {
        try {
            doc.removeFromList("tags", tags);
        } catch (Exception e) {
            logger.trace("failed to remove tags [{}]", tags, e);
        }
    }

    @ProcessorProvider(name = NAME)
    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public Processor create(String config) {
            RemoveTagProcessor.Configuration removeTagConfig = JsonUtils.fromJsonString(RemoveTagProcessor.Configuration.class, config);

            return new RemoveTagProcessor(removeTagConfig.getTags());
        }
    }

    public static class Configuration implements Processor.Configuration {
        private List<String> tags;

        public Configuration() { }

        public Configuration(List<String> tags) {
            this.tags = tags;
        }

        public List<String> getTags() { return tags; }
    }
}
