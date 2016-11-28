package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.List;

@ProcessorProvider(type = "removeTag", factory = RemoveTagProcessor.Factory.class)
public class RemoveTagProcessor implements Processor {

    private final List<String> tags;

    public RemoveTagProcessor(List<String> tags) {
        this.tags = tags;
    }

    @Override
    public ProcessResult process(Doc doc) {
        doc.removeFromList("tags", tags);
        return ProcessResult.success();
    }

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
