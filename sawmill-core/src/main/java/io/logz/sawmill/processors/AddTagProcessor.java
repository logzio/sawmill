package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.TemplateService;
import io.logz.sawmill.TemplatedValue;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.List;
import java.util.Map;

@ProcessorProvider(type = "addTag", factory = AddTagProcessor.Factory.class)
public class AddTagProcessor implements Processor {
    private final TemplatedValue tags;

    public AddTagProcessor(TemplatedValue tags) {
        // TODO: TEST checkState(CollectionUtils.isNotEmpty(tags), "tags cannot be empty");
        this.tags = tags;
    }

    @Override
    public ProcessResult process(Doc doc) {
        doc.appendList("tags", tags);
        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public AddTagProcessor create(Map<String,Object> config) {
            AddTagProcessor.Configuration addTagConfig = JsonUtils.fromJsonMap(AddTagProcessor.Configuration.class, config);

            return new AddTagProcessor(TemplateService.compileValue(addTagConfig.getTags()));
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
