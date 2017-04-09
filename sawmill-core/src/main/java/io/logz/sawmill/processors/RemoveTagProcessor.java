package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.Template;
import io.logz.sawmill.TemplateService;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

@ProcessorProvider(type = "removeTag", factory = RemoveTagProcessor.Factory.class)
public class RemoveTagProcessor implements Processor {
    private final List<Template> tags;

    public RemoveTagProcessor(List<Template> tags) {
        checkState(CollectionUtils.isNotEmpty(tags), "tags cannot be empty");
        this.tags = tags;
    }

    @Override
    public ProcessResult process(Doc doc) {
        List<String> renderedTags = tags.stream().map(tag -> tag.render(doc)).collect(Collectors.toList());
        doc.removeFromList("tags", renderedTags);
        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {
        private final TemplateService templateService;

        @Inject
        public Factory(TemplateService templateService) {
            this.templateService = templateService;
        }

        @Override
        public Processor create(Map<String,Object> config) {
            RemoveTagProcessor.Configuration removeTagConfig = JsonUtils.fromJsonMap(RemoveTagProcessor.Configuration.class, config);
            List<Template> tags = removeTagConfig.getTags().stream().map(templateService::createTemplate).collect(Collectors.toList());

            return new RemoveTagProcessor(tags);
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
