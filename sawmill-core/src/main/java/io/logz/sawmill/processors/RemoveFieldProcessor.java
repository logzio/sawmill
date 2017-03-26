package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.Template;
import io.logz.sawmill.TemplateService;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

import javax.inject.Inject;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@ProcessorProvider(type = "removeField", factory = RemoveFieldProcessor.Factory.class)
public class RemoveFieldProcessor implements Processor {
    private final Template path;

    public RemoveFieldProcessor(Template path) {
        this.path = checkNotNull(path, "path cannot be null");
    }

    @Override
    public ProcessResult process(Doc doc) {
        doc.removeField(path.render(doc));
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
            RemoveFieldProcessor.Configuration removeFieldConfig = JsonUtils.fromJsonMap(RemoveFieldProcessor.Configuration.class, config);
            Template path = templateService.createTemplate(removeFieldConfig.getPath());

            return new RemoveFieldProcessor(path);
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String path;

        public Configuration() { }

        public Configuration(String path) {
            this.path = path;
        }

        public String getPath() { return path; }
    }
}
