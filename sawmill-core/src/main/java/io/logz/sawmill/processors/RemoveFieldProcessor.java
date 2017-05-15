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

import static com.google.common.base.Preconditions.checkNotNull;

@ProcessorProvider(type = "removeField", factory = RemoveFieldProcessor.Factory.class)
public class RemoveFieldProcessor implements Processor {
    private final Template path;
    private final List<Template> fields;

    public RemoveFieldProcessor(Template path) {
        this.path = checkNotNull(path, "path cannot be null");
        this.fields = null;
    }

    public RemoveFieldProcessor(List<Template> fields) {
        this.path = null;
        this.fields = fields;
    }

    @Override
    public ProcessResult process(Doc doc) {
        if (CollectionUtils.isEmpty(fields)) {
            doc.removeField(path.render(doc));
        } else {
            fields.stream().map(field -> field.render(doc)).forEach(doc::removeField);
        }

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
            if (CollectionUtils.isEmpty(removeFieldConfig.getFields())) {
                Template path = templateService.createTemplate(removeFieldConfig.getPath());
                return new RemoveFieldProcessor(path);
            } else {
                List<Template> templateFields = removeFieldConfig.getFields().stream().map(templateService::createTemplate).collect(Collectors.toList());
                return new RemoveFieldProcessor(templateFields);
            }
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String path;
        private List<String> fields;

        public Configuration() { }

        public String getPath() { return path; }

        public List<String> getFields() {
            return fields;
        }
    }
}
