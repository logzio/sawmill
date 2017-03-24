package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.Template;
import io.logz.sawmill.TemplateService;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@ProcessorProvider(type = "appendList", factory = AppendListProcessor.Factory.class, services = TemplateService.class)
public class AppendListProcessor implements Processor {

    private final Template path;
    private final List<Template> values;

    public AppendListProcessor(Template path, List<Template> values) {
        this.path = checkNotNull(path, "path cannot be null");
        this.values = values;

        checkState(CollectionUtils.isNotEmpty(values), "values cannot be empty");
    }

    @Override
    public ProcessResult process(Doc doc) {
        List<String> renderedValues = values.stream().map(value -> value.render(doc)).collect(Collectors.toList());
        doc.appendList(path.render(doc), renderedValues);
        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {
        private final TemplateService templateService;

        public Factory(TemplateService templateService) {
            this.templateService = templateService;
        }

        @Override
        public AppendListProcessor create(Map<String,Object> config) {
            AppendListProcessor.Configuration configuration = JsonUtils.fromJsonMap(AppendListProcessor.Configuration.class, config);
            Template path = templateService.createTemplate(configuration.getPath());
            List<Template> values = configuration.getValues().stream().map(templateService::createTemplate).collect(Collectors.toList());

            return new AppendListProcessor(path, values);
        }

    }

    public static class Configuration implements Processor.Configuration {

        private String path;
        private List<String> values;

        public Configuration() { }

        public Configuration(String path, List<String> values) {
            this.path = path;
            this.values = values;
        }

        public String getPath() {
            return path;
        }

        public List<String> getValues() {
            return values;
        }

    }

}
