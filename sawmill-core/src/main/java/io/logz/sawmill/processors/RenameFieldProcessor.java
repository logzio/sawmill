package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.Template;
import io.logz.sawmill.TemplateService;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@ProcessorProvider(type = "rename", factory = RenameFieldProcessor.Factory.class)
public class RenameFieldProcessor implements Processor {
    private final Map<Template, Template> renames;

    public RenameFieldProcessor(Map<Template, Template> renames) {
        this.renames = requireNonNull(renames, "cannot work without any renames");
    }

    @Override
    public ProcessResult process(Doc doc) {
        List<String> missingFields = new ArrayList<>();
        for (Map.Entry<Template, Template> rename : renames.entrySet()) {
            String renderedFrom = rename.getKey().render(doc);
            if (!doc.hasField(renderedFrom)) {
                missingFields.add(renderedFrom);
                continue;
            }
            String renderedTo = rename.getValue().render(doc);
            Object fieldValue = doc.getField(renderedFrom);
            doc.removeField(renderedFrom);
            doc.addField(renderedTo, fieldValue);
        }

        if (!missingFields.isEmpty()) {
            return ProcessResult.failure(String.format("failed to rename fields [%s], fields are missing", missingFields));
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
            RenameFieldProcessor.Configuration renameFieldConfig = JsonUtils.fromJsonMap(RenameFieldProcessor.Configuration.class, config);

            Map<Template, Template> renames = new HashMap<>();

            if (MapUtils.isEmpty(renameFieldConfig.getRenames()) && (StringUtils.isEmpty(renameFieldConfig.getFrom()) || StringUtils.isEmpty(renameFieldConfig.getTo())))
            {
                throw new ProcessorConfigurationException("failed to parse rename processor config, couldn't resolve rename/s");
            }

            if (StringUtils.isNotEmpty(renameFieldConfig.getTo()) && StringUtils.isNotEmpty(renameFieldConfig.getFrom())) {
                renames.put(templateService.createTemplate(renameFieldConfig.getFrom()),
                        templateService.createTemplate(renameFieldConfig.getTo()));
            }

            if (MapUtils.isNotEmpty(renameFieldConfig.getRenames())) {
                renameFieldConfig.getRenames().entrySet()
                        .forEach((entry -> renames.put(templateService.createTemplate(entry.getKey()),
                                templateService.createTemplate(entry.getValue()))));
            }

            return new RenameFieldProcessor(renames);
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String from;
        private String to;
        private Map<String, String> renames;

        public Configuration() { }

        public Configuration(String from, String to) {
            this.from = from;
            this.to = to;
        }

        public String getFrom() { return from; }

        public String getTo() { return to; }

        public Map<String, String> getRenames() {
            return renames;
        }
    }
}
