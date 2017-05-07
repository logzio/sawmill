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

@ProcessorProvider(type = "rename", factory = RenameFieldProcessor.Factory.class)
public class RenameFieldProcessor implements Processor {
    private final Template from;
    private final Template to;

    public RenameFieldProcessor(Template from, Template to) {
        this.from = checkNotNull(from, "from field path cannot be null");
        this.to = checkNotNull(to, "to field path cannot be null");
    }

    @Override
    public ProcessResult process(Doc doc) {
        String renderedFrom = from.render(doc);
        String renderedTo = to.render(doc);
        if (!doc.hasField(renderedFrom)) {
            return ProcessResult.failure(String.format("failed to rename field [%s] to [%s], couldn't find field", renderedFrom, renderedTo));
        }
        Object fieldValue = doc.getField(renderedFrom);
        doc.removeField(renderedFrom);
        doc.addField(renderedTo, fieldValue);

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

            Template toTemplate = templateService.createTemplate(renameFieldConfig.getTo());
            Template fromTemplate = templateService.createTemplate(renameFieldConfig.getFrom());
            return new RenameFieldProcessor(fromTemplate, toTemplate);
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
