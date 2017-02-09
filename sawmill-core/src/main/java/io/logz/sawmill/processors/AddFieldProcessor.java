package io.logz.sawmill.processors;

import com.samskivert.mustache.Template;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.TemplateService;
import io.logz.sawmill.TemplatedValue;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@ProcessorProvider(type = "addField", factory = AddFieldProcessor.Factory.class)
public class AddFieldProcessor implements Processor {
    private final Template path;
    private final TemplatedValue value;

    public AddFieldProcessor(Template path, TemplatedValue value) {
        this.path = checkNotNull(path, "path cannot be null");
        this.value = value;
    }

    @Override
    public ProcessResult process(Doc doc) {
        doc.addField(path, value);
        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public AddFieldProcessor create(Map<String,Object> config) {
            AddFieldProcessor.Configuration addFieldConfig = JsonUtils.fromJsonMap(AddFieldProcessor.Configuration.class, config);

            return new AddFieldProcessor(TemplateService.compileTemplate(addFieldConfig.getPath()), TemplateService.compileValue(addFieldConfig.getValue()));
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String path;
        private Object value;

        public Configuration() { }

        public Configuration(String path, Object value) {
            this.path = path;
            this.value = value;
        }

        public String getPath() { return path; }

        public Object getValue() { return value; }
    }
}
