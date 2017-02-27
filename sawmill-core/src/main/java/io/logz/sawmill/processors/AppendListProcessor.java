package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

@ProcessorProvider(type = "appendList", factory = AppendListProcessor.Factory.class)
public class AppendListProcessor implements Processor {

    private final String path;
    private final List<String> values;

    public AppendListProcessor(String path, List<String> values) {
        this.path = path;
        this.values = values;

        checkState(StringUtils.isNotEmpty(path), "path cannot be null");
        checkState(CollectionUtils.isNotEmpty(values), "values cannot be empty");
    }

    @Override
    public ProcessResult process(Doc doc) {
        doc.appendList(path, values);
        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {

        @Override
        public AppendListProcessor create(Map<String,Object> config) {
            AppendListProcessor.Configuration configuration = JsonUtils.fromJsonMap(AppendListProcessor.Configuration.class, config);
            return new AppendListProcessor(configuration.getPath(), configuration.getValues());
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
