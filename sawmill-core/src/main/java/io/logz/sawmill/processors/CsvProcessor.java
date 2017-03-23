package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.FieldType;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@ProcessorProvider(type = "csv", factory = CsvProcessor.Factory.class)
public class CsvProcessor implements Processor {
    private final String field;
    private final String targetField;
    private final String separator;
    private final String quoteChar;
    private final List<String> columns;
    private final boolean autoGenerateColumnNames;
    private final boolean skipEmptyColumns;
    private final Map<String, FieldType> convert;

    public CsvProcessor(String field,
                        String targetField,
                        String separator,
                        String quoteChar,
                        List<String> columns,
                        boolean autoGenerateColumnNames,
                        boolean skipEmptyColumns,
                        Map<String, FieldType> convert) {
        this.field = field;
        this.targetField = targetField;
        this.separator = separator;
        this.quoteChar = quoteChar;
        this.columns = columns;
        this.autoGenerateColumnNames = autoGenerateColumnNames;
        this.skipEmptyColumns = skipEmptyColumns;
        this.convert = convert;
    }

    @Override
    public ProcessResult process(Doc doc) {
        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public Processor create(Map<String,Object> config) {
            CsvProcessor.Configuration csvConfig = JsonUtils.fromJsonMap(CsvProcessor.Configuration.class, config);

            return new CsvProcessor(csvConfig.getField(),
                    csvConfig.getTargetField(),
                    csvConfig.getSeparator(),
                    csvConfig.getQuoteChar(),
                    csvConfig.getColumns(),
                    csvConfig.isAutoGenerateColumnNames(),
                    csvConfig.isSkipEmptyColumns(),
                    csvConfig.getConvert());
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String field = "message";
        private String targetField;
        private String separator = ",";
        private String quoteChar = "\"";
        private List<String> columns;
        private boolean autoGenerateColumnNames = true;
        private boolean skipEmptyColumns = false;
        private Map<String, FieldType> convert;

        public Configuration() { }

        public String getField() {
            return field;
        }

        public String getTargetField() {
            return targetField;
        }

        public String getSeparator() {
            return separator;
        }

        public String getQuoteChar() {
            return quoteChar;
        }

        public List<String> getColumns() {
            return columns;
        }

        public boolean isAutoGenerateColumnNames() {
            return autoGenerateColumnNames;
        }

        public boolean isSkipEmptyColumns() {
            return skipEmptyColumns;
        }

        public Map<String, FieldType> getConvert() {
            return convert;
        }
    }
}
