package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.FieldType;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorExecutionException;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ProcessorProvider(type = "csv", factory = CsvProcessor.Factory.class)
public class CsvProcessor implements Processor {
    private final String field;
    private final String targetField;
    private final String separator;
    private final String quoteChar;
    private List<String> columns;
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
        if (!doc.hasField(field, String.class)) {
            return ProcessResult.failure(String.format("failed to parse csv, couldn't find field [%s] or not instance of String", field));
        }

        Map<String, Object> csv = new HashMap<>();
        CSVParser csvParser;
        List<CSVRecord> records;
        String csvString = doc.getField(this.field);

        try {
             csvParser = CSVParser.parse(csvString,
                     CSVFormat.DEFAULT
                             .withDelimiter(getChar(separator))
                             .withQuote(getChar(quoteChar))
                             .withTrim());
            records = csvParser.getRecords();
        } catch (IOException e) {
            return ProcessResult.failure(String.format("failed to parse csv for csv [%s]", csvString),
                    new ProcessorExecutionException("csv", e));
        }

        records.forEach(record -> {
            for (int i = 0; i < record.size(); i++) {
                if (skipEmptyColumns && StringUtils.isEmpty(record.get(i))) {
                    continue;
                }

                boolean columnNameNotExists = columns.size() <= i || StringUtils.isEmpty(columns.get(i));
                if (!autoGenerateColumnNames && columnNameNotExists) {
                    continue;
                }
                String fieldName = columnNameNotExists ? "column" + (i + 1) : columns.get(i);
                csv.put(fieldName, transform(fieldName, StringUtils.strip(record.get(i), quoteChar)));
            }
        });

        if (targetField != null) {
            doc.addField(targetField, csv);
        } else {
            csv.forEach(doc::addField);
        }

        return ProcessResult.success();
    }

    private Object transform(String fieldName, String value) {
        if (MapUtils.isEmpty(convert) || convert.get(fieldName) == null) return value;

        return convert.get(fieldName).convertFrom(value, value);
    }

    private char getChar(String s) {
        return Character.valueOf(s.charAt(0));
    }

    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public CsvProcessor create(Map<String,Object> config) {
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
        private List<String> columns = new ArrayList<>();
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
