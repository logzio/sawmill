package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.collections4.CollectionUtils.intersection;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;

@ProcessorProvider(type = "listIntersect", factory = ListIntersectProcessor.Factory.class)
public class ListIntersectProcessor implements Processor {

    private final String sourceFieldA;
    private final String sourceFieldB;
    private final String targetField;

    public ListIntersectProcessor(String sourceFieldA, String sourceFieldB, String targetField) {
        this.sourceFieldA = sourceFieldA;
        this.sourceFieldB = sourceFieldB;
        this.targetField = targetField;
    }

    @Override
    public ProcessResult process(Doc doc) {
        Iterable<Object> listA = doc.getField(sourceFieldA);
        Iterable<Object> listB = doc.getField(sourceFieldB);

        Set<Object> intersection = new HashSet<>(intersection(listA, listB));
        doc.addField(targetField, intersection);

        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {

        @Inject
        public Factory() {}

        @Override
        public Processor create(Map<String, Object> config) {
            ListIntersectProcessor.Configuration listIntersectConfig = JsonUtils.fromJsonMap(ListIntersectProcessor.Configuration.class, config);

            requireNonNull(listIntersectConfig.getSourceFieldA(), "sourceFieldA cannot be null");
            requireNonNull(listIntersectConfig.getSourceFieldB(), "sourceFieldB cannot be null");
            requireNonNull(listIntersectConfig.getTargetField(), "targetField cannot be null");

            return new ListIntersectProcessor(listIntersectConfig.getSourceFieldA(), listIntersectConfig.getSourceFieldB(), listIntersectConfig.targetField);
        }

    }

    public static class Configuration implements Processor.Configuration {
        private String sourceFieldA;
        private String sourceFieldB;
        private String targetField;

        public Configuration() {}

        public Configuration(String sourceFieldA, String sourceFieldB, String targetField) {
            this.sourceFieldA = sourceFieldA;
            this.sourceFieldB = sourceFieldB;
            this.targetField = targetField;
        }

        public String getSourceFieldA() {
            return sourceFieldA;
        }

        public String getSourceFieldB() {
            return sourceFieldB;
        }

        public String getTargetField() {
            return targetField;
        }
    }
}
