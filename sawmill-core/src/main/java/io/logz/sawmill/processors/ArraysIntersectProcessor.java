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

@ProcessorProvider(type = "arraysIntersect", factory = ArraysIntersectProcessor.Factory.class)
public class ArraysIntersectProcessor implements Processor {

    private final String sourceFieldA;
    private final String sourceFieldB;
    private final String targetField;

    public ArraysIntersectProcessor(String sourceFieldA, String sourceFieldB, String targetField) {
        this.sourceFieldA = sourceFieldA;
        this.sourceFieldB = sourceFieldB;
        this.targetField = targetField;
    }

    @Override
    public ProcessResult process(Doc doc) {
        if (!doc.hasField(sourceFieldA) || !doc.hasField(sourceFieldB)) {
            return ProcessResult.failure("One or both input fields are missing");
        }

        Iterable<Object> arrayA = doc.getField(sourceFieldA);
        Iterable<Object> arrayB = doc.getField(sourceFieldB);

        Set<Object> intersection = new HashSet<>(intersection(arrayA, arrayB));
        doc.addField(targetField, intersection);

        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {

        @Inject
        public Factory() {}

        @Override
        public Processor create(Map<String, Object> config) {
            ArraysIntersectProcessor.Configuration arraysIntersectionConfig = JsonUtils.fromJsonMap(ArraysIntersectProcessor.Configuration.class, config);

            requireNonNull(arraysIntersectionConfig.getSourceFieldA(), "sourceFieldA cannot be null");
            requireNonNull(arraysIntersectionConfig.getSourceFieldB(), "sourceFieldB cannot be null");
            requireNonNull(arraysIntersectionConfig.getTargetField(), "targetField cannot be null");

            return new ArraysIntersectProcessor(arraysIntersectionConfig.getSourceFieldA(), arraysIntersectionConfig.getSourceFieldB(), arraysIntersectionConfig.targetField);
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
