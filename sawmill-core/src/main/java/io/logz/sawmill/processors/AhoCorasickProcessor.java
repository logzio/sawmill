package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.processors.ahocorasick.AhoCorasickModel;
import io.logz.sawmill.utilities.JsonUtils;
import org.ahocorasick.trie.Emit;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@ProcessorProvider(type = "ahoCorasick", factory = AhoCorasickProcessor.Factory.class)
public class AhoCorasickProcessor implements Processor {

    private String field;
    private String targetField;

    private AhoCorasickModel ahoCorasickModel;

    public AhoCorasickProcessor(String field, String targetField, List<String> inputWords) {
        validateInputWords(inputWords);

        this.field = requireNonNull(field);
        this.targetField = requireNonNull(targetField);

        ahoCorasickModel = new AhoCorasickModel();
        ahoCorasickModel.build(inputWords);
    }

    @Override
    public ProcessResult process(Doc doc) {
        if (!doc.hasField(field, String.class)) {
            return ProcessResult.failure(String.format("failed to process date, field in path [%s] is missing", field));
        }

        Collection<Emit> emits = ahoCorasickModel.search(doc.getField(field));
        List<String> result = emits.stream().map(emit -> emit.getKeyword()).collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(result)) {
            doc.addField(targetField, result);
            return ProcessResult.success();
        }
        return ProcessResult.failure("Failed to match input words for target field - " + targetField);
    }

    private static void validateInputWords(List<String> input) {
        if (CollectionUtils.isEmpty(input)) {
            throw new ProcessorConfigurationException("cannot create AhoCorasickProcessor without any input words/files");
        }
    }

    public static class Factory implements Processor.Factory {
        public Factory() {}

        @Override
        public Processor create(Map<String,Object> config) {
            AhoCorasickProcessor.Configuration ahoCorasickConfig = JsonUtils.fromJsonMap(AhoCorasickProcessor.Configuration.class, config);

            validateInputWords(ahoCorasickConfig.getInputWords());

            String field = ahoCorasickConfig.getField();
            String targetField = ahoCorasickConfig.getTargetField();

            List<String> inputWords = ahoCorasickConfig.getInputWords();

            return new AhoCorasickProcessor(field, targetField, inputWords);
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String field;
        private String targetField = "ahocorasick";

        //input words for the algoritm
        private List<String> inputWords;

        public Configuration() { }

        public String getField() { return field; }

        public String getTargetField() {
            return targetField;
        }

        public List<String> getInputWords() {
            return inputWords;
        }

    }
}
