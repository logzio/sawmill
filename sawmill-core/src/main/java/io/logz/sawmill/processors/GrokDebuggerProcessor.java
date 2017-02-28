package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorParseException;
import io.logz.sawmill.utilities.Grok;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.EMPTY_LIST;

@ProcessorProvider(type = "grokDebugger", factory = GrokDebuggerProcessor.Factory.class)
public class GrokDebuggerProcessor implements Processor {

    private final String field;
    private final List<String> expressions;
    private final List<Grok> groks;
    private final List<String> overwrite;
    private final boolean ignoreMissing;

    public GrokDebuggerProcessor(String field, List<String> matchExpressions, Map<String, String> patternsBank, List<String> overwrite, boolean ignoreMissing) {
        checkState(CollectionUtils.isNotEmpty(matchExpressions), "patterns cannot be empty");
        this.field = checkNotNull(field, "field cannot be null");
        this.expressions = matchExpressions;
        this.overwrite = overwrite != null ? overwrite : EMPTY_LIST;
        this.ignoreMissing = ignoreMissing;

        this.groks = new ArrayList<>();

        compileExpressions(matchExpressions, patternsBank);
    }

    private void compileExpressions(List<String> matchExpressions, Map<String, String> patternsBank) {
        matchExpressions.forEach(expression -> {
            Grok grok = new Grok(patternsBank, expression);
            this.groks.add(grok);
        });
    }

    @Override
    public ProcessResult process(Doc doc) {
        if (!doc.hasField(field, String.class)) {
            if (ignoreMissing) return ProcessResult.success();

            return ProcessResult.failure(String.format("failed to grok field in path [%s], field is missing or not instance of [%s]", field, String.class));
        }

        String fieldValue = doc.getField(field);

        List<Grok.Match> matches = getMatches(fieldValue);

        if (CollectionUtils.isEmpty(matches)) {
            doc.appendList("tags", "_grokparsefailure");
            return ProcessResult.failure(String.format("failed to grok field [%s] in path [%s], doesn't match any of the expressions [%s]", fieldValue, field, expressions));
        }

        matches.stream()
                .filter(match -> !CollectionUtils.isEmpty(match.getValues()))
                .forEach(match -> {
                    String field = match.getName();
                    List<Grok.MatchValue> matchValues = match.getMatchValues();
                    Object value = getValue(matchValues);
                    if (overwrite.contains(field) || !doc.hasField(field)) {
                        doc.addField(field, value);
                    } else {
                        doc.appendList(field, value);
                    }
                });

        return ProcessResult.success();
    }

    private Object getValue(List<Grok.MatchValue> matchValues) {
        List<Map<String, Object>> valueList = matchValues.stream().map(matchValue -> {
            Map<String, Object> value = new HashMap<>();
            value.put("value", matchValue.getValue());
            value.put("start", matchValue.getStart());
            value.put("end", matchValue.getEnd());
            return value;
        }).collect(Collectors.toList());

        return valueList.size() == 1 ? valueList.get(0) : valueList;

    }

    private List<Grok.Match> getMatches(String value) {
        for (int i=0; i< groks.size(); i++) {
            List<Grok.Match> captures = groks.get(i).matches(value);
            if (CollectionUtils.isNotEmpty(captures)) {
                return captures;
            }
        }
        return Collections.emptyList();
    }

    public static class Factory implements Processor.Factory {

        private final GrokProcessor.Factory grokProcessorFactory;

        public Factory() {
            this.grokProcessorFactory = new GrokProcessor.Factory();
        }

        public Factory(String dirPath) {
            this.grokProcessorFactory = new GrokProcessor.Factory(dirPath);
        }

        @Override
        public GrokDebuggerProcessor create(Map<String,Object> config) {
            GrokProcessor.Configuration grokConfig = JsonUtils.fromJsonMap(GrokProcessor.Configuration.class, config);

            if (CollectionUtils.isEmpty(grokConfig.getPatterns())) {
                throw new ProcessorParseException("cannot create grok without any pattern");
            }

            return new GrokDebuggerProcessor(
                    grokConfig.getField(),
                    grokConfig.getPatterns(),
                    grokProcessorFactory.loadBuiltinPatterns(),
                    grokConfig.getOverwrite(),
                    grokConfig.getIgnoreMissing());
        }
    }
}
