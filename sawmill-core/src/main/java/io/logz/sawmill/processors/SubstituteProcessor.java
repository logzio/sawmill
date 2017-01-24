package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorParseException;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.google.common.base.Preconditions.checkNotNull;

@ProcessorProvider(type = "gsub", factory = SubstituteProcessor.Factory.class)
public class SubstituteProcessor implements Processor {

    private final String field;
    private final Pattern pattern;
    private final String replacement;

    public SubstituteProcessor(String field, Pattern pattern, String replacement) {
        this.field = checkNotNull(field, "field cannot be null");
        this.pattern = checkNotNull(pattern, "pattern cannot be null");
        this.replacement = checkNotNull(replacement, "replacement cannot be null");
    }

    @Override
    public ProcessResult process(Doc doc) {
        if (!doc.hasField(field, String.class)) {
            return ProcessResult.failure(String.format("failed to convert field in path [%s], field is missing or not instance of [%s]", field, String.class));
        }
        String beforeSubstitute = doc.getField(field);
        Matcher matcher = pattern.matcher(beforeSubstitute);
        String afterSubstitute = matcher.replaceAll(replacement);

        doc.addField(field, afterSubstitute);
        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public SubstituteProcessor create(Map<String,Object> config) {
            SubstituteProcessor.Configuration subConfig = JsonUtils.fromJsonMap(SubstituteProcessor.Configuration.class, config);

            Pattern pattern;
            try {
                pattern = Pattern.compile(subConfig.getPattern());
            } catch (PatternSyntaxException e) {
                throw new ProcessorParseException("cannot create gsub processor with invalid pattern");
            }

            return new SubstituteProcessor(subConfig.getField(), pattern, subConfig.getReplacement());
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String field;
        private String pattern;
        private String replacement;

        public Configuration() { }

        public String getField() {
            return field;
        }

        public String getPattern() {
            return pattern;
        }

        public String getReplacement() {
            return replacement;
        }
    }
}
