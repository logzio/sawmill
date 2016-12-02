package io.logz.sawmill.processors;

import com.google.common.io.Resources;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;
import io.thekraken.grok.api.Grok;
import io.thekraken.grok.api.Match;
import io.thekraken.grok.api.exception.GrokException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@ProcessorProvider(type = GrokProcessor.TYPE, factory = GrokProcessor.Factory.class)
public class GrokProcessor implements Processor {
    public static final String TYPE = "grok";

    private final String field;
    private final Grok grok;

    public GrokProcessor(String field, String matchPattern, Map<String, String> patternsBank) {
        this.field = field;

        this.grok = new Grok();
        grok.getPatterns().putAll(patternsBank);

        try {
            grok.compile(matchPattern);
        } catch (GrokException e) {
            throw new RuntimeException(String.format("failed to compile grok pattern [%s]", matchPattern), e);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public ProcessResult process(Doc doc) {
        if (!doc.hasField(field)) {
            return ProcessResult.failure(String.format("failed to grok field in path [%s], field is missing", field));
        }

        String value = doc.getField(field);

        Map<String, Object> matches = getMatches(value);

        matches.entrySet().stream()
                .filter((e) -> Objects.nonNull(e.getValue()))
                .forEach((e) -> doc.addField(e.getKey(), e.getValue()));

        return ProcessResult.success();
    }

    private Map<String, Object> getMatches(String value) {
        Match match = grok.match(value);
        match.captures();
        return match.toMap();
    }

    public static class Factory implements Processor.Factory {
        private Map<String,String> patternsBank;

        public Factory() {
            this(Resources.getResource("patterns").getFile());
        }

        public Factory(String dirPath) {
            this.patternsBank = new HashMap<>();
            File patternsDirectory = new File(dirPath);
            loadPatterns(patternsDirectory);
        }

        private void loadPatterns(File dir) {
            String[] patternFiles = dir.list();

            Pattern pattern = Pattern.compile("^([A-z0-9_]+)\\s+(.*)$");

            for (String patternFileName : patternFiles) {
                try (BufferedReader br = new BufferedReader(new FileReader(dir.getPath() + "/" + patternFileName))){
                String line;

                while ((line = br.readLine()) != null) {
                    Matcher m = pattern.matcher(line);
                    if (m.matches()) {
                        patternsBank.put(m.group(1), m.group(2));
                    }
                }
                } catch (Exception e) {
                    throw new RuntimeException(String.format("failed to load pattern file [%s]", patternFileName), e);
                }
            }
        }

        @Override
        public GrokProcessor create(String config) {
            GrokProcessor.Configuration grokConfig = JsonUtils.fromJsonString(GrokProcessor.Configuration.class, config);

            return new GrokProcessor(grokConfig.getField(), grokConfig.getPattern(), patternsBank);
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String field;
        private String pattern;

        public Configuration() { }

        public Configuration(String field, String pattern) {
            this.field = field;
            this.pattern = pattern;
        }

        public String getField() {
            return field;
        }

        public String getPattern() {
            return pattern;
        }
    }
}
