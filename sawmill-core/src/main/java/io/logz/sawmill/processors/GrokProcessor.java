package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorParseException;
import io.logz.sawmill.utilities.JsonUtils;
import io.thekraken.grok.api.Grok;
import io.thekraken.grok.api.Match;
import io.thekraken.grok.api.exception.GrokException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

@ProcessorProvider(type = "grok", factory = GrokProcessor.Factory.class)
public class GrokProcessor implements Processor {
    private final String field;
    private final List<String> expressions;
    private final List<Grok> groks;
    private final List<String> overwrite;
    private final boolean ignoreMissing;

    public GrokProcessor(String field, List<String> matchExpressions, Map<String, String> patternsBank, List<String> overwrite, boolean ignoreMissing) {
        this.field = checkNotNull(field, "field cannot be null");
        this.expressions = matchExpressions;
        this.overwrite = overwrite != null ? overwrite : Collections.EMPTY_LIST;
        this.ignoreMissing = ignoreMissing;

        this.groks = new ArrayList<>();

        compileExpressions(matchExpressions, patternsBank);
    }

    private void compileExpressions(List<String> matchExpressions, Map<String, String> patternsBank) {
        matchExpressions.forEach(expression -> {
            Grok grok = new Grok();

            grok.getPatterns().putAll(patternsBank);

            try {
                grok.compile(expression, true);
                if (grok.getNamedRegex().equals("(?<name0>null)")) {
                    throw new RuntimeException(String.format("failed to create grok processor, unknown expressions [%s]", expressions));
                }

                this.groks.add(grok);
            } catch (GrokException e) {
                throw new RuntimeException(String.format("failed to compile grok pattern [%s]", expression), e);
            }
        });
    }

    @Override
    public ProcessResult process(Doc doc) {
        if (!doc.hasField(field)) {
            if (ignoreMissing) return ProcessResult.success();

            return ProcessResult.failure(String.format("failed to grok field in path [%s], field is missing", field));
        }

        String value = doc.getField(field);

        Map<String, Object> matches = getMatches(value);

        if (MapUtils.isEmpty(matches)) {
            return ProcessResult.failure(String.format("failed to grok field [%s] in path [%s], doesn't match any of the expressions [%s]", value, field, expressions));
        }

        matches.entrySet().stream()
                .filter((e) -> Objects.nonNull(e.getValue()))
                .forEach((e) -> {
                    if (overwrite.contains(e.getKey()) || !doc.hasField(e.getKey())) {
                        doc.addField(e.getKey(), e.getValue());
                    } else {
                        doc.appendList(e.getKey(), e.getValue());
                    }
                });

        return ProcessResult.success();
    }

    private Map<String, Object> getMatches(String value) {
        for (int i=0; i< groks.size(); i++) {
            Match match = groks.get(i).match(value);
            match.captures();
            Map<String, Object> map = match.toMap();
            if (map.size() > 0) {
                if (i > 0) Collections.swap(groks, 0, i); // Use the grok that matched first
                return map;
            }
        }
        return Collections.EMPTY_MAP;
    }

    public static class Factory implements Processor.Factory {
        private static final String[] PATTERN_NAMES = new String[] {
                "gpfs", "grok-patterns", "haproxy",
                "java", "linux-syslog", "mcollective", "mcollective-patterns", "mongodb", "nagios",
                "postgresql", "redis", "ruby", "SYSLOG5424BASEOLDER"
        };

        private final Map<String,String> patternsBank;

        public Factory() {
            this.patternsBank = loadBuiltinPatterns();
        }

        public Factory(String dirPath) {
            File patternsDirectory = new File(dirPath);
            this.patternsBank = loadExternalPatterns(patternsDirectory);
        }

        public Map<String, String> loadBuiltinPatterns() {
            Map<String, String> builtinPatterns = new HashMap<>();
            for (String pattern : PATTERN_NAMES) {
                try(InputStream is = getClass().getResourceAsStream("/grok/patterns/" + pattern)) {
                    loadPatterns(builtinPatterns, is);
                } catch (Exception e) {
                    throw new RuntimeException(String.format("failed to load pattern file [%s]", pattern), e);
                }
            }
            return Collections.unmodifiableMap(builtinPatterns);
        }

        private Map<String,String> loadExternalPatterns(File dir) {
            Map<String,String> externalPatterns = new HashMap<>();
            String[] patternFiles = dir.list();

            for (String patternFileName : patternFiles) {
                try (FileInputStream is = new FileInputStream(dir.getPath() + "/" + patternFileName)){
                    loadPatterns(externalPatterns, is);
                } catch (Exception e) {
                    throw new RuntimeException(String.format("failed to load pattern file [%s]", patternFileName), e);
                }
            }

            return Collections.unmodifiableMap(externalPatterns);
        }

        private void loadPatterns(Map<String, String> patternBank, InputStream inputStream) throws IOException {
            String line;
            try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                while ((line = br.readLine()) != null) {
                    String trimmedLine = line.replaceAll("^\\s+", "");
                    if (trimmedLine.startsWith("#") || trimmedLine.length() == 0) {
                        continue;
                    }

                    String[] parts = trimmedLine.split("\\s+", 2);
                    if (parts.length == 2) {
                        patternBank.put(parts[0], parts[1]);
                    }
                }
            }
        }

        @Override
        public GrokProcessor create(Map<String,Object> config) {
            GrokProcessor.Configuration grokConfig = JsonUtils.fromJsonMap(GrokProcessor.Configuration.class, config);

            if (CollectionUtils.isEmpty(grokConfig.getPatterns())) {
                throw new ProcessorParseException("cannot create grok without any pattern");
            }

            return new GrokProcessor(grokConfig.getField(),
                    grokConfig.getPatterns(),
                    patternsBank,
                    grokConfig.getOverwrite(),
                    grokConfig.getIgnoreMissing() != null ? grokConfig.getIgnoreMissing() : true);
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String field;
        private List<String> patterns;
        private List<String> overwrite;
        private Boolean ignoreMissing;

        public Configuration() { }

        public Configuration(String field, List<String> patterns, List<String> overwrite, boolean ignoreMissing) {
            this.field = field;
            this.patterns = patterns;
            this.overwrite = overwrite;
            this.ignoreMissing = ignoreMissing;
        }

        public String getField() {
            return field;
        }

        public List<String> getPatterns() {
            return patterns;
        }

        public List<String> getOverwrite() {
            return overwrite;
        }

        public Boolean getIgnoreMissing() {
            return ignoreMissing;
        }
    }
}
