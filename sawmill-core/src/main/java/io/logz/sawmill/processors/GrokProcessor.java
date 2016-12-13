package io.logz.sawmill.processors;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
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
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@ProcessorProvider(type = "grok", factory = GrokProcessor.Factory.class)
public class GrokProcessor implements Processor {
    private final String field;
    private final List<String> expressions;
    private final List<Grok> groks;
    private final List<String> overwrite;
    private final boolean ignoreMissing;

    public GrokProcessor(String field, List<String> matchExpressions, Map<String, String> patternsBank, List<String> overwrite, boolean ignoreMissing) {
        this.field = field;
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
                grok.compile(expression);
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
            if (match.toMap().size() > 0) {
                Collections.swap(groks, 0, i);
                return match.toMap();
            }
        }
        return Collections.EMPTY_MAP;
    }

    public static class Factory implements Processor.Factory {
        private final ImmutableMap<String,String> patternsBank;

        public Factory() {
            this(Resources.getResource("grok/patterns").getFile());
        }

        public Factory(String dirPath) {
            File patternsDirectory = new File(dirPath);
            this.patternsBank = loadPatterns(patternsDirectory);
        }

        private ImmutableMap<String,String> loadPatterns(File dir) {
            Map<String,String> map = new HashMap<>();
            String[] patternFiles = dir.list();

            Pattern pattern = Pattern.compile("^([A-z0-9_]+)\\s+(.*)$");

            for (String patternFileName : patternFiles) {
                try (BufferedReader br = new BufferedReader(new FileReader(dir.getPath() + "/" + patternFileName))){
                    String line;

                    while ((line = br.readLine()) != null) {
                        Matcher m = pattern.matcher(line);
                        if (m.matches()) {
                            map.putIfAbsent(m.group(1), m.group(2));
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(String.format("failed to load pattern file [%s]", patternFileName), e);
                }
            }

            return ImmutableMap.copyOf(map);
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
