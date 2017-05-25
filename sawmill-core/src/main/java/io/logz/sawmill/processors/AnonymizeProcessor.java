package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;

@ProcessorProvider(type = "anonymize", factory = AnonymizeProcessor.Factory.class)
public class AnonymizeProcessor implements Processor {
    private final List<String> fields;
    private final Algorithm algorithm;
    private final String key;

    public AnonymizeProcessor(List<String> fields, Algorithm algorithm, String key) {
        this.fields = requireNonNull(fields, "fields cannot be null");
        this.key = requireNonNull(key, "key cannot be null");
        this.algorithm = algorithm;
    }

    @Override
    public ProcessResult process(Doc doc) {
        List<String> missingFields = new ArrayList<>();
        for (String field : fields) {
            if (!doc.hasField(field, String.class)) {
                missingFields.add(field);
                continue;
            }

            String value = doc.getField(field);
            doc.addField(field, algorithm.anonimize(value));
        }

        if (!missingFields.isEmpty()) {
            return ProcessResult.failure(String.format("failed to anonymize fields in path [%s], fields are missing or not instance of [String]", missingFields));
        }

        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public AnonymizeProcessor create(Map<String,Object> config) {
            AnonymizeProcessor.Configuration anonymizeConfig = JsonUtils.fromJsonMap(AnonymizeProcessor.Configuration.class, config);

            if (CollectionUtils.isEmpty(anonymizeConfig.getFields())) {
                throw new ProcessorConfigurationException("failed to parse anonymize config, cannot be processed without fields");
            }

            if (StringUtils.isEmpty(anonymizeConfig.getKey())) {
                throw new ProcessorConfigurationException("failed to parse anonymize config, cannot be processed without key");
            }

            return new AnonymizeProcessor(anonymizeConfig.getFields(),
                    anonymizeConfig.getAlgorithm(),
                    anonymizeConfig.getKey());
        }
    }

    public static class Configuration implements Processor.Configuration {
        private List<String> fields;
        private Algorithm algorithm = Algorithm.SHA1;
        private String key;

        public Configuration() { }

        public List<String> getFields() {
            return fields;
        }

        public Algorithm getAlgorithm() {
            return algorithm;
        }

        public String getKey() {
            return key;
        }
    }

    public enum Algorithm {
        SHA1 {
            @Override
            public String anonimize(String value) {
                return DigestUtils.sha1Hex(value);
            }
        },
        SHA256 {
            @Override
            public String anonimize(String value) {
                return DigestUtils.sha256Hex(value);
            }
        },
        SHA384 {
            @Override
            public String anonimize(String value) {
                return DigestUtils.sha384Hex(value);
            }
        },
        SHA512 {
            @Override
            public String anonimize(String value) {
                return DigestUtils.sha512Hex(value);
            }
        },
        MD5 {
            @Override
            public String anonimize(String value) {
                return DigestUtils.md5Hex(value);
            }
        };

        public abstract String anonimize(String value);
    }
}
