package io.logz.sawmill.processors;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.http.ExternalMappingsClient;
import io.logz.sawmill.utilities.JsonUtils;
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import static java.util.Objects.requireNonNull;

@ProcessorProvider(type = "externalMapping", factory = ExternalMappingSourceProcessor.Factory.class)
public class ExternalMappingSourceProcessor implements Processor {

    private final String keyFieldName;
    private final String targetFieldName;
    private final ExternalMappingsClient externalMappingsClient;
    private final long mappingRefreshPeriodInSeconds;

    @SuppressWarnings("Guava")
    private final Supplier<Void> lazyInitSupplier;

    private Map<String, List<String>> keyValueMappingsCache;

    public ExternalMappingSourceProcessor(String keyFieldName, String targetFieldName, String mappingSourceUrl, long mappingRefreshPeriodInSeconds) {
        this.keyFieldName = requireNonNull(keyFieldName);
        this.targetFieldName = requireNonNull(targetFieldName);

        try {
            this.externalMappingsClient = new ExternalMappingsClient(requireNonNull(mappingSourceUrl));
        } catch (MalformedURLException e) {
            throw new ProcessorConfigurationException("Cannot create ExternalMappingSourceProcessor due to the malformed url: " + mappingSourceUrl, e);
        }

        this.mappingRefreshPeriodInSeconds = mappingRefreshPeriodInSeconds;

        lazyInitSupplier = Suppliers.memoize(this::lazyInit);
    }


    private Void lazyInit() {
        refreshExternalMapping();
        if (mappingRefreshPeriodInSeconds > 0) {
            startScheduledExecutor();
        }
        /* return value is not used */
        return null;
    }

    private void refreshExternalMapping() {
        keyValueMappingsCache = externalMappingsClient.getMappings();
    }

    private void startScheduledExecutor() {
        ScheduledExecutorService scheduledRulesUpdaterExecutor = Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat("refresh-mapping-executor").setDaemon(true).build());
        scheduledRulesUpdaterExecutor.scheduleAtFixedRate(this::refreshExternalMapping, mappingRefreshPeriodInSeconds,
                mappingRefreshPeriodInSeconds, TimeUnit.SECONDS);
    }

    @Override
    public ProcessResult process(Doc doc) throws InterruptedException {
        lazyInitSupplier.get();

        if (!doc.hasField(keyFieldName)) {
            return ProcessResult.success();
        }

        String key = doc.getField(keyFieldName);
        List<String> values = keyValueMappingsCache.get(key);

        doc.addField(targetFieldName, values != null ? values : Collections.EMPTY_LIST);

        return ProcessResult.success();
    }

    public static class Configuration implements Processor.Configuration {
        private String keyFieldName;
        private String targetFieldName;
        private String mappingSourceUrl;
        private long mappingRefreshPeriodInSeconds = -1;

        public String getKeyFieldName() {
            return keyFieldName;
        }

        public String getTargetFieldName() {
            return targetFieldName;
        }

        public String getMappingSourceUrl() {
            return mappingSourceUrl;
        }

        public long getMappingRefreshPeriodInSeconds() {
            return mappingRefreshPeriodInSeconds;
        }

        public void validate() throws ProcessorConfigurationException {
            if (StringUtils.isEmpty(keyFieldName)) {
                throw new ProcessorConfigurationException("failed to create ExternalMappingSourceProcessor, keyFieldName shouldn't be empty");
            }
            if (StringUtils.isEmpty(targetFieldName)) {
                throw new ProcessorConfigurationException("failed to create ExternalMappingSourceProcessor, targetFieldName shouldn't be empty");
            }
            if (StringUtils.isEmpty(mappingSourceUrl)) {
                throw new ProcessorConfigurationException("failed to create ExternalMappingSourceProcessor, mappingSourceUrl shouldn't be empty");
            }
        }
    }

    public static class Factory implements Processor.Factory {

        @Override
        public ExternalMappingSourceProcessor create(Map<String, Object> config) {
            ExternalMappingSourceProcessor.Configuration processorConfig = JsonUtils.fromJsonMap(ExternalMappingSourceProcessor.Configuration.class, config);
            processorConfig.validate();

            return new ExternalMappingSourceProcessor(processorConfig.getKeyFieldName(), processorConfig.getTargetFieldName(),
                    processorConfig.getMappingSourceUrl(), processorConfig.getMappingRefreshPeriodInSeconds());
        }
    }

}
