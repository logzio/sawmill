package io.logz.sawmill.processors;

import com.google.common.annotations.VisibleForTesting;
import java.util.function.Supplier;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ProcessorProvider(type = "externalMapping", factory = ExternalMappingSourceProcessor.Factory.class)
public class ExternalMappingSourceProcessor implements Processor {

    public static final int MINIMUM_REFRESH_PERIOD_IN_MILLIS = 15_000;
    public static final int DISABLE_MAPPING_REFRESH = -1;

    private final static Logger logger = LoggerFactory.getLogger(ExternalMappingSourceProcessor.class);

    private final String sourceField;
    private final String targetField;
    private final ExternalMappingsClient externalMappingsClient;
    private final long mappingRefreshPeriodInMillis;

    private static ScheduledExecutorService scheduledRefreshMappingsExecutor;

    private final Supplier<Void> lazyInitSupplier;

    private volatile Map<String, Iterable<String>> keyValueMappingsCache;

    public ExternalMappingSourceProcessor(Configuration configuration) throws MalformedURLException {
        this.sourceField = requireNonNull(configuration.getSourceField());
        this.targetField = requireNonNull(configuration.getTargetField());
        this.externalMappingsClient = new ExternalMappingsClient(configuration);
        this.mappingRefreshPeriodInMillis = configuration.getMappingRefreshPeriodInMillis();

        lazyInitSupplier = Suppliers.memoize(this::lazyInit);
    }


    private Void lazyInit() {
        refreshExternalMapping();
        if (mappingRefreshPeriodInMillis != DISABLE_MAPPING_REFRESH) {
            initScheduledExecutor();
            scheduleMappingRefreshTask();
        }
        /* return value is not used */
        return null;
    }

    @VisibleForTesting
    protected void refreshExternalMapping() {
        try {
            keyValueMappingsCache = externalMappingsClient.loadMappings();
        } catch (Exception e) {
            logger.error("Cannot load external mapping for field {} due to an unexpected error", sourceField, e);
        }

        if (keyValueMappingsCache == null) {
            keyValueMappingsCache = new HashMap<>();
        }

        if (keyValueMappingsCache.isEmpty()) {
            logger.error("Cannot load external mapping for field: {}, mapping is empty", sourceField);
        }
    }

    private void initScheduledExecutor() {
        if (scheduledRefreshMappingsExecutor != null) return;
        scheduledRefreshMappingsExecutor = Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("refresh-mapping-executor").setDaemon(true).build());
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdownScheduledExecutor, "shutdown-hook-thread"));
    }

    private void scheduleMappingRefreshTask() {
        scheduledRefreshMappingsExecutor.scheduleAtFixedRate(this::refreshExternalMapping, 0,
            mappingRefreshPeriodInMillis, TimeUnit.MILLISECONDS);
    }

    private void shutdownScheduledExecutor() {
        scheduledRefreshMappingsExecutor.shutdownNow();
    }

    @Override
    public ProcessResult process(Doc doc) throws InterruptedException {
        lazyInitSupplier.get();

        if (!doc.hasField(sourceField)) {
            return ProcessResult.failure(String.format("field [%s] is missing", sourceField));
        }

        if (keyValueMappingsCache.isEmpty()) {
            doc.appendList("tags", "_externalMappingProcessorFailure");
            return ProcessResult.failure(String.format("field [%s] mapping is missing, external mapping source is empty", sourceField));
        }

        String sourceField = doc.getField(this.sourceField);
        Iterable<String> values = keyValueMappingsCache.get(StringEscapeUtils.escapeJava(sourceField));

        doc.addField(targetField, values != null ? values : Collections.EMPTY_LIST);

        return ProcessResult.success();
    }

    public static class Configuration implements Processor.Configuration {
        private String sourceField;
        private String targetField;
        private String mappingSourceUrl;
        private long mappingRefreshPeriodInMillis = 60_000;

        private int externalMappingConnectTimeout = 5000;
        private int externalMappingReadTimeout = 10000;

        public Configuration() {
        }

        @VisibleForTesting
        Configuration(
            String sourceField, String targetField, String mappingSourceUrl, long mappingRefreshPeriodInMillis) {
            this.sourceField = sourceField;
            this.targetField = targetField;
            this.mappingSourceUrl = mappingSourceUrl;
            this.mappingRefreshPeriodInMillis = mappingRefreshPeriodInMillis;
        }

        public String getSourceField() {
            return sourceField;
        }

        public String getTargetField() {
            return targetField;
        }

        public String getMappingSourceUrl() {
            return mappingSourceUrl;
        }

        public long getMappingRefreshPeriodInMillis() {
            return mappingRefreshPeriodInMillis;
        }

        public int getExternalMappingReadTimeout() {
            return externalMappingReadTimeout;
        }

        public int getExternalMappingConnectTimeout() {
            return externalMappingConnectTimeout;
        }

        public void validate() throws IllegalStateException {
            boolean mappingRefreshPeriodIsGreaterThanMinimum =
                mappingRefreshPeriodInMillis > MINIMUM_REFRESH_PERIOD_IN_MILLIS || mappingRefreshPeriodInMillis == -1;

            checkState(StringUtils.isNotEmpty(sourceField),
                "failed to create ExternalMappingSourceProcessor, sourceField shouldn't be empty");
            checkState(StringUtils.isNotEmpty(targetField),
                "failed to create ExternalMappingSourceProcessor, targetField shouldn't be empty");
            checkState(StringUtils.isNotEmpty(mappingSourceUrl),
                "failed to create ExternalMappingSourceProcessor, mappingSourceUrl shouldn't be empty");
            checkState(mappingRefreshPeriodIsGreaterThanMinimum,
                "failed to create ExternalMappingSourceProcessor, mappingRefreshPeriodInMillis " +
                    "should be greater or equals to " + MINIMUM_REFRESH_PERIOD_IN_MILLIS);
            checkState(externalMappingConnectTimeout > 0, "externalMappingConnectTimeout should be greater than 0");
            checkState(externalMappingReadTimeout > 0, "externalMappingReadTimeout should be greater than 0");
        }
    }

    public static class Factory implements Processor.Factory {

        @Override
        public ExternalMappingSourceProcessor create(Map<String, Object> config) {
            ExternalMappingSourceProcessor.Configuration processorConfig = JsonUtils.fromJsonMap(ExternalMappingSourceProcessor.Configuration.class, config);
            processorConfig.validate();

            try {
                return new ExternalMappingSourceProcessor(processorConfig);
            } catch (MalformedURLException e) {
                throw new ProcessorConfigurationException("Cannot create ExternalMappingSourceProcessor due to the malformed url: "
                    + processorConfig.getMappingSourceUrl(), e);
            }
        }
    }

}
