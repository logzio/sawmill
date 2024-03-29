package io.logz.sawmill.processors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.http.ExternalMappingResponse;
import io.logz.sawmill.http.ExternalMappingsClient;
import io.logz.sawmill.utilities.JsonUtils;
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ProcessorProvider(type = "externalMapping", factory = ExternalMappingSourceProcessor.Factory.class)
public class ExternalMappingSourceProcessor implements Processor {

    private final static Logger logger = LoggerFactory.getLogger(ExternalMappingSourceProcessor.class);

    private final String sourceField;
    private final String targetField;
    private final ExternalMappingsClient externalMappingsClient;
    private final long mappingRefreshPeriodInMillis;

    private static ScheduledExecutorService scheduledRefreshMappingsExecutor;

    private final Supplier<Void> lazyInitSupplier;

    private volatile Map<String, Iterable<String>> keyValueMappingsCache;

    private final AtomicLong lastModifiedTime = new AtomicLong();
    private volatile boolean refreshErrorOccurred = false;

    public ExternalMappingSourceProcessor(Configuration configuration) throws MalformedURLException {
        this.sourceField = requireNonNull(configuration.getSourceField());
        this.targetField = requireNonNull(configuration.getTargetField());
        this.externalMappingsClient = new ExternalMappingsClient(configuration);
        this.mappingRefreshPeriodInMillis = configuration.getMappingRefreshPeriodInMillis();

        lazyInitSupplier = Suppliers.memoize(this::lazyInit);
    }


    private Void lazyInit() {
        refreshExternalMapping();
        if (mappingRefreshPeriodInMillis != Constants.DISABLE_MAPPING_REFRESH) {
            initScheduledExecutor();
            scheduleMappingRefreshTask();
        }
        /* return value is not used */
        return null;
    }

    @VisibleForTesting
    protected void refreshExternalMapping() {
        try {
            ExternalMappingResponse externalMappingResponse = externalMappingsClient.loadMappings(lastModifiedTime.get());
            if (!externalMappingResponse.isModified()) {
                logger.debug("External mapping didn't change since {}. Skipping refresh.", lastModifiedTime.get());
                refreshErrorOccurred = false;
                return;
            }

            this.keyValueMappingsCache = externalMappingResponse.getMappings();
            this.lastModifiedTime.set(externalMappingResponse.getLastModified());
            refreshErrorOccurred = false;
        } catch (Exception e) {
            logger.error("Cannot load external mapping for field {} due to an unexpected error", sourceField, e);
            refreshErrorOccurred = true;
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

        if (keyValueMappingsCache.isEmpty() || refreshErrorOccurred) {
            doc.appendList("tags", Constants.PROCESSOR_FAILURE_TAG);
        }

        if (keyValueMappingsCache.isEmpty()) {
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
                mappingRefreshPeriodInMillis > Constants.MINIMUM_REFRESH_PERIOD_IN_MILLIS || mappingRefreshPeriodInMillis == -1;

            checkState(StringUtils.isNotEmpty(sourceField),
                "failed to create ExternalMappingSourceProcessor, sourceField shouldn't be empty");
            checkState(StringUtils.isNotEmpty(targetField),
                "failed to create ExternalMappingSourceProcessor, targetField shouldn't be empty");
            checkState(StringUtils.isNotEmpty(mappingSourceUrl),
                "failed to create ExternalMappingSourceProcessor, mappingSourceUrl shouldn't be empty");
            checkState(mappingRefreshPeriodIsGreaterThanMinimum,
                "failed to create ExternalMappingSourceProcessor, mappingRefreshPeriodInMillis " +
                    "should be greater or equals to " + Constants.MINIMUM_REFRESH_PERIOD_IN_MILLIS);
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

    public static final class Constants {
        private Constants() {
        }

        public static final int MINIMUM_REFRESH_PERIOD_IN_MILLIS = 15_000;
        public static final int DISABLE_MAPPING_REFRESH = -1;

        public static final long EXTERNAL_MAPPING_MAX_BYTES = 50 * 1024 * 1024;
        public static final long EXTERNAL_MAPPING_MAX_LINES = 50_000;

        public static final String PROCESSOR_FAILURE_TAG = "_externalMappingProcessorFailure";
    }

}
