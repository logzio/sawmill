package io.logz.sawmill.http;

import com.google.common.collect.Iterables;
import com.google.common.net.HttpHeaders;
import io.logz.sawmill.exceptions.HttpRequestExecutionException;
import io.logz.sawmill.exceptions.ProcessorInitializationException;
import io.logz.sawmill.processors.ExternalMappingSourceProcessor;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.google.common.base.Preconditions.checkState;

public class ExternalMappingsClient {

    private static final Logger logger = LoggerFactory.getLogger(ExternalMappingsClient.class);

    private final URL mappingSourceUrl;
    private final int connectTimeout;
    private final int readTimeout;

    public ExternalMappingsClient(ExternalMappingSourceProcessor.Configuration configuration) throws MalformedURLException {
        this.mappingSourceUrl = new URL(configuration.getMappingSourceUrl());
        this.connectTimeout = configuration.getExternalMappingConnectTimeout();
        this.readTimeout = configuration.getExternalMappingReadTimeout();
    }

    public Map<String, Iterable<String>> loadMappings() {
        Map<String, Iterable<String>> mappings = new HashMap<>();

        try {
            HttpURLConnection conn = (HttpURLConnection) mappingSourceUrl.openConnection();
            conn.setConnectTimeout(connectTimeout);
            conn.setReadTimeout(readTimeout);

            if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                throw new HttpRequestExecutionException("Couldn't load external mappings. Message: " + conn.getResponseMessage());
            }

            if (hasContentLengthHeader(conn)) {
                validateContentLength(conn);
            }

            loadMappingsFromHttpConnection(conn, mappings);
        } catch (IOException e) {
            logger.error("Failed to get external mappings", e);
            throw new HttpRequestExecutionException(e.getMessage(), e);
        }

        return mappings;
    }

    private void loadMappingsFromHttpConnection(HttpURLConnection connection, Map<String, Iterable<String>> mappings) throws IOException {
        MappingSizeTracker mappingSizeTracker = new MappingSizeTracker(!hasContentLengthHeader(connection));

        try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                if (inputLine.isEmpty()) continue;

                mappingSizeTracker.increaseTotalInputSize(inputLine);
                validateMappingSize(mappingSizeTracker);

                inputLine = StringEscapeUtils.escapeJava(inputLine);
                Pair<String, Iterable<String>> entry = toKeyValuePair(inputLine);
                mappings.merge(entry.getLeft(), entry.getRight(), Iterables::concat);
            }
        }
    }

    private void validateContentLength(HttpURLConnection conn) {
        long externalMappingSize = Long.parseLong(conn.getHeaderField(HttpHeaders.CONTENT_LENGTH));
        if (externalMappingSize > ExternalMappingSourceProcessor.Constants.EXTERNAL_MAPPING_MAX_BYTES) {
            throw new ProcessorInitializationException(
                String.format("Cannot load external mappings, the size of the mapping %d exceeds the limit of %d bytes",
                    externalMappingSize, ExternalMappingSourceProcessor.Constants.EXTERNAL_MAPPING_MAX_BYTES)
            );
        }
    }

    private boolean hasContentLengthHeader(HttpURLConnection conn) {
        return conn.getHeaderField(HttpHeaders.CONTENT_LENGTH) != null;
    }

    private void validateMappingSize(MappingSizeTracker mappingSizeTracker) {
        if (mappingSizeTracker.isMaxSizeExceeded()) {
            throw new ProcessorInitializationException(
                String.format("Cannot load external mappings, the mapping size exceeds the limit of %d bytes",
                    ExternalMappingSourceProcessor.Constants.EXTERNAL_MAPPING_MAX_BYTES)
            );
        }

        if (mappingSizeTracker.isMaxLinesCountExceeded()) {
            throw new ProcessorInitializationException(
                String.format("Cannot load external mappings, the mapping length exceeds the limit of %d lines",
                    ExternalMappingSourceProcessor.Constants.EXTERNAL_MAPPING_MAX_LINES)
            );
        }
    }

    private Pair<String, Iterable<String>> toKeyValuePair(String inputLine) {
        checkState(inputLine.contains("="), "Key-value inputs should be delimited using '=' sign");

        String[] keyValSplit = inputLine.split("=");
        String[] arraySplit = keyValSplit[1].trim().split(",");

        String key = keyValSplit[0].trim();
        checkState(StringUtils.isNotEmpty(key), "Invalid mapping key value. Key should not be empty");

        Iterable<String> values = Arrays.stream(arraySplit)
            .map(String::trim)
            .collect(Collectors.toList());

        return new ImmutablePair<>(key, values);
    }

    private static class MappingSizeTracker {
        private long linesCount = 0;
        private long bytesCount = 0;

        private final boolean trackBytesCount;

        public MappingSizeTracker(boolean trackBytesCount) {
            this.trackBytesCount = trackBytesCount;
        }

        public void increaseTotalInputSize(String inputLine) {
            linesCount++;
            if (trackBytesCount) {
                bytesCount += inputLine.getBytes().length;
            }
        }

        public boolean isMaxLinesCountExceeded() {
            return linesCount > ExternalMappingSourceProcessor.Constants.EXTERNAL_MAPPING_MAX_LINES;
        }

        public boolean isMaxSizeExceeded() {
            return trackBytesCount && bytesCount > ExternalMappingSourceProcessor.Constants.EXTERNAL_MAPPING_MAX_BYTES;
        }
    }
}
