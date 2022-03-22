package io.logz.sawmill.http;

import com.google.common.collect.Iterables;
import io.logz.sawmill.exceptions.HttpRequestExecutionException;
import io.logz.sawmill.exceptions.ProcessorInitializationException;
import io.logz.sawmill.processors.ExternalMappingSourceProcessor;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHeaders;
import static com.google.common.base.Preconditions.checkState;

public class ExternalMappingsClient {

    private final URL mappingSourceUrl;
    private final int connectTimeout;
    private final int readTimeout;

    public ExternalMappingsClient(ExternalMappingSourceProcessor.Configuration configuration) throws MalformedURLException {
        this.mappingSourceUrl = new URL(configuration.getMappingSourceUrl());
        this.connectTimeout = configuration.getExternalMappingConnectTimeout();
        this.readTimeout = configuration.getExternalMappingReadTimeout();
    }

    public ExternalMappingResponse loadMappings(Long lastModified) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) mappingSourceUrl.openConnection();

        setIfModifiedSinceHeader(conn, lastModified);
        conn.setConnectTimeout(connectTimeout);
        conn.setReadTimeout(readTimeout);

        if (conn.getResponseCode() == HttpURLConnection.HTTP_NOT_MODIFIED) {
            return new ExternalMappingResponse(conn.getLastModified(), null);
        }

        if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
            throw new HttpRequestExecutionException(
                String.format("Couldn't load external mappings. Response status: %d Message: %s",
                    conn.getResponseCode(), conn.getResponseMessage())
            );
        }

        Map<String, Iterable<String>> mappings = loadMappingsFromHttpConnection(conn);
        return new ExternalMappingResponse(conn.getLastModified(), mappings);
    }

    private Map<String, Iterable<String>> loadMappingsFromHttpConnection(HttpURLConnection connection) throws IOException {
        Map<String, Iterable<String>> mappings = new HashMap<>();
        MappingSizeTracker mappingSizeTracker = new MappingSizeTracker();

        try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                if (inputLine.trim().isEmpty()) continue;

                mappingSizeTracker.increaseTotalInputSize(inputLine);
                validateMappingSize(mappingSizeTracker);

                inputLine = StringEscapeUtils.escapeJava(inputLine);
                Pair<String, Iterable<String>> entry = toKeyValuePair(inputLine);
                mappings.merge(entry.getLeft(), entry.getRight(), Iterables::concat);
            }
        }

        return mappings;
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

    private void setIfModifiedSinceHeader(HttpURLConnection conn, Long lastModified) {
        String lastModifiedValue = DateTimeFormatter.RFC_1123_DATE_TIME.format(
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(lastModified), ZoneOffset.UTC));
        conn.setRequestProperty(HttpHeaders.IF_MODIFIED_SINCE, lastModifiedValue);
    }

    private static class MappingSizeTracker {
        private long linesCount = 0;
        private long bytesCount = 0;

        public void increaseTotalInputSize(String inputLine) {
            linesCount++;
            bytesCount += inputLine.getBytes().length;
        }

        public boolean isMaxLinesCountExceeded() {
            return linesCount > ExternalMappingSourceProcessor.Constants.EXTERNAL_MAPPING_MAX_LINES;
        }

        public boolean isMaxSizeExceeded() {
            return bytesCount > ExternalMappingSourceProcessor.Constants.EXTERNAL_MAPPING_MAX_BYTES;
        }
    }
}
