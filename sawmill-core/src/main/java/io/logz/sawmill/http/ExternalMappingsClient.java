package io.logz.sawmill.http;

import io.logz.sawmill.exceptions.HttpRequestExecutionException;
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
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExternalMappingsClient {

    private final Logger logger = LoggerFactory.getLogger(ExternalMappingsClient.class);
    private final URL mappingSourceUrl;

    public ExternalMappingsClient(String mappingSourceUrl) throws MalformedURLException {
        this.mappingSourceUrl = new URL(mappingSourceUrl);
    }

    public Map<String, Iterable<String>> getMappings() {
        Map<String, Iterable<String>> mappings = new HashMap<>();

        try {
            HttpURLConnection conn = (HttpURLConnection) mappingSourceUrl.openConnection();
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);

            if (conn.getResponseCode() != 200) {
                throw new HttpRequestExecutionException("Couldn't load external mappings. Message: " + conn.getResponseMessage());
            }

            try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    Pair<String, Iterable<String>> entry = toKeyValuePair(inputLine);
                    mappings.put(entry.getLeft(), entry.getRight());
                }
            }

        } catch (IOException e) {
            logger.error("Failed to get external mappings", e);
            throw new HttpRequestExecutionException(e.getMessage(), e);
        }

        return mappings;
    }

    private Pair<String, Iterable<String>> toKeyValuePair(String inputLine) {
        String[] keyVal = inputLine.split("=");
        String[] split = keyVal[1].trim().split(",");

        String key = keyVal[0].trim();
        Iterable<String> values = Arrays.stream(split)
                .map(String::trim)
                .collect(Collectors.toList());

        return new ImmutablePair<>(key, values);
    }
}
