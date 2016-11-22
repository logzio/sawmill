package io.logz.sawmill.benchmark;

import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DocumentGenerator {
    public static final int MAX_FIELDS_AMOUNT = 20;
    public static final int MAX_STRING_FIELD = 100;

    public static Random random = new Random();

    public static void generateDocs(String path, int filesAmount, int docsPerFile, DocType type) {
        for (int i=0; i< filesAmount; i++) {
            try {
                String fileName = RandomStringUtils.randomAlphanumeric(5) + ".json";
                File file = new File(new File(path), fileName);
                List<String> lines = new ArrayList<>();
                for (int j=0; j< docsPerFile; j++) {
                    Map<String, Object> map = new HashMap<>();
                    switch (type) {
                        case RANDOM:
                            map = generateRandomJsonDoc(random.nextInt(MAX_FIELDS_AMOUNT) + 1, 0);
                    }

                    lines.add(JsonUtils.toJsonString(map));
                }
                FileUtils.writeLines(file, "UTF-8", lines, "\r");
            } catch (IOException e) {
                throw new RuntimeException("failed to generate docs", e);
            }
        }

    }

    private static Map<String,Object> generateRandomJsonDoc(int size, int nestingDepth) {
        Map<String,Object> map = new HashMap<>();
        for (int i=0; i < size; i++) {
            String fieldName = RandomStringUtils.randomAlphabetic(5);
            if (nestingDepth < 3 && random.nextBoolean()) {
                Map<String,Object> nestedMap = generateRandomJsonDoc(random.nextInt(MAX_FIELDS_AMOUNT / 2), nestingDepth + 1);
                map.put(fieldName, nestedMap);
            } else if (random.nextBoolean()) {
                map.put(fieldName, generateRandomList(random.nextInt(MAX_FIELDS_AMOUNT / 2)));
            } else {
                map.put(fieldName, RandomStringUtils.randomAlphanumeric(random.nextInt(MAX_STRING_FIELD)));
            }
        }

        return map;
    }

    private static List<String> generateRandomList(int size) {
        return IntStream.range(0,size)
                .mapToObj(i -> RandomStringUtils.randomAlphanumeric(5))
                .collect(Collectors.toList());
    }

    public enum DocType {
        RANDOM,
        APACHE,
        NGINX
    }
}
