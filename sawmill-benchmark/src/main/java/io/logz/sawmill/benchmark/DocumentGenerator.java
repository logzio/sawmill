package io.logz.sawmill.benchmark;

import com.github.javafaker.Faker;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.File;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DocumentGenerator {
    public static final int MAX_FIELDS_AMOUNT = 20;
    public static final int MAX_STRING_FIELD = 100;
    public static final List<String> RESPONSE = Arrays.asList("200", "404", "500", "301");
    public static final List<String> VERB = Arrays.asList("GET", "POST", "DELETE", "PUT");
    public static final List<String> RESOURCE = Arrays.asList("/list","/wp-content","/wp-admin","/explore","/search/tag/list","/app/main/posts","/posts/posts/explore");
    public static final List<String> USER_AGENT = Arrays.asList("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:46.0) Gecko/20100101 Firefox/46.0");

    public static Random random = new Random();
    public static Faker faker = new Faker();

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
                            break;
                        case APACHE:
                            map = generateApacheLog();
                            break;
                    }

                    lines.add(JsonUtils.toJsonString(map));
                }
                FileUtils.writeLines(file, "UTF-8", lines, "\r");
            } catch (IOException e) {
                throw new RuntimeException("failed to generate docs", e);
            }
        }

    }

    private static Map<String, Object> generateApacheLog() {
        Map<String,Object> map = new HashMap<>();
        String datetime = ZonedDateTime.now().minusSeconds(random.nextInt(3000)).format(DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z"));
        String ip = faker.internet().ipV4Address();
        String verb = VERB.get(random.nextInt(VERB.size()));
        String response = RESPONSE.get(random.nextInt(RESPONSE.size()));
        String resource = RESOURCE.get(random.nextInt(RESOURCE.size()));
        String ua = USER_AGENT.get(random.nextInt(USER_AGENT.size()));
        String bytes = String.valueOf(random.nextInt(5000));
        String referrer = faker.internet().url();

        map.put("message",
                String.format("%s - - [%s] \"%s %s HTTP/1.1\" %s %s \"%s\" \"%s\"",ip, datetime, verb, resource, response, bytes, referrer, ua));
        return map;
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
