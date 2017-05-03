package io.logz.sawmill.utilities;


import com.google.common.io.Resources;
import ua_parser.CachingParser;
import ua_parser.Parser;

import java.io.IOException;

public class UserAgentParserProvider {

    private final ThreadLocal<Parser> localParser = ThreadLocal.withInitial(() -> {
        try {
            return new CachingParser(Resources.getResource("regexes.yaml").openStream());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create CachingParser:" + e.getMessage(), e);
        }
    });

    public UserAgentParserProvider() {}

    public Parser provide() {
        return localParser.get();
    }
}
