package io.logz.sawmill.executor;

import joptsimple.OptionParser;
import joptsimple.OptionSpec;

import java.io.File;

public class Main {
    public static OptionParser parser = new OptionParser();
    public static OptionSpec<File> configFileSpec = parser.accepts("f", "config path").withRequiredArg().ofType(File.class);

    public static void main(String... args) throws Exception {

    }
}
