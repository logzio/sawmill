package io.logz.sawmill.executor;

import com.typesafe.config.ConfigException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.io.FileUtils;

import java.io.File;

public class Main {
    public static OptionParser parser = new OptionParser();
    public static OptionSpec<File> configFileSpec = parser.accepts("f", "config path").withRequiredArg().ofType(File.class);

    public static void main(String... args) throws Exception {

        OptionSet set = parser.parse(args);
        try {
            String config = FileUtils.readFileToString(configFileSpec.value(set), "UTF-8");

            ExecutionPlan executionPlan = new ExecutionPlan.Factory().create(config);

            SawmillExecutor sawmillExecutor = new SawmillExecutor();

            sawmillExecutor.execute(executionPlan);

        } catch (ConfigException e) {
        System.err.println("Error parsing config file:");
        System.err.println(" " + e);
        parser.printHelpOn(System.err);
        System.exit(1);
        } catch (Exception e) {
            System.err.println("Error parsing command line:");
            System.err.println(" " + e);
            parser.printHelpOn(System.err);
            System.exit(1);
        }
    }
}
