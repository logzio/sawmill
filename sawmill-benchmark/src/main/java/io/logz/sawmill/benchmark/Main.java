package io.logz.sawmill.benchmark;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.logz.sawmill.utilities.JsonUtils;
import joptsimple.NonOptionArgumentSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.io.FileUtils;
import org.openjdk.jmh.runner.Defaults;
import org.openjdk.jmh.runner.NoBenchmarksException;
import org.openjdk.jmh.runner.ProfilersFailedException;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.File;
import java.util.Arrays;

public class Main {

    public static OptionParser parser = new OptionParser();

    public static NonOptionArgumentSpec<String> modeSpec = parser.nonOptions("Run Mode: normal JMH or scenario file").ofType(String.class);
    public static OptionSpec<File> configFileSpec = parser.accepts("sf", "Scenario file").withRequiredArg().ofType(File.class);

    public static void main(String... args) throws Exception {
        Runner runner = null;
        Options opts = null;
        RunMode mode = null;

        OptionSet set = parser.parse(args);

        String modeValue = modeSpec.value(set);

        try {
            mode = RunMode.valueOf(modeValue.toUpperCase());
        } catch (Exception e) {
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        switch (mode) {
            case JMH:
                org.openjdk.jmh.Main.main(Arrays.copyOfRange(args, 1, args.length));
                System.exit(0);
            case SCENARIO_FILE: {
                try {
                    String config = FileUtils.readFileToString(configFileSpec.value(set), "UTF-8");
                    String json = ConfigFactory.parseString(config).root().render(ConfigRenderOptions.concise());
                    opts = JsonUtils.fromJsonString(SawmillBenchmarkOptions.class, json).toJmhOptions();

                    runner = new Runner(opts);
                } catch (Exception e) {
                    System.err.println("Error parsing command line:");
                    System.err.println(" " + e);
                    parser.printHelpOn(System.err);
                    System.exit(1);
                }

                try {
                    runner.run();
                } catch (NoBenchmarksException e) {
                    System.err.println("No matching benchmarks. Miss-spelled regexp?");

                    if (opts.verbosity().orElse(Defaults.VERBOSITY) != VerboseMode.EXTRA) {
                        System.err.println("Use " + VerboseMode.EXTRA + " verbose mode to debug the pattern matching.");
                    } else {
                        runner.list();
                    }
                    System.exit(1);
                } catch (ProfilersFailedException e) {
                    // This is not exactly an error, set non-zero exit code
                    System.err.println(e.getMessage());
                    System.exit(1);
                } catch (RunnerException e) {
                    System.err.print("ERROR: ");
                    e.printStackTrace(System.err);
                    System.exit(1);
                }
            }
        }
    }

    public enum RunMode {
        JMH,
        SCENARIO_FILE
    }
}
