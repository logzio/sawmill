package io.logz.sawmill.benchmark;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.logz.sawmill.utilities.JsonUtils;
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

public class Main {

    public static final String NORMAL_MODE = "normal";

    public static void main(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<String> runningMode = parser.accepts("m", "Running Mode: sawmill or normal (default: sawmill)").withRequiredArg().ofType(String.class);
        OptionSpec<String> configFilePath = parser.accepts("cp", "Config File Path").withOptionalArg().ofType(String.class);
        OptionSet set = parser.parse(args);

        if (set.has(runningMode) && runningMode.value(set).toLowerCase().equals(NORMAL_MODE)) {
            org.openjdk.jmh.Main.main(args);
            System.exit(0);
        }

        try {
            File configFile = new File(configFilePath.value(set));
            String config = FileUtils.readFileToString(configFile, "UTF-8");
            String json = ConfigFactory.parseString(config).root().render(ConfigRenderOptions.concise());
            Options opts = JsonUtils.fromJsonString(SawmillBenchmarkOptions.class, json).withParams();

            Runner runner = new Runner(opts);

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
        } catch (Exception e) {
            System.err.println("Error parsing command line:");
            System.err.println(" " + e.getMessage());
            System.exit(1);
        }

    }}
