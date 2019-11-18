package net.pincette.jes.prodcon;

import static java.lang.System.exit;
import static java.util.Arrays.stream;
import static net.pincette.util.Util.tryToDoRethrow;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Properties;
import net.pincette.util.ArgsBuilder;

/**
 * The command-line for the JES console consumer and producer. The syntax is as follows: <code>
 * prodcon (consume | produce) (-c | --config-file) config_file (-t | --topic) topic</code> The
 * consumer reads from stdin and the producer writes to stdout. The input stream may be a JSON
 * object or an array of JSON objects.
 *
 * @author Werner Donn\u00e9
 */
public class Application {
  private static final String CONFIG_FILE = "config";
  private static final String CONFIG_FILE_OPT = "--config-file";
  private static final String CONFIG_FILE_OPT_SHORT = "-c";
  private static final String CONSUME = "consume";
  private static final String PRODUCE = "produce";
  private static final String TOPIC = "topic";
  private static final String TOPIC_OPT = "--topic";
  private static final String TOPIC_OPT_SHORT = "-t";

  private static ArgsBuilder adder(final ArgsBuilder builder, final String arg) {
    switch (arg) {
      case CONFIG_FILE_OPT:
      case CONFIG_FILE_OPT_SHORT:
        return builder.addPending(CONFIG_FILE);
      case TOPIC_OPT:
      case TOPIC_OPT_SHORT:
        return builder.addPending(TOPIC);
      default:
        return builder.add(arg);
    }
  }

  public static Runnable consume(final Map<String, String> options, final OutputStream out) {
    return () -> {
      if (!ConsoleConsumer.consume(loadConfig(options), options.get(TOPIC), out)) {
        exit(1);
      }
    };
  }

  private static Properties loadConfig(final Map<String, String> options) {
    return loadConfig(new File(options.get(CONFIG_FILE)));
  }

  private static Properties loadConfig(final File configFile) {
    final Properties config = new Properties();

    tryToDoRethrow(() -> config.load(new FileInputStream(configFile)));

    return config;
  }

  @SuppressWarnings("squid:S106") // Not logging.
  public static void main(final String[] args) {
    stream(args)
        .reduce(new ArgsBuilder(), Application::adder, (b1, b2) -> b1)
        .build()
        .filter(map -> map.containsKey(CONFIG_FILE))
        .filter(map -> map.containsKey(TOPIC))
        .filter(map -> map.containsKey(TOPIC))
        .filter(map -> map.containsKey(CONSUME) || map.containsKey(PRODUCE))
        .map(map -> map.containsKey(CONSUME) ? consume(map, System.out) : produce(map, System.in))
        .orElse(Application::usage)
        .run();

    exit(0);
  }

  private static Runnable produce(final Map<String, String> options, final InputStream in) {
    return () -> {
      if (!ConsoleProducer.produce(loadConfig(options), options.get(TOPIC), in)) {
        exit(1);
      }
    };
  }

  @SuppressWarnings("squid:S106") // Not logging.
  private static void usage() {
    System.err.println(
        "Usage: net.pincette.jes.prodcon.Application (consume | produce) (-c | --config-file) "
            + "config_file (-t | --topic) topic");
    exit(1);
  }
}
