package net.pincette.jes.prodcon;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.ofSeconds;
import static java.util.UUID.randomUUID;
import static net.pincette.util.Collections.set;
import static net.pincette.util.StreamUtil.stream;
import static net.pincette.util.Util.doForever;
import static net.pincette.util.Util.tryToDoWithRethrow;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Properties;
import java.util.function.Predicate;
import javax.json.JsonObject;
import net.pincette.jes.util.JsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * A JES Kafka consumer for the console. It consumes records from a topic and writes them to a
 * stream. Records are always JSON documents.
 *
 * @author Werner Donn\u00e9
 */
public class ConsoleConsumer {
  private ConsoleConsumer() {}

  /**
   * Consumes JSON records from <code>topic</code> which are serialized with the JES JSON serializer
   * and writes them to <code>out</code>.
   *
   * @param config the Kafka configuration. The "group.id" is always set to a UUID.
   * @param topic the topic to consume.
   * @param out the output stream.
   * @return This method runs forever.
   */
  public static boolean consume(
      final Properties config, final String topic, final OutputStream out) {
    return consume(config, topic, out, json -> true);
  }

  /**
   * Consumes JSON records from <code>topic</code> which are serialized with the JES JSON serializer
   * and writes them to <code>out</code>.
   *
   * @param config the Kafka configuration. The "group.id" is always set to a UUID.
   * @param topic the topic to consume.
   * @param out the output stream.
   * @param filter a filter to eliminate certain objects.
   * @return This method runs forever.
   * @since 1.0.2
   */
  public static boolean consume(
      final Properties config,
      final String topic,
      final OutputStream out,
      final Predicate<JsonObject> filter) {
    final PrintWriter writer = new PrintWriter(new OutputStreamWriter(out, UTF_8));

    config.setProperty("group.id", randomUUID().toString());

    tryToDoWithRethrow(
        () -> new KafkaConsumer<>(config, new StringDeserializer(), new JsonDeserializer()),
        consumer -> {
          consumer.subscribe(set(topic));

          doForever(
              () ->
                  stream(consumer.poll(ofSeconds(1)).iterator())
                      .filter(record -> filter.test(record.value()))
                      .forEach(record -> print(record, writer)));
        });

    return true;
  }

  private static void print(
      final ConsumerRecord<String, JsonObject> record, final PrintWriter writer) {
    writer.println(record.value());
    writer.flush();
  }
}
