package net.pincette.jes.prodcon;

import static java.util.logging.Level.SEVERE;
import static java.util.logging.Logger.getGlobal;
import static java.util.stream.Collectors.toMap;
import static javax.json.Json.createParser;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.Kafka.createReliableProducer;
import static net.pincette.jes.util.Kafka.send;
import static net.pincette.util.StreamUtil.composeAsyncStream;
import static net.pincette.util.Util.getStackTrace;
import static net.pincette.util.Util.tryToGetWithRethrow;

import java.io.InputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import javax.json.JsonValue;
import net.pincette.function.SideEffect;
import net.pincette.jes.util.JsonSerializer;
import net.pincette.util.Json;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * A JES Kafka producer for the console. It reads JSON from an input stream and produces messages on
 * the given topic using the JES serializer.
 *
 * @author Werner Donn\u00e9
 */
public class ConsoleProducer {
  private ConsoleProducer() {}

  private static Map<String, Object> asMap(final Properties properties) {
    return properties.entrySet().stream()
        .collect(toMap(e -> e.getKey().toString(), Entry::getValue));
  }

  private static boolean logError(final Throwable e) {
    return SideEffect.<Boolean>run(() -> getGlobal().log(SEVERE, getStackTrace(e)))
        .andThenGet(() -> false);
  }

  /**
   * Reads JSON from <code>in</code> and produces JES serialized messages on <code>topic</code>.
   *
   * @param config the Kafka configuration.
   * @param topic the topic to produce on.
   * @param in a JSON object or a JSON object array. The field "_id" is used as the message key.
   * @return Returns <code>true</code> if the operation was successful.
   */
  public static boolean produce(final Properties config, final String topic, final InputStream in) {
    return tryToGetWithRethrow(
            () ->
                createReliableProducer(asMap(config), new StringSerializer(), new JsonSerializer()),
            producer ->
                composeAsyncStream(
                        net.pincette.jf.Util.stream(createParser(in))
                            .filter(Json::isObject)
                            .map(JsonValue::asJsonObject)
                            .filter(json -> json.containsKey(ID))
                            .map(
                                json ->
                                    send(
                                            producer,
                                            new ProducerRecord<>(topic, json.getString(ID), json))
                                        .exceptionally(ConsoleProducer::logError)))
                    .thenApply(results -> results.reduce((r1, r2) -> r1 && r2).orElse(true))
                    .toCompletableFuture()
                    .get())
        .orElse(false);
  }
}