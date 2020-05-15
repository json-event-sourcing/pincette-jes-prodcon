# JSON Event Sourcing Console Consumer and Producer

This command-line tool can consume JSON messages, serialized with the JES serializer, from a Kafka topic. It writes one message per line to the console. It can also produce messages on a Kafka topic using the JES serializer. The input from the console can be one JSON object or an array of JSON objects. The field ```_id``` is used for the message key.

You can build it with ```mvn clean package```.

Run it as follows:

```
> java -jar target/pincette-jes-prodcon-<version>-jar-with-dependencies.jar
  (consume | produce)
  (-c | --config-file) $HOME/.ccloud/config
  (-t | --topic) topic
```

It reads from stdin when producing and writes to stdout when consuming. The format of the configuration file is the same as for your ccloud configuration.

See also the [API documentation](https://www.javadoc.io/doc/net.pincette/pincette-jes-prodcon/latest/index.html).
