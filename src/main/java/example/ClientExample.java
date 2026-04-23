package example;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;

public class ClientExample {
  private static final String PRODUCE_TOPIC = "dkp-java-client-test";
  private static final String CONSUME_TOPIC = "dkp-java-client-test";
  private static final int MESSAGE_COUNT = 10;

  public static void main(String[] args) {
    try {
      final Properties producerConfig = readConfig("producer-client.properties");
      final Properties consumerConfig = readConfig("consumer-client.properties");

      produce(PRODUCE_TOPIC, producerConfig);
      consume(CONSUME_TOPIC, consumerConfig);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static Properties readConfig(final String configFile) throws IOException {
    if (!Files.exists(Paths.get(configFile))) {
      throw new IOException(configFile + " not found.");
    }

    final Properties config = new Properties();
    try (InputStream inputStream = new FileInputStream(configFile)) {
      config.load(inputStream);
    }

    return config;
  }

  public static void produce(String topic, Properties config) {
    try (Producer<String, String> producer = new KafkaProducer<>(config)) {
      for (int i = 0; i < MESSAGE_COUNT; i++) {
        final String key = "key-" + i;
        final String value = "hello from Java OIDC producer " + i;
        producer.send(new ProducerRecord<>(topic, key, value), (metadata, exception) -> {
          if (exception == null) {
            System.out.println(
                String.format(
                    "Produced message to topic %s partition %d offset %d: key = %s value = %s",
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset(),
                    key,
                    value));
          } else {
            exception.printStackTrace();
          }
        });
      }

      producer.flush();
    }
  }

  public static void consume(String topic, Properties config) {
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
      Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
      consumer.subscribe(Arrays.asList(topic));

      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

        if (records.isEmpty()) {
          System.out.println("No records yet.");
          continue;
        }

        for (ConsumerRecord<String, String> record : records) {
          System.out.println(
              String.format(
                  "Consumed message from topic %s: key = %s value = %s",
                  topic,
                  record.key(),
                  record.value()));
        }
      }
    } catch (WakeupException e) {
      System.out.println("Shutdown signal received; closing consumer.");
    }
  }
}