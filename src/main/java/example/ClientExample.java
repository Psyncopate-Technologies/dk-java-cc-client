package example;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class ClientExample {
  public static void main(String[] args) {
    try {
      String topic = "without_dkp_test";

      final Properties producerConfig = readConfig("producer-client.properties");
      final Properties consumerConfig = readConfig("consumer-client.properties");

      produce(topic, producerConfig);
      consume(topic, consumerConfig);
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
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    String key = "key";
    String value = "hello from Java OIDC producer 1";

    try (Producer<String, String> producer = new KafkaProducer<>(config)) {
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

      producer.flush();
    }
  }

  public static void consume(String topic, Properties config) {
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "dkp-java-group-1");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
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
    }
  }
}