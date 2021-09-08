package no.cantara.messi.cloudstorage;

import no.cantara.config.ApplicationProperties;
import no.cantara.config.ProviderLoader;
import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiClientFactory;
import no.cantara.messi.api.MessiConsumer;
import no.cantara.messi.api.MessiMessage;
import no.cantara.messi.api.MessiProducer;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class ExampleApp {

    public static void main(String[] args) throws Exception {
        ApplicationProperties configuration = ApplicationProperties.builder()
                .values()
                .put("local-temp-folder", "target/_tmp_avro_")
                .put("avro-file.max.seconds", "3600")
                .put("avro-file.max.bytes", Long.toString(10 * 1024 * 1024)) // 10 MiB
                .put("avro-file.sync.interval", Long.toString(200))
                .put("gcs.bucket-name", "kcg-experimental-bucket")
                .put("gcs.listing.min-interval-seconds", Long.toString(3))
                .put("gcs.credential-provider", "service-account")
                .put("gcs.service-account.key-file", "secret/gcs_test_sa.json")
                .end()
                .build();

        final MessiClient client = ProviderLoader.configure(configuration,
                "gcs", MessiClientFactory.class);

        Thread consumerThread = new Thread(() -> consumeMessages(client));
        consumerThread.start();

        produceMessages(client);

        consumerThread.join();
    }

    static void consumeMessages(MessiClient client) {
        try (MessiConsumer consumer = client.consumer("my-messi-stream")) {
            for (; ; ) {
                MessiMessage message = consumer.receive(30, TimeUnit.SECONDS);
                if (message != null) {
                    System.out.printf("Consumed message with id: %s%n", message.ulid());
                    if (message.externalId().equals("582AACB30")) {
                        return;
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static void produceMessages(MessiClient client) {
        try (MessiProducer producer = client.producer("my-messi-stream")) {
            producer.publish(MessiMessage.builder().externalId("4BA210EC2")
                    .put("the-payload", "Hello 1".getBytes(StandardCharsets.UTF_8))
                    .build());
            producer.publish(MessiMessage.builder().externalId("B827B4CCE")
                    .put("the-payload", "Hello 2".getBytes(StandardCharsets.UTF_8))
                    .put("metadata", ("created-time " + System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8))
                    .build());
            producer.publish(MessiMessage.builder().externalId("582AACB30")
                    .put("the-payload", "Hello 3".getBytes(StandardCharsets.UTF_8))
                    .build());
        }
    }
}
