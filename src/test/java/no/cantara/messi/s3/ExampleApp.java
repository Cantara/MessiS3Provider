package no.cantara.messi.s3;

import com.google.protobuf.ByteString;
import no.cantara.config.ApplicationProperties;
import no.cantara.config.ProviderLoader;
import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiClientFactory;
import no.cantara.messi.api.MessiConsumer;
import no.cantara.messi.api.MessiProducer;
import no.cantara.messi.api.MessiULIDUtils;
import no.cantara.messi.protos.MessiMessage;

import java.util.concurrent.TimeUnit;

public class ExampleApp {

    public static void main(String[] args) throws Exception {
        ApplicationProperties configuration = ApplicationProperties.builder()
                .values()
                .put("local-temp-folder", "target/_tmp_avro_")
                .put("avro-file.max.seconds", "3600")
                .put("avro-file.max.bytes", Long.toString(10 * 1024 * 1024)) // 10 MiB
                .put("avro-file.sync.interval", Long.toString(200))
                .put("s3.bucket-name", "your-s3-bucket-name")
                .put("s3.listing.min-interval-seconds", Long.toString(3))
                .put("s3.region", "your-aws-region")
                .put("s3.credential-provider", "default") // Use 'static' if you want to provide access key and secret
                // If using static credentials, uncomment these lines and provide your AWS credentials
                // .put("s3.access-key-id", "your-access-key-id")
                // .put("s3.secret-access-key", "your-secret-access-key")
                .end()
                .build();

        final MessiClient client = ProviderLoader.configure(configuration,
                "s3", MessiClientFactory.class);

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
                    System.out.printf("Consumed message with id: %s%n", MessiULIDUtils.toUlid(message.getUlid()));
                    if (message.getExternalId().equals("582AACB30")) {
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
            producer.publish(MessiMessage.newBuilder().setExternalId("4BA210EC2")
                    .putData("the-payload", ByteString.copyFromUtf8("Hello 1"))
                    .build());
            producer.publish(MessiMessage.newBuilder().setExternalId("B827B4CCE")
                    .putData("the-payload", ByteString.copyFromUtf8("Hello 2"))
                    .putData("metadata", ByteString.copyFromUtf8("created-time " + System.currentTimeMillis()))
                    .build());
            producer.publish(MessiMessage.newBuilder().setExternalId("582AACB30")
                    .putData("the-payload", ByteString.copyFromUtf8("Hello 3"))
                    .build());
        }
    }
}