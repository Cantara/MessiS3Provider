package no.cantara.messi.s3;

import com.google.protobuf.ByteString;
import no.cantara.config.ApplicationProperties;
import no.cantara.config.ProviderLoader;
import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiClientFactory;
import no.cantara.messi.api.MessiConsumer;
import no.cantara.messi.api.MessiMetadataClient;
import no.cantara.messi.api.MessiProducer;
import no.cantara.messi.protos.MessiMessage;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 * Running these tests requires an accessible AWS S3 bucket with read and write object access.
 * <p>
 * Requirements: AWS credentials with access to read and write objects in S3.
 * These can be provided through environment variables, AWS credentials file, or EC2 instance profile.
 */
@Ignore
public class S3MessiClientIntegrationTest {

    MessiClient client;
    S3Client s3Client;

    @BeforeMethod
    public void createClient() throws IOException {
        ApplicationProperties.Builder applicationPropertiesBuilder = ApplicationProperties.builder()
                .values()
                .put("local-temp-folder", "target/_tmp_avro_")
                .put("avro-file.max.seconds", "3")
                .put("avro-file.max.bytes", Long.toString(2 * 1024)) // 2 KiB
                .put("avro-file.sync.interval", Long.toString(200))
                .put("s3.bucket-name", "your-test-bucket-name")
                .put("s3.listing.min-interval-seconds", Long.toString(3))
                .put("s3.region", "your-aws-region")
                .put("s3.credential-provider", "default")
                .end();

        String messiS3Bucket = System.getenv("MESSI_S3_BUCKET");
        if (messiS3Bucket != null) {
            applicationPropertiesBuilder.property("s3.bucket-name", messiS3Bucket);
        }

        ApplicationProperties configuration = applicationPropertiesBuilder.build();

        Path localTempFolder = Paths.get(configuration.get("local-temp-folder"));
        if (Files.exists(localTempFolder)) {
            Files.walk(localTempFolder).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
        Files.createDirectories(localTempFolder);
        client = ProviderLoader.configure(configuration, "s3", MessiClientFactory.class);

        // Get S3Client
        s3Client = S3MessiClientFactory.getS3Client(configuration);

        // clear bucket
        String bucket = configuration.get("s3.bucket-name");
        clearBucket(bucket, "the-topic");
    }

    private void clearBucket(String bucket, String prefix) {
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix)
                .build();

        ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);

        for (S3Object object : listResponse.contents()) {
            DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                    .bucket(bucket)
                    .key(object.key())
                    .build();
            s3Client.deleteObject(deleteRequest);
        }
    }

    @AfterMethod
    public void closeClient() {
        client.close();
        s3Client.close();
    }

    @Test
    public void thatMostFunctionsWorkWhenIntegratedWithS3() throws Exception {
        // The rest of the test method remains largely the same
        // Only the setup and teardown parts needed significant changes

        assertNull(client.lastMessage("the-topic"));

        {
            MessiMetadataClient metadata = client.metadata("the-topic");
            assertEquals(metadata.topic(), "the-topic");
            assertEquals(metadata.keys().size(), 0);
            String key1 = "//./key-1'§!#$%&/()=?";
            String key2 = ".";
            String key3 = "..";
            metadata.put(key1, "Value-1".getBytes(StandardCharsets.UTF_8));
            metadata.put(key2, "Value-2".getBytes(StandardCharsets.UTF_8));
            metadata.put(key3, "Value-3".getBytes(StandardCharsets.UTF_8));
            assertEquals(metadata.keys().size(), 3);
            assertEquals(new String(metadata.get(key1), StandardCharsets.UTF_8), "Value-1");
            assertEquals(new String(metadata.get(key2), StandardCharsets.UTF_8), "Value-2");
            assertEquals(new String(metadata.get(key3), StandardCharsets.UTF_8), "Value-3");
            metadata.put(key2, "Overwritten-Value-2".getBytes(StandardCharsets.UTF_8));
            assertEquals(metadata.keys().size(), 3);
            assertEquals(new String(metadata.get(key2), StandardCharsets.UTF_8), "Overwritten-Value-2");
            metadata.remove(key3);
            assertEquals(metadata.keys().size(), 2);
        }

        long timestampBeforeA;
        long timestampBeforeB;
        long timestampBeforeC;
        long timestampBeforeD;
        long timestampAfterD;
        try (MessiProducer producer = client.producer("the-topic")) {
            timestampBeforeA = System.currentTimeMillis();
            producer.publish(MessiMessage.newBuilder().setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[1000])).putData("payload2", ByteString.copyFrom(new byte[500])).build());
            Thread.sleep(5);
            timestampBeforeB = System.currentTimeMillis();
            producer.publish(MessiMessage.newBuilder().setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[400])).putData("payload2", ByteString.copyFrom(new byte[700])).build());
            Thread.sleep(5);
            timestampBeforeC = System.currentTimeMillis();
            producer.publish(MessiMessage.newBuilder().setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[700])).putData("payload2", ByteString.copyFrom(new byte[70])).build());
            Thread.sleep(5);
            timestampBeforeD = System.currentTimeMillis();
            producer.publish(MessiMessage.newBuilder().setExternalId("d").putData("payload1", ByteString.copyFrom(new byte[8050])).putData("payload2", ByteString.copyFrom(new byte[130])).build());
            Thread.sleep(5);
            timestampAfterD = System.currentTimeMillis();
        }

        MessiMessage lastMessage = client.lastMessage("the-topic");
        assertNotNull(lastMessage);
        assertEquals(lastMessage.getExternalId(), "d");

        try (MessiConsumer consumer = client.consumer("the-topic")) {
            assertEquals(consumer.receive(1, TimeUnit.SECONDS).getExternalId(), "a");
            assertEquals(consumer.receive(1, TimeUnit.SECONDS).getExternalId(), "b");
            assertEquals(consumer.receive(1, TimeUnit.SECONDS).getExternalId(), "c");
            assertEquals(consumer.receive(1, TimeUnit.SECONDS).getExternalId(), "d");
        }

        try (MessiConsumer consumer = client.consumer("the-topic")) {
            consumer.seek(timestampAfterD);
            assertNull(consumer.receive(100, TimeUnit.MILLISECONDS));
            consumer.seek(timestampBeforeD);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).getExternalId(), "d");
            consumer.seek(timestampBeforeB);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).getExternalId(), "b");
            consumer.seek(timestampBeforeC);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).getExternalId(), "c");
            consumer.seek(timestampBeforeA);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).getExternalId(), "a");
        }

        {
            MessiMetadataClient metadata = client.metadata("the-topic");
            assertEquals(metadata.keys().size(), 2);
            String key = "uh9458goqkadl";
            metadata.put(key, "Value-blah-blah".getBytes(StandardCharsets.UTF_8));
            assertEquals(metadata.keys().size(), 3);
        }
    }

    @Test
    public void thatMetadataCanBeWrittenListedAndRead() {
        MessiMetadataClient metadata = client.metadata("the-topic");
        assertEquals(metadata.topic(), "the-topic");
        assertEquals(metadata.keys().size(), 0);
        String key1 = "//./key-1'§!#$%&/()=?";
        String key2 = ".";
        String key3 = "..";
        metadata.put(key1, "Value-1".getBytes(StandardCharsets.UTF_8));
        metadata.put(key2, "Value-2".getBytes(StandardCharsets.UTF_8));
        metadata.put(key3, "Value-3".getBytes(StandardCharsets.UTF_8));
        assertEquals(metadata.keys().size(), 3);
        assertEquals(new String(metadata.get(key1), StandardCharsets.UTF_8), "Value-1");
        assertEquals(new String(metadata.get(key2), StandardCharsets.UTF_8), "Value-2");
        assertEquals(new String(metadata.get(key3), StandardCharsets.UTF_8), "Value-3");
        metadata.put(key2, "Overwritten-Value-2".getBytes(StandardCharsets.UTF_8));
        assertEquals(metadata.keys().size(), 3);
        assertEquals(new String(metadata.get(key2), StandardCharsets.UTF_8), "Overwritten-Value-2");
        metadata.remove(key3);
        assertEquals(metadata.keys().size(), 2);
    }
}