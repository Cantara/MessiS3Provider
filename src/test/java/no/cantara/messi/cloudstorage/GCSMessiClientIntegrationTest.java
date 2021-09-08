package no.cantara.messi.cloudstorage;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import no.cantara.config.ApplicationProperties;
import no.cantara.config.ProviderLoader;
import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiClientFactory;
import no.cantara.messi.api.MessiConsumer;
import no.cantara.messi.api.MessiMessage;
import no.cantara.messi.api.MessiMetadataClient;
import no.cantara.messi.api.MessiProducer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 * Running these tests requires an accessible google-cloud-storage bucket with read and write object access.
 * <p>
 * Requirements: A google cloud service-account key with access to read and write objects in cloud-storage.
 * This can be put as a file at the path "secret/gcs_sa_test.json"
 */
@Ignore
public class GCSMessiClientIntegrationTest {

    MessiClient client;

    @BeforeMethod
    public void createClient() throws IOException {
        ApplicationProperties.Builder applicationPropertiesBuilder = ApplicationProperties.builder()
                .values()
                .put("local-temp-folder", "target/_tmp_avro_")
                .put("avro-file.max.seconds", "3")
                .put("avro-file.max.bytes", Long.toString(2 * 1024)) // 2 KiB
                .put("avro-file.sync.interval", Long.toString(200))
                .put("gcs.bucket-name", "kcg-experimental-bucket")
                .put("gcs.listing.min-interval-seconds", Long.toString(3))
                .put("gcs.credential-provider", "service-account")
                .put("gcs.service-account.key-file", "secret/gcs_test_sa.json")
                .end();

        String messiGcsBucket = System.getenv("MESSI_GCS_BUCKET");
        if (messiGcsBucket != null) {
            applicationPropertiesBuilder.property("gcs.bucket-name", messiGcsBucket);
        }

        String messiGcsSaKeyFile = System.getenv("MESSI_GCS_SERVICE_ACCOUNT_KEY_FILE");
        if (messiGcsSaKeyFile != null) {
            applicationPropertiesBuilder.property("gcs.service-account.key-file", messiGcsSaKeyFile);
        }

        ApplicationProperties configuration = applicationPropertiesBuilder.build();

        Path localTempFolder = Paths.get(configuration.get("local-temp-folder"));
        if (Files.exists(localTempFolder)) {
            Files.walk(localTempFolder).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
        Files.createDirectories(localTempFolder);
        client = ProviderLoader.configure(configuration, "gcs", MessiClientFactory.class);

        // clear bucket
        String bucket = configuration.get("gcs.bucket-name");


        ServiceAccountCredentials credentials = ServiceAccountCredentials.fromStream(Files.newInputStream(Paths.get(configuration.get("gcs.service-account.key-file")), StandardOpenOption.READ));
        Storage storage = GCSMessiClientFactory.getWritableStorage(credentials);
        Page<Blob> page = storage.list(bucket, Storage.BlobListOption.prefix("the-topic"));
        BlobId[] blobs = StreamSupport.stream(page.iterateAll().spliterator(), false).map(BlobInfo::getBlobId).collect(Collectors.toList()).toArray(new BlobId[0]);
        if (blobs.length > 0) {
            List<Boolean> deletedList = storage.delete(blobs);
            for (Boolean deleted : deletedList) {
                if (!deleted) {
                    throw new RuntimeException("Unable to delete blob in bucket");
                }
            }
        }
    }

    @AfterMethod
    public void closeClient() {
        client.close();
    }

    @Test
    public void thatMostFunctionsWorkWhenIntegratedWithGCS() throws Exception {
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
            producer.publish(MessiMessage.builder().externalId("a").put("payload1", new byte[1000]).put("payload2", new byte[500]).build());
            Thread.sleep(5);
            timestampBeforeB = System.currentTimeMillis();
            producer.publish(MessiMessage.builder().externalId("b").put("payload1", new byte[400]).put("payload2", new byte[700]).build());
            Thread.sleep(5);
            timestampBeforeC = System.currentTimeMillis();
            producer.publish(MessiMessage.builder().externalId("c").put("payload1", new byte[700]).put("payload2", new byte[70]).build());
            Thread.sleep(5);
            timestampBeforeD = System.currentTimeMillis();
            producer.publish(MessiMessage.builder().externalId("d").put("payload1", new byte[8050]).put("payload2", new byte[130]).build());
            Thread.sleep(5);
            timestampAfterD = System.currentTimeMillis();
        }

        MessiMessage lastMessage = client.lastMessage("the-topic");
        assertNotNull(lastMessage);
        assertEquals(lastMessage.externalId(), "d");

        try (MessiConsumer consumer = client.consumer("the-topic")) {
            assertEquals(consumer.receive(1, TimeUnit.SECONDS).externalId(), "a");
            assertEquals(consumer.receive(1, TimeUnit.SECONDS).externalId(), "b");
            assertEquals(consumer.receive(1, TimeUnit.SECONDS).externalId(), "c");
            assertEquals(consumer.receive(1, TimeUnit.SECONDS).externalId(), "d");
        }

        try (MessiConsumer consumer = client.consumer("the-topic")) {
            consumer.seek(timestampAfterD);
            assertNull(consumer.receive(100, TimeUnit.MILLISECONDS));
            consumer.seek(timestampBeforeD);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).externalId(), "d");
            consumer.seek(timestampBeforeB);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).externalId(), "b");
            consumer.seek(timestampBeforeC);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).externalId(), "c");
            consumer.seek(timestampBeforeA);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).externalId(), "a");
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
