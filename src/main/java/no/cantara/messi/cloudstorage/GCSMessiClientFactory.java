package no.cantara.messi.cloudstorage;

import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import no.cantara.config.ApplicationProperties;
import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiClientFactory;
import no.cantara.messi.avro.AvroMessiUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

public class GCSMessiClientFactory implements MessiClientFactory {

    static Storage getWritableStorage(GoogleCredentials sourceCredentials) {
        GoogleCredentials scopedCredentials = sourceCredentials.createScoped(Arrays.asList("https://www.googleapis.com/auth/devstorage.read_write"));
        Storage storage = StorageOptions.newBuilder().setCredentials(scopedCredentials).build().getService();
        return storage;
    }

    static Storage getReadOnlyStorage(GoogleCredentials sourceCredentials) {
        GoogleCredentials scopedCredentials = sourceCredentials.createScoped(Arrays.asList("https://www.googleapis.com/auth/devstorage.read_only"));
        Storage storage = StorageOptions.newBuilder().setCredentials(scopedCredentials).build().getService();
        return storage;
    }

    @Override
    public Class<?> providerClass() {
        return GCSMessiClient.class;
    }

    @Override
    public String alias() {
        return "gcs";
    }

    public String providerTechnology() {
        return alias();
    }

    @Override
    public MessiClient create(ApplicationProperties configuration) {
        String bucket = configuration.get("gcs.bucket-name");
        if (bucket == null) {
            throw new IllegalArgumentException("Missing configuration property: gcs.bucket-name");
        }

        String localTempFolderConfig = configuration.get("local-temp-folder");
        if (localTempFolderConfig == null) {
            throw new IllegalArgumentException("Missing configuration property: local-temp-folder");
        }
        Path localTempFolder = Paths.get(localTempFolderConfig);

        String avroFileMaxSecondsConfig = configuration.get("avro-file.max.seconds");
        if (avroFileMaxSecondsConfig == null) {
            throw new IllegalArgumentException("Missing configuration property: avro-file.max.seconds");
        }
        long avroMaxSeconds = Long.parseLong(avroFileMaxSecondsConfig);

        String avroFileMaxBytesConfig = configuration.get("avro-file.max.bytes");
        if (avroFileMaxBytesConfig == null) {
            throw new IllegalArgumentException("Missing configuration property: avro-file.max.bytes");
        }
        long avroMaxBytes = Long.parseLong(avroFileMaxBytesConfig);

        String avroFileSyncIntervalConfig = configuration.get("avro-file.sync.interval");
        if (avroFileSyncIntervalConfig == null) {
            throw new IllegalArgumentException("Missing configuration property: avro-file.sync.interval");
        }
        int avroSyncInterval = Integer.parseInt(avroFileSyncIntervalConfig);

        String gcsListingMinIntervalSecondsConfig = configuration.get("gcs.listing.min-interval-seconds");
        if (gcsListingMinIntervalSecondsConfig == null) {
            throw new IllegalArgumentException("Missing configuration property: gcs.listing.min-interval-seconds");
        }
        int gcsFileListingMaxIntervalSeconds = Integer.parseInt(gcsListingMinIntervalSecondsConfig);

        String credentialProvider = configuration.get("gcs.credential-provider", "service-account");

        GoogleCredentials credentials;
        if ("service-account".equalsIgnoreCase(credentialProvider)) {
            try {
                String gcsServiceAccountKeyFileConfig = configuration.get("gcs.service-account.key-file");
                if (gcsServiceAccountKeyFileConfig == null) {
                    throw new IllegalArgumentException("Missing configuration property: gcs.service-account.key-file");
                }
                Path serviceAccountKeyPath = Paths.get(gcsServiceAccountKeyFileConfig);
                credentials = ServiceAccountCredentials.fromStream(Files.newInputStream(serviceAccountKeyPath, StandardOpenOption.READ));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if ("compute-engine".equalsIgnoreCase(credentialProvider)) {
            credentials = ComputeEngineCredentials.create();
        } else {
            throw new IllegalArgumentException("'gcs.credential-provider' must be one of 'service-account' or 'compute-engine'");
        }

        AvroMessiUtils readOnlyGcsMessiUtils = new GCSMessiUtils(getReadOnlyStorage(credentials), bucket);
        Storage writableStorage = getWritableStorage(credentials);
        AvroMessiUtils readWriteGcsMessiUtils = new GCSMessiUtils(writableStorage, bucket);
        return new GCSMessiClient(localTempFolder, avroMaxSeconds, avroMaxBytes, avroSyncInterval, gcsFileListingMaxIntervalSeconds, readOnlyGcsMessiUtils, readWriteGcsMessiUtils, providerTechnology(), writableStorage, bucket);
    }
}
