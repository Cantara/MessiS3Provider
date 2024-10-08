package no.cantara.messi.s3;

import no.cantara.config.ApplicationProperties;
import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiClientFactory;
import no.cantara.messi.avro.AvroMessiUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.nio.file.Path;
import java.nio.file.Paths;

public class S3MessiClientFactory implements MessiClientFactory {

    static S3Client getS3Client(ApplicationProperties configuration) {
        String region = configuration.get("s3.region");
        if (region == null) {
            throw new IllegalArgumentException("Missing configuration property: s3.region");
        }

        String credentialProvider = configuration.get("s3.credential-provider", "default");
        AwsCredentialsProvider awsCredentialsProvider;

        if ("static".equalsIgnoreCase(credentialProvider)) {
            String accessKey = configuration.get("s3.access-key-id");
            String secretKey = configuration.get("s3.secret-access-key");
            if (accessKey == null || secretKey == null) {
                throw new IllegalArgumentException("Missing configuration properties: s3.access-key-id or s3.secret-access-key");
            }
            awsCredentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
        } else if ("default".equalsIgnoreCase(credentialProvider)) {
            awsCredentialsProvider = DefaultCredentialsProvider.create();
        } else {
            throw new IllegalArgumentException("'s3.credential-provider' must be one of 'static' or 'default'");
        }

        return S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(awsCredentialsProvider)
                .build();
    }

    @Override
    public Class<?> providerClass() {
        return S3MessiClient.class;
    }

    @Override
    public String alias() {
        return "s3";
    }

    public String providerTechnology() {
        return alias();
    }

    @Override
    public MessiClient create(ApplicationProperties configuration) {
        String bucket = configuration.get("s3.bucket-name");
        if (bucket == null) {
            throw new IllegalArgumentException("Missing configuration property: s3.bucket-name");
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

        String s3ListingMinIntervalSecondsConfig = configuration.get("s3.listing.min-interval-seconds");
        if (s3ListingMinIntervalSecondsConfig == null) {
            throw new IllegalArgumentException("Missing configuration property: s3.listing.min-interval-seconds");
        }
        int s3FileListingMaxIntervalSeconds = Integer.parseInt(s3ListingMinIntervalSecondsConfig);

        S3Client s3Client = getS3Client(configuration);

        AvroMessiUtils readOnlyS3MessiUtils = new S3MessiUtils(s3Client, bucket);
        AvroMessiUtils readWriteS3MessiUtils = new S3MessiUtils(s3Client, bucket);

        return new S3MessiClient(localTempFolder, avroMaxSeconds, avroMaxBytes, avroSyncInterval, s3FileListingMaxIntervalSeconds, readOnlyS3MessiUtils, readWriteS3MessiUtils, providerTechnology(), s3Client, bucket);
    }
}