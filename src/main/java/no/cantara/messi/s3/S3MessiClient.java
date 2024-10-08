package no.cantara.messi.s3;

import no.cantara.messi.api.MessiMetadataClient;
import no.cantara.messi.avro.AvroMessiClient;
import no.cantara.messi.avro.AvroMessiTopic;
import no.cantara.messi.avro.AvroMessiUtils;
import software.amazon.awssdk.services.s3.S3Client;

import java.nio.file.Path;

public class S3MessiClient extends AvroMessiClient {

    final S3Client s3Client;
    final String bucketName;

    public S3MessiClient(Path tmpFileFolder, long avroMaxSeconds, long avroMaxBytes, int avroSyncInterval, int fileListingMinIntervalSeconds, AvroMessiUtils readOnlyAvroMessiUtils, AvroMessiUtils readWriteAvroMessiUtils, String providerTechnology, S3Client s3Client, String bucketName) {
        super(tmpFileFolder, avroMaxSeconds, avroMaxBytes, avroSyncInterval, fileListingMinIntervalSeconds, readOnlyAvroMessiUtils, readWriteAvroMessiUtils, providerTechnology);
        this.s3Client = s3Client;
        this.bucketName = bucketName;
    }

    @Override
    public MessiMetadataClient metadata(String topic) {
        return new S3MessiMetadataClient(s3Client, bucketName, topic);
    }

    @Override
    public AvroMessiTopic topicOf(String s) {
        // Implement this method based on your requirements
        return null;
    }
}
