package no.cantara.messi.cloudstorage;

import com.google.cloud.storage.Storage;
import no.cantara.messi.api.MessiMetadataClient;
import no.cantara.messi.avro.AvroMessiClient;
import no.cantara.messi.avro.AvroMessiTopic;
import no.cantara.messi.avro.AvroMessiUtils;

import java.nio.file.Path;

public class GCSMessiClient extends AvroMessiClient {

    final Storage storage;
    final String bucketName;

    public GCSMessiClient(Path tmpFileFolder, long avroMaxSeconds, long avroMaxBytes, int avroSyncInterval, int fileListingMinIntervalSeconds, AvroMessiUtils readOnlyAvroMessiUtils, AvroMessiUtils readWriteAvroMessiUtils, String providerTechnology, Storage storage, String bucketName) {
        super(tmpFileFolder, avroMaxSeconds, avroMaxBytes, avroSyncInterval, fileListingMinIntervalSeconds, readOnlyAvroMessiUtils, readWriteAvroMessiUtils, providerTechnology);
        this.storage = storage;
        this.bucketName = bucketName;
    }

    @Override
    public MessiMetadataClient metadata(String topic) {
        return new GCSMessiMetadataClient(storage, bucketName, topic);
    }

    @Override
    public AvroMessiTopic topicOf(String s) {
        return null;
    }
}
