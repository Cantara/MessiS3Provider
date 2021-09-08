package no.cantara.messi.cloudstorage;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import no.cantara.messi.avro.AvroFileMetadata;
import no.cantara.messi.avro.MessiAvroFile;

class GCSAvroFileMetadata extends AvroFileMetadata {

    final Storage storage;
    final String bucket;

    GCSAvroFileMetadata(Storage storage, String bucket) {
        this.storage = storage;
        this.bucket = bucket;
    }

    @Override
    public MessiAvroFile toMessiAvroFile(String topic) {
        return new GCSMessiAvroFile(storage, BlobId.of(bucket, topic + "/" + toFilename()));
    }
}
