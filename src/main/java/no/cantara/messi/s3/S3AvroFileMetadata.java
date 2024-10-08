package no.cantara.messi.s3;

import no.cantara.messi.avro.AvroFileMetadata;
import no.cantara.messi.avro.MessiAvroFile;
import software.amazon.awssdk.services.s3.S3Client;

class S3AvroFileMetadata extends AvroFileMetadata {

    final S3Client s3Client;
    final String bucket;

    S3AvroFileMetadata(S3Client s3Client, String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    @Override
    public MessiAvroFile toMessiAvroFile(String topic) {
        return new S3MessiAvroFile(s3Client, bucket, topic + "/" + toFilename());
    }
}