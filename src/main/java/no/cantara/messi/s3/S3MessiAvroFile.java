package no.cantara.messi.s3;

import no.cantara.messi.avro.MessiAvroFile;
import org.apache.avro.file.SeekableInput;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.file.Path;

class S3MessiAvroFile implements MessiAvroFile {

    private final S3Client s3Client;
    private final String bucket;
    private final String key;

    S3MessiAvroFile(S3Client s3Client, String bucket, String key) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.key = key;
    }

    @Override
    public SeekableInput seekableInput() {
        return new S3SeekableInput(s3Client, bucket, key);
    }

    @Override
    public long getOffsetOfLastBlock() {
        return S3MessiUtils.getOffsetOfLastBlock(key);
    }

    @Override
    public void copyFrom(Path sourcePath) {
        s3Client.putObject(PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .contentType("text/plain")
                        .build(),
                RequestBody.fromFile(sourcePath));
    }

    @Override
    public String toString() {
        return "S3MessiAvroFile{" +
                "bucket='" + bucket + '\'' +
                ", key='" + key + '\'' +
                '}';
    }
}