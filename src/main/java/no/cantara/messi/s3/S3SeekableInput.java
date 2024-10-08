package no.cantara.messi.s3;

import org.apache.avro.file.SeekableInput;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;

class S3SeekableInput implements SeekableInput {

    private final S3Client s3Client;
    private final String bucket;
    private final String key;
    private final long size;
    private final AtomicLong position = new AtomicLong(0);
    private InputStream currentStream;

    S3SeekableInput(S3Client s3Client, String bucket, String key) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.key = key;
        this.size = s3Client.headObject(HeadObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build()).contentLength();
    }

    @Override
    public void seek(long p) throws IOException {
        if (p < 0 || p >= size) {
            throw new IOException("Invalid seek position: " + p);
        }
        position.set(p);
        refreshStream();
    }

    @Override
    public long tell() throws IOException {
        return position.get();
    }

    @Override
    public long length() throws IOException {
        return size;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (currentStream == null) {
            refreshStream();
        }
        int bytesRead = currentStream.read(b, off, len);
        if (bytesRead > 0) {
            position.addAndGet(bytesRead);
        }
        return bytesRead;
    }

    @Override
    public void close() throws IOException {
        if (currentStream != null) {
            currentStream.close();
        }
    }

    private void refreshStream() throws IOException {
        if (currentStream != null) {
            currentStream.close();
        }
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .range("bytes=" + position.get() + "-")
                .build();
        currentStream = s3Client.getObject(request, ResponseTransformer.toInputStream());
    }
}