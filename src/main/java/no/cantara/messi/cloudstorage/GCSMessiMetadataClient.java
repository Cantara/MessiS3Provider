package no.cantara.messi.cloudstorage;

import com.google.api.gax.paging.Page;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import no.cantara.messi.api.MessiMetadataClient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class GCSMessiMetadataClient implements MessiMetadataClient {

    final Storage storage;
    final String bucketName;
    final String topic;

    public GCSMessiMetadataClient(Storage storage, String bucketName, String topic) {
        this.storage = storage;
        this.bucketName = bucketName;
        this.topic = topic;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public Set<String> keys() {
        Page<Blob> page = storage.list(bucketName, Storage.BlobListOption.prefix(topic + "/metadata/"));
        return StreamSupport.stream(page.iterateAll().spliterator(), false)
                .filter(blob -> !blob.isDirectory())
                .map(BlobInfo::getName)
                .map(name -> name.substring(1 + name.lastIndexOf('/')))
                .map(this::unescapeFilename)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    String escapeFilename(String filename) {
        try {
            return URLEncoder.encode(filename, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    String unescapeFilename(String filename) {
        try {
            return URLDecoder.decode(filename, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] get(String key) {
        String path = topic + "/metadata/" + escapeFilename(key);
        byte[] data = storage.readAllBytes(BlobId.of(bucketName, path));
        return data;
    }

    @Override
    public MessiMetadataClient put(String key, byte[] value) {
        String path = topic + "/metadata/" + escapeFilename(key);
        try (WriteChannel channel = storage.writer(BlobInfo.newBuilder(BlobId.of(bucketName, path)).build())) {
            channel.write(ByteBuffer.wrap(value));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    @Override
    public MessiMetadataClient remove(String key) {
        String path = topic + "/metadata/" + escapeFilename(key);
        storage.delete(BlobId.of(bucketName, path));
        return this;
    }
}
