package no.cantara.messi.s3;

import no.cantara.messi.api.MessiMetadataClient;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class S3MessiMetadataClient implements MessiMetadataClient {

    final S3Client s3Client;
    final String bucketName;
    final String topic;

    public S3MessiMetadataClient(S3Client s3Client, String bucketName, String topic) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.topic = topic;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public Set<String> keys() {
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(topic + "/metadata/")
                .build();

        ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);

        return listResponse.contents().stream()
                .map(S3Object::key)
                .map(key -> key.substring(key.lastIndexOf('/') + 1))
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
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(path)
                .build();

        return s3Client.getObjectAsBytes(getObjectRequest).asByteArray();
    }

    @Override
    public MessiMetadataClient put(String key, byte[] value) {
        String path = topic + "/metadata/" + escapeFilename(key);
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(path)
                .build();

        s3Client.putObject(putObjectRequest, RequestBody.fromBytes(value));
        return this;
    }

    @Override
    public MessiMetadataClient remove(String key) {
        String path = topic + "/metadata/" + escapeFilename(key);
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(path)
                .build();

        s3Client.deleteObject(deleteObjectRequest);
        return this;
    }
}