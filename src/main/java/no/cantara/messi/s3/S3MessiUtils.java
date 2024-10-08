package no.cantara.messi.s3;

import no.cantara.messi.avro.AvroFileMetadata;
import no.cantara.messi.avro.AvroMessiUtils;
import no.cantara.messi.avro.MessiAvroFile;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

class S3MessiUtils implements AvroMessiUtils {

    final S3Client s3Client;
    final String bucket;

    S3MessiUtils(S3Client s3Client, String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    static final Pattern topicAndFilenamePattern = Pattern.compile("(?<topic>.+)/(?<filename>[^/]+)");

    static final Pattern topicAndMetadataFilenamePattern = Pattern.compile("(?<topic>.+)/metadata/(?<filename>[^/]+)");

    static Matcher topicMatcherOf(String key) {
        Matcher topicAndFilenameMatcher = topicAndFilenamePattern.matcher(key);
        if (!topicAndFilenameMatcher.matches()) {
            throw new RuntimeException("S3 key does not match topicAndFilenamePattern. key=" + key);
        }
        return topicAndFilenameMatcher;
    }

    static String topic(String key) {
        Matcher topicAndFilenameMatcher = topicMatcherOf(key);
        return topicAndFilenameMatcher.group("topic");
    }

    static String filename(String key) {
        Matcher topicAndFilenameMatcher = topicMatcherOf(key);
        return topicAndFilenameMatcher.group("filename");
    }

    static final Pattern filenamePattern = Pattern.compile("(?<from>[^_]+)_(?<count>[0123456789]+)_(?<lastBlockOffset>[0123456789]+)_(?<position>.+)\\.avro");

    static Matcher filenameMatcherOf(String key) {
        String filename = filename(key);
        Matcher filenameMatcher = filenamePattern.matcher(filename);
        if (!filenameMatcher.matches()) {
            throw new RuntimeException("S3 filename does not match filenamePattern. filename=" + filename);
        }
        return filenameMatcher;
    }

    /**
     * @return lower-bound (inclusive) timestamp of this file range
     */
    static long getFromTimestamp(String key) {
        Matcher filenameMatcher = filenameMatcherOf(key);
        String from = filenameMatcher.group("from");
        return AvroMessiUtils.parseTimestamp(from);
    }

    /**
     * @return lower-bound (inclusive) position of this file range
     */
    String getFirstPosition(String key) {
        Matcher filenameMatcher = filenameMatcherOf(key);
        return filenameMatcher.group("position");
    }

    /**
     * @return count of messages in the file
     */
    static long getMessageCount(String key) {
        Matcher filenameMatcher = filenameMatcherOf(key);
        return Long.parseLong(filenameMatcher.group("count"));
    }

    /**
     * @return count of messages in the file
     */
    static long getOffsetOfLastBlock(String key) {
        Matcher filenameMatcher = filenameMatcherOf(key);
        return Long.parseLong(filenameMatcher.group("lastBlockOffset"));
    }

    Stream<S3Object> listTopicFiles(String bucketName, String topic) {
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(topic + "/")
                .build();

        ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);

        return listResponse.contents().stream()
                .filter(s3Object -> s3Object.size() > 0)
                .filter(s3Object -> !topicAndMetadataFilenamePattern.matcher(s3Object.key()).matches());
    }

    @Override
    public NavigableMap<Long, MessiAvroFile> getTopicBlobs(String topic) {
        NavigableMap<Long, MessiAvroFile> map = new TreeMap<>();
        listTopicFiles(bucket, topic).forEach(s3Object -> {
            long fromTimestamp = getFromTimestamp(s3Object.key());
            map.put(fromTimestamp, new S3MessiAvroFile(s3Client, bucket, s3Object.key()));
        });
        return map;
    }

    @Override
    public AvroFileMetadata newAvrofileMetadata() {
        return new S3AvroFileMetadata(s3Client, bucket);
    }
}