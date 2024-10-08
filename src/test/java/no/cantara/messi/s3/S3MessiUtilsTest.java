package no.cantara.messi.s3;

import org.testng.Assert;
import org.testng.annotations.Test;

public class S3MessiUtilsTest {

    @Test
    public void testFilename() {
        String key = "abc/123/something/20210421054707/20210421054707/2021-04-21T05:47:10.694Z_100_343_1.avro";
        String filename = S3MessiUtils.filename(key);
        Assert.assertEquals(filename, "2021-04-21T05:47:10.694Z_100_343_1.avro");
    }

    @Test
    public void testTopic() {
        String key = "abc/123/something/20210421054707/20210421054707/2021-04-21T05:47:10.694Z_100_343_1.avro";
        String topic = S3MessiUtils.topic(key);
        Assert.assertEquals(topic, "abc/123/something/20210421054707/20210421054707");
    }

    @Test
    public void testGetFromTimestamp() {
        String key = "abc/123/something/20210421054707/20210421054707/2021-04-21T05:47:10.694Z_100_343_1.avro";
        long timestamp = S3MessiUtils.getFromTimestamp(key);
        Assert.assertEquals(timestamp, 1618984030694L);
    }

    @Test
    public void testGetMessageCount() {
        String key = "abc/123/something/20210421054707/20210421054707/2021-04-21T05:47:10.694Z_100_343_1.avro";
        long count = S3MessiUtils.getMessageCount(key);
        Assert.assertEquals(count, 100);
    }

    @Test
    public void testGetOffsetOfLastBlock() {
        String key = "abc/123/something/20210421054707/20210421054707/2021-04-21T05:47:10.694Z_100_343_1.avro";
        long offset = S3MessiUtils.getOffsetOfLastBlock(key);
        Assert.assertEquals(offset, 343);
    }
}