module messi.provider.s3 {
    requires java.base;

    requires messi.sdk;
    requires messi.provider.filesystem;
    requires property.config;

    requires org.slf4j;
    requires org.apache.avro;

    requires software.amazon.awssdk.auth;
    requires software.amazon.awssdk.awscore;
    requires software.amazon.awssdk.core;
    requires software.amazon.awssdk.http;
    requires software.amazon.awssdk.regions;
    requires software.amazon.awssdk.services.s3;

    provides no.cantara.messi.api.MessiClientFactory with no.cantara.messi.s3.S3MessiClientFactory;

    opens no.cantara.messi.s3 to software.amazon.awssdk.core, org.apache.avro;

    exports no.cantara.messi.s3;
}
