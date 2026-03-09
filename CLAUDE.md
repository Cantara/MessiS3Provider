# MessiS3Provider

## Purpose
AWS S3 provider implementation for the Messi messaging and streaming abstraction layer. Enables applications using the Messi SDK to store and retrieve messages via Amazon S3.

## Tech Stack
- Language: Java 11+
- Framework: None (library)
- Build: Maven
- Key dependencies: Messi SDK, AWS S3 SDK

## Architecture
Provider library implementing the Messi SDK interfaces for Amazon S3. Uses the provider pattern (Java SPI) so applications can use S3 as a message/event storage backend. Suitable for batch processing, archival, and scenarios where stream ordering is less critical than durability.

## Key Entry Points
- Messi S3 provider implementation classes
- `pom.xml` - Maven coordinates: `no.cantara.messi:messi-s3-provider`

## Development
```bash
# Build
mvn clean install

# Test
mvn test
```

## Domain Context
Cloud messaging infrastructure. Part of the Messi messaging abstraction ecosystem, providing AWS S3 as a storage backend for event archival and batch message processing.
