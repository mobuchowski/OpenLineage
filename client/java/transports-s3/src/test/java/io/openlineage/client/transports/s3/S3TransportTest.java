/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.transports_s3.TestBuildConfig;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
@Testcontainers
class S3TransportTest {
  private static final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
  private static final String HASH_REGEX = "[0-9a-f]{16}";

  @Container
  private final S3MockContainer s3Mock = new S3MockContainer(TestBuildConfig.S3_MOCK_VERSION);

  @Test
  void shouldWriteEventToS3WithoutPrefix() {
    testEventStored("test-bucket-1", null, "");
  }

  @Test
  void shouldWriteEventToS3WithNamePrefix() {
    testEventStored("test-bucket-2", "event", "event_");
  }

  @Test
  void shouldWriteEventToS3WithPathAndNamePrefix() {
    testEventStored("test-bucket-3", "some/path/event", "some/path/event_");
  }

  @Test
  void shouldWriteEventToS3WithPathPrefix() {
    testEventStored("test-bucket-4", "another/path/", "another/path/");
  }

  @Test
  void shouldStoreAllEventsWithTheSameEventTime() {
    String bucketName = "test-bucket-5";
    OpenLineage.RunEvent start = runEvent(clock, OpenLineage.RunEvent.EventType.START);
    OpenLineage.RunEvent complete = runEvent(clock, OpenLineage.RunEvent.EventType.COMPLETE);
    try (S3Transport transport = transport(bucketName, "some/prefix/")) {
      emit(transport, start, complete);

      List<String> contents =
          listKeys(transport.getS3Client(), bucketName).stream()
              .map(key -> readObject(transport.getS3Client(), bucketName, key))
              .collect(Collectors.toList());
      assertEquals(2, contents.size());
      assertTrue(contents.contains(OpenLineageClientUtils.toJson(start)));
      assertTrue(contents.contains(OpenLineageClientUtils.toJson(complete)));
    }
  }

  @Test
  void shouldStoreIdenticalEventOnce() {
    String bucketName = "test-bucket-6";
    OpenLineage.RunEvent event = runEvent(clock);
    try (S3Transport transport = transport(bucketName, "some/prefix/")) {
      emit(transport, event, event);

      List<String> keys = listKeys(transport.getS3Client(), bucketName);
      assertEquals(1, keys.size());
      assertEquals(
          OpenLineageClientUtils.toJson(event),
          readObject(transport.getS3Client(), bucketName, keys.get(0)));
    }
  }

  /**
   * Emits one event to the given bucket, then checks that it is the only object, that its key is
   * {@code <expectedKeyPrefix><epochMillis>-<hash>.json}, and that it holds the event JSON.
   */
  private void testEventStored(
      String bucketName, @Nullable String prefix, String expectedKeyPrefix) {
    OpenLineage.RunEvent event = runEvent(clock);
    try (S3Transport transport = transport(bucketName, prefix)) {
      emit(transport, event);

      List<String> keys = listKeys(transport.getS3Client(), bucketName);
      assertEquals(1, keys.size());
      String key = keys.get(0);
      assertTrue(
          key.matches(
              Pattern.quote(expectedKeyPrefix) + epochMillis() + "-" + HASH_REGEX + "\\.json"),
          key);
      assertEquals(
          OpenLineageClientUtils.toJson(event),
          readObject(transport.getS3Client(), bucketName, key));
    }
  }

  private S3Transport transport(String bucketName, @Nullable String prefix) {
    mockAwsCredentials();
    S3Transport transport =
        new S3Transport(new S3TransportConfig(s3Mock.getHttpsEndpoint(), bucketName, prefix));
    ensureBucketExists(transport.getS3Client(), bucketName);
    return transport;
  }

  private static void emit(S3Transport transport, OpenLineage.RunEvent... events) {
    OpenLineageClient client = new OpenLineageClient(transport);
    for (OpenLineage.RunEvent event : events) {
      client.emit(event);
    }
  }

  private static List<String> listKeys(S3Client s3Client, String bucketName) {
    return s3Client
        .listObjectsV2Paginator(ListObjectsV2Request.builder().bucket(bucketName).build())
        .contents()
        .stream()
        .map(S3Object::key)
        .collect(Collectors.toList());
  }

  private static String readObject(S3Client s3Client, String bucketName, String key) {
    return new String(
        s3Client
            .getObjectAsBytes(GetObjectRequest.builder().bucket(bucketName).key(key).build())
            .asByteArray(),
        StandardCharsets.UTF_8);
  }

  private static long epochMillis() {
    return ZonedDateTime.now(clock).toInstant().toEpochMilli();
  }

  /**
   * The AWS credentials and region are not used, but the SDK still required them to satisfy
   * credentials provider chain.
   */
  private static void mockAwsCredentials() {
    System.setProperty("aws.region", "dummy");
    System.setProperty("aws.accessKeyId", "dummy");
    System.setProperty("aws.secretAccessKey", "dummy");
  }

  private static void ensureBucketExists(S3Client s3Client, String bucketName) {
    boolean bucketExists =
        s3Client.listBuckets().buckets().stream().anyMatch(b -> bucketName.equals(b.name()));
    if (!bucketExists) {
      s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
    }
  }

  public static OpenLineage.RunEvent runEvent(Clock clock) {
    return runEvent(clock, OpenLineage.RunEvent.EventType.START);
  }

  public static OpenLineage.RunEvent runEvent(
      Clock clock, OpenLineage.RunEvent.EventType eventType) {
    OpenLineage.Job job =
        new OpenLineage.JobBuilder().namespace("test-namespace").name("test-job").build();
    OpenLineage.Run run = new OpenLineage.RunBuilder().runId(UUID.randomUUID()).build();
    return new OpenLineage(URI.create("http://test.producer"))
        .newRunEventBuilder()
        .eventTime(ZonedDateTime.now(clock))
        .eventType(eventType)
        .job(job)
        .run(run)
        .build();
  }
}
