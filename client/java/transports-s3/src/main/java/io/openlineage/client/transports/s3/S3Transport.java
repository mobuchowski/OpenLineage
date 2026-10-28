/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.s3;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.transports.Transport;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.utils.AttributeMap;

@Slf4j
public class S3Transport extends Transport {
  private final S3Client s3Client;
  private final S3TransportConfig config;

  public S3Transport(S3TransportConfig config) {
    this.config = Objects.requireNonNull(config, "S3TransportConfig must not be null");
    this.s3Client = buildS3Client(config);
  }

  private static S3Client buildS3Client(S3TransportConfig config) {
    S3ClientBuilder builder = S3Client.builder();
    if (config.getEndpoint() != null) {
      builder.endpointOverride(URI.create(config.getEndpoint()));
    }
    return builder
        .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
        .httpClient(
            UrlConnectionHttpClient.builder()
                .buildWithDefaults(
                    AttributeMap.builder()
                        .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
                        .build()))
        .build();
  }

  @Override
  public void emit(OpenLineage.@NonNull RunEvent runEvent) {
    emitEvent(runEvent);
  }

  @Override
  public void emit(OpenLineage.@NonNull DatasetEvent datasetEvent) {
    emitEvent(datasetEvent);
  }

  @Override
  public void emit(OpenLineage.@NonNull JobEvent jobEvent) {
    emitEvent(jobEvent);
  }

  private <T extends OpenLineage.BaseEvent> void emitEvent(T event) {
    // eventTime alone is not unique: several events can share the same millisecond, so a hash of
    // the event content is appended. Only identical events share a name. Names still sort by
    // eventTime.
    byte[] content = OpenLineageClientUtils.toJson(event).getBytes(StandardCharsets.UTF_8);
    String baseName =
        String.format(
            "%d-%s.json", event.getEventTime().toInstant().toEpochMilli(), contentHash(content));
    uploadObject(getFileName(baseName), content);
  }

  /** Returns the first 64 bits of the SHA-256 of {@code content} as 16 hex characters. */
  private static String contentHash(byte[] content) {
    try {
      byte[] digest = MessageDigest.getInstance("SHA-256").digest(content);
      return String.format("%016x", ByteBuffer.wrap(digest).getLong());
    } catch (NoSuchAlgorithmException e) {
      throw new OpenLineageClientException(e);
    }
  }

  private String getFileName(String baseName) {
    String prefix = config.getFileNamePrefix();
    if (prefix == null) {
      return baseName;
    }
    return prefix.endsWith("/")
        ? String.format("%s%s", prefix, baseName)
        : String.format("%s_%s", prefix, baseName);
  }

  // No existence check: the key depends on the event content, so an existing key holds the same
  // bytes and overwriting it loses nothing.
  private void uploadObject(String objectName, byte[] content) {
    String bucketName = config.getBucketName();
    log.debug("Attempting to upload event to bucket: {} with key: {}", bucketName, objectName);
    s3Client.putObject(
        PutObjectRequest.builder().bucket(bucketName).key(objectName).build(),
        RequestBody.fromBytes(content));
    log.debug("Stored event: {}/{}", bucketName, objectName);
  }

  public void close() {
    s3Client.close();
  }

  // Visible for testing
  S3Client getS3Client() {
    return s3Client;
  }
}
