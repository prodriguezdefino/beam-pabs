/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.aws2.common.providers;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.Serializable;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Suppliers;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleWithWebIdentityCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityRequest;
import software.amazon.awssdk.utils.SdkAutoCloseable;

public class StsAssumeRoleWithDynamicWebIdentityCredentialsProvider
    implements AwsCredentialsProvider, SdkAutoCloseable, Serializable {

  @Nullable
  private transient Supplier<StsAssumeRoleWithWebIdentityCredentialsProvider> delegate = null;

  private final String audience;
  private final String assumedRoleArn;
  private final String webIdTokenProviderFQCN;
  @Nullable private final Integer sessionDurationSecs;

  private StsAssumeRoleWithDynamicWebIdentityCredentialsProvider(
      String audience,
      String assumedRoleArn,
      String webIdTokenProviderFQCN,
      @Nullable Integer sessionDurationSecs) {
    this.audience = audience;
    this.assumedRoleArn = assumedRoleArn;
    this.webIdTokenProviderFQCN = webIdTokenProviderFQCN;
    this.sessionDurationSecs = sessionDurationSecs;
  }

  private Supplier<StsAssumeRoleWithWebIdentityCredentialsProvider> initializeDelegate() {
    delegate = Suppliers.memoize(this::createDelegate);
    return delegate;
  }

  public String audience() {
    return audience;
  }

  public String assumedRoleArn() {
    return assumedRoleArn;
  }

  public String webIdTokenProviderFQCN() {
    return webIdTokenProviderFQCN;
  }

  @Nullable
  public Integer sessionDurationSecs() {
    return sessionDurationSecs;
  }

  public static StsAssumeRoleWithDynamicWebIdentityCredentialsProvider.Builder builder() {
    return new StsAssumeRoleWithDynamicWebIdentityCredentialsProvider.Builder();
  }

  @SuppressWarnings("initialization")
  public static final class Builder {

    private String audience;
    private String assumedRoleArn;
    private String webIdTokenProviderFQCN;
    @Nullable private Integer sessionDurationSecs = null;

    private Builder() {}

    public Builder setAssumedRoleArn(String roleArn) {
      this.assumedRoleArn = roleArn;
      return this;
    }

    public Builder setAudience(String audience) {
      this.audience = audience;
      return this;
    }

    public Builder setWebIdTokenProviderFQCN(String idTokenProviderFQCN) {
      this.webIdTokenProviderFQCN = idTokenProviderFQCN;
      return this;
    }

    public Builder setSessionDurationSecs(@Nullable Integer durationSecs) {
      this.sessionDurationSecs = durationSecs;
      return this;
    }

    public StsAssumeRoleWithDynamicWebIdentityCredentialsProvider build() {
      checkState(audience != null, "Audience value should not be null");
      checkState(assumedRoleArn != null, "The role to assume should not be null");
      checkState(
          webIdTokenProviderFQCN != null,
          "The web id token provider fully qualified class name should not be null");
      return new StsAssumeRoleWithDynamicWebIdentityCredentialsProvider(
          audience, assumedRoleArn, webIdTokenProviderFQCN, sessionDurationSecs);
    }
  }

  WebIdTokenProvider retrieveWebIdTokenProvider() {
    try {
      return InstanceBuilder.ofType(WebIdTokenProvider.class)
          .fromClassName(webIdTokenProviderFQCN())
          .build();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(
          "Problems while trying to instantiate a dynamic web id token provider class.", e);
    }
  }

  StsAssumeRoleWithWebIdentityCredentialsProvider createDelegate() {
    return StsAssumeRoleWithWebIdentityCredentialsProvider.builder()
        .stsClient(StsClient.builder().region(Region.AWS_GLOBAL).build())
        .asyncCredentialUpdateEnabled(true)
        .refreshRequest(
            () ->
                AssumeRoleWithWebIdentityRequest.builder()
                    .webIdentityToken(retrieveWebIdTokenProvider().resolveTokenValue(audience()))
                    .roleArn(assumedRoleArn())
                    .roleSessionName("apache-beam-auth-session-" + UUID.randomUUID())
                    .durationSeconds(Optional.ofNullable(sessionDurationSecs()).orElse(3600))
                    .build())
        .build();
  }

  @Override
  public AwsCredentials resolveCredentials() {
    return Optional.ofNullable(delegate).orElse(initializeDelegate()).get().resolveCredentials();
  }

  @Override
  public void close() {
    if (delegate != null) {
      delegate.get().close();
    }
  }
}
