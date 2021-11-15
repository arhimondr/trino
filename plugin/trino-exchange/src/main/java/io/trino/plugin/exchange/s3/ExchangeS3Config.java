/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.exchange.s3;

import com.google.common.base.StandardSystemProperty;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.DataSize;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;

import javax.validation.constraints.NotNull;

import java.io.File;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class ExchangeS3Config
{
    private String s3AwsAccessKey;
    private String s3AwsSecretKey;
    private boolean s3SslEnabled = true;
    private boolean s3SseEnabled = true;
    private File s3StagingDirectory = new File(StandardSystemProperty.JAVA_IO_TMPDIR.value());
    private boolean s3StreamingUploadEnabled = true;
    private DataSize s3StreamingPartSize = DataSize.of(5, MEGABYTE);

    public String getS3AwsAccessKey()
    {
        return s3AwsAccessKey;
    }

    @Config("exchange.s3.aws-access-key")
    public ExchangeS3Config setS3AwsAccessKey(String s3AwsAccessKey)
    {
        this.s3AwsAccessKey = s3AwsAccessKey;
        return this;
    }

    public String getS3AwsSecretKey()
    {
        return s3AwsSecretKey;
    }

    @Config("exchange.s3.aws-secret-key")
    @ConfigSecuritySensitive
    public ExchangeS3Config setS3AwsSecretKey(String s3AwsSecretKey)
    {
        this.s3AwsSecretKey = s3AwsSecretKey;
        return this;
    }

    public boolean isS3SslEnabled()
    {
        return s3SslEnabled;
    }

    @Config("exchange.s3.ssl.enabled")
    public ExchangeS3Config setS3SslEnabled(boolean s3SslEnabled)
    {
        this.s3SslEnabled = s3SslEnabled;
        return this;
    }

    public boolean isS3SseEnabled()
    {
        return s3SseEnabled;
    }

    @Config("exchange.s3.sse.enabled")
    @ConfigDescription("Enable S3 server side encryption")
    public ExchangeS3Config setS3SseEnabled(boolean s3SseEnabled)
    {
        this.s3SseEnabled = s3SseEnabled;
        return this;
    }

    @NotNull
    @FileExists
    public File getS3StagingDirectory()
    {
        return s3StagingDirectory;
    }

    @Config("exchange.s3.staging-directory")
    @ConfigDescription("Temporary directory for staging files before uploading to S3")
    public ExchangeS3Config setS3StagingDirectory(File s3StagingDirectory)
    {
        this.s3StagingDirectory = s3StagingDirectory;
        return this;
    }

    public boolean isS3StreamingUploadEnabled()
    {
        return s3StreamingUploadEnabled;
    }

    @Config("exchange.s3.streaming.enabled")
    public ExchangeS3Config setS3StreamingUploadEnabled(boolean s3StreamingUploadEnabled)
    {
        this.s3StreamingUploadEnabled = s3StreamingUploadEnabled;
        return this;
    }

    @NotNull
    @MinDataSize("5MB")
    @MaxDataSize("16MB")
    public DataSize getS3StreamingPartSize()
    {
        return s3StreamingPartSize;
    }

    @Config("exchange.s3.streaming.part-size")
    @ConfigDescription("Part size for S3 streaming upload")
    public ExchangeS3Config setS3StreamingPartSize(DataSize s3StreamingPartSize)
    {
        this.s3StreamingPartSize = s3StreamingPartSize;
        return this;
    }
}
