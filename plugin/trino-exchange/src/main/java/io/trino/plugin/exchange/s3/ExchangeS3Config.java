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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class ExchangeS3Config
{
    private String s3AwsAccessKey;
    private String s3AwsSecretKey;
    private String s3Endpoint;
    private String s3IamRole;
    private String s3ExternalId;
    private int s3MaxErrorRetries = 10;
    private Duration s3ConnectTimeout = new Duration(5, TimeUnit.SECONDS);
    private Duration s3SocketTimeout = new Duration(5, TimeUnit.SECONDS);
    private int s3MaxConnections = 500;
    private boolean pinS3ClientToCurrentRegion;
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

    public String getS3Endpoint()
    {
        return s3Endpoint;
    }

    @Config("exchange.s3.endpoint")
    public ExchangeS3Config setS3Endpoint(String s3Endpoint)
    {
        this.s3Endpoint = s3Endpoint;
        return this;
    }

    public String getS3IamRole()
    {
        return s3IamRole;
    }

    @Config("exchange.s3.iam-role")
    @ConfigDescription("ARN of an IAM role to assume when connecting to S3")
    public ExchangeS3Config setS3IamRole(String s3IamRole)
    {
        this.s3IamRole = s3IamRole;
        return this;
    }

    public String getS3ExternalId()
    {
        return s3ExternalId;
    }

    @Config("exchange.s3.external-id")
    @ConfigDescription("External ID for the IAM role trust policy when connecting to S3")
    public ExchangeS3Config setS3ExternalId(String s3ExternalId)
    {
        this.s3ExternalId = s3ExternalId;
        return this;
    }

    @Min(0)
    public int getS3MaxErrorRetries()
    {
        return s3MaxErrorRetries;
    }

    @Config("exchange.s3.max-error-retries")
    public ExchangeS3Config setS3MaxErrorRetries(int s3MaxErrorRetries)
    {
        this.s3MaxErrorRetries = s3MaxErrorRetries;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getS3ConnectTimeout()
    {
        return s3ConnectTimeout;
    }

    @Config("exchange.s3.connect-timeout")
    public ExchangeS3Config setS3ConnectTimeout(Duration s3ConnectTimeout)
    {
        this.s3ConnectTimeout = s3ConnectTimeout;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getS3SocketTimeout()
    {
        return s3SocketTimeout;
    }

    @Config("exchange.s3.socket-timeout")
    public ExchangeS3Config setS3SocketTimeout(Duration s3SocketTimeout)
    {
        this.s3SocketTimeout = s3SocketTimeout;
        return this;
    }

    @Min(1)
    public int getS3MaxConnections()
    {
        return s3MaxConnections;
    }

    @Config("exchange.s3.max-connections")
    public ExchangeS3Config setS3MaxConnections(int s3MaxConnections)
    {
        this.s3MaxConnections = s3MaxConnections;
        return this;
    }

    public boolean isPinS3ClientToCurrentRegion()
    {
        return pinS3ClientToCurrentRegion;
    }

    @Config("exchange.s3.pin-client-to-current-region")
    @ConfigDescription("Should the S3 client be pinned to the current EC2 region")
    public ExchangeS3Config setPinS3ClientToCurrentRegion(boolean pinS3ClientToCurrentRegion)
    {
        this.pinS3ClientToCurrentRegion = pinS3ClientToCurrentRegion;
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
