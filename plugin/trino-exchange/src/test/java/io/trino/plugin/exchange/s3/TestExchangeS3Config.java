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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestExchangeS3Config
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ExchangeS3Config.class)
                .setS3AwsAccessKey(null)
                .setS3AwsSecretKey(null)
                .setS3Endpoint(null)
                .setS3IamRole(null)
                .setS3ExternalId(null)
                .setS3MaxErrorRetries(10)
                .setS3ConnectTimeout(new Duration(5, TimeUnit.SECONDS))
                .setS3SocketTimeout(new Duration(5, TimeUnit.SECONDS))
                .setS3MaxConnections(500)
                .setPinS3ClientToCurrentRegion(false)
                .setS3StreamingPartSize(DataSize.of(5, Unit.MEGABYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("exchange.s3.aws-access-key", "access")
                .put("exchange.s3.aws-secret-key", "secret")
                .put("exchange.s3.endpoint", "s3.us-west-1.amazonaws.com")
                .put("exchange.s3.iam-role", "roleArn")
                .put("exchange.s3.external-id", "externalId")
                .put("exchange.s3.max-error-retries", "8")
                .put("exchange.s3.connect-timeout", "8s")
                .put("exchange.s3.socket-timeout", "4m")
                .put("exchange.s3.max-connections", "77")
                .put("exchange.s3.pin-client-to-current-region", "true")
                .put("exchange.s3.streaming.part-size", "16MB")
                .build();

        ExchangeS3Config expected = new ExchangeS3Config()
                .setS3AwsAccessKey("access")
                .setS3AwsSecretKey("secret")
                .setS3Endpoint("s3.us-west-1.amazonaws.com")
                .setS3IamRole("roleArn")
                .setS3ExternalId("externalId")
                .setS3MaxErrorRetries(8)
                .setS3ConnectTimeout(new Duration(8, TimeUnit.SECONDS))
                .setS3SocketTimeout(new Duration(4, TimeUnit.MINUTES))
                .setS3MaxConnections(77)
                .setPinS3ClientToCurrentRegion(true)
                .setS3StreamingPartSize(DataSize.of(16, Unit.MEGABYTE));

        assertFullMapping(properties, expected);
    }
}
