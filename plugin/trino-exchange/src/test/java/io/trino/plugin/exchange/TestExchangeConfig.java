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
package io.trino.plugin.exchange;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestExchangeConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ExchangeConfig.class)
                .setS3AwsAccessKey(null)
                .setS3AwsSecretKey(null)
                .setS3Bucket(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("exchange.s3.aws-access-key", "access")
                .put("exchange.s3.aws-secret-key", "secret")
                .put("exchange.s3.bucket", "s3n://exchange-spooling-test/")
                .build();

        ExchangeConfig expected = new ExchangeConfig()
                .setS3AwsAccessKey("access")
                .setS3AwsSecretKey("secret")
                .setS3Bucket("s3n://exchange-spooling-test/");

        assertFullMapping(properties, expected);
    }
}
