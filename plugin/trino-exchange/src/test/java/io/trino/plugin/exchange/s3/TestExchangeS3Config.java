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
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

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
                .setS3SslEnabled(true)
                .setS3SseEnabled(true)
                .setS3StagingDirectory(new File(StandardSystemProperty.JAVA_IO_TMPDIR.value()))
                .setS3StreamingUploadEnabled(true)
                .setS3StreamingPartSize(DataSize.of(5, Unit.MEGABYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path stagingDirectory = Files.createTempDirectory(null);

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("exchange.s3.aws-access-key", "access")
                .put("exchange.s3.aws-secret-key", "secret")
                .put("exchange.s3.sse.enabled", "false")
                .put("exchange.s3.ssl.enabled", "false")
                .put("exchange.s3.staging-directory", stagingDirectory.toString())
                .put("exchange.s3.streaming.enabled", "false")
                .put("exchange.s3.streaming.part-size", "16MB")
                .build();

        ExchangeS3Config expected = new ExchangeS3Config()
                .setS3AwsAccessKey("access")
                .setS3AwsSecretKey("secret")
                .setS3SslEnabled(false)
                .setS3SseEnabled(false)
                .setS3StagingDirectory(stagingDirectory.toFile())
                .setS3StreamingUploadEnabled(false)
                .setS3StreamingPartSize(DataSize.of(16, Unit.MEGABYTE));

        assertFullMapping(properties, expected);
    }
}
