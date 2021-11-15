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

import io.airlift.units.DataSize;
import io.trino.plugin.hive.ConfigurationInitializer;
import io.trino.plugin.hive.s3.TrinoS3FileSystem;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

import java.io.File;

import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_ACCESS_KEY;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_SECRET_KEY;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_SSE_ENABLED;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_SSL_ENABLED;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_STAGING_DIRECTORY;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_STREAMING_UPLOAD_ENABLED;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_STREAMING_UPLOAD_PART_SIZE;

public class ExchangeS3ConfigurationInitializer
        implements ConfigurationInitializer
{
    private final String awsAccessKey;
    private final String awsSecretKey;
    private final boolean sslEnabled;
    private final boolean sseEnabled;
    private final File stagingDirectory;
    private final Boolean s3StreamingUploadEnabled;
    private final DataSize streamingPartSize;

    @Inject
    public ExchangeS3ConfigurationInitializer(ExchangeS3Config config)
    {
        this.awsAccessKey = config.getS3AwsAccessKey();
        this.awsSecretKey = config.getS3AwsSecretKey();
        this.sslEnabled = config.isS3SslEnabled();
        this.sseEnabled = config.isS3SseEnabled();
        this.stagingDirectory = config.getS3StagingDirectory();
        this.s3StreamingUploadEnabled = config.isS3StreamingUploadEnabled();
        this.streamingPartSize = config.getS3StreamingPartSize();
    }

    @Override
    public void initializeConfiguration(Configuration config)
    {
        config.set("fs.s3.impl", TrinoS3FileSystem.class.getName());
        config.set("fs.s3a.impl", TrinoS3FileSystem.class.getName());
        config.set("fs.s3n.impl", TrinoS3FileSystem.class.getName());

        if (awsAccessKey != null) {
            config.set(S3_ACCESS_KEY, awsAccessKey);
        }
        if (awsSecretKey != null) {
            config.set(S3_SECRET_KEY, awsSecretKey);
        }
        config.setBoolean(S3_SSL_ENABLED, sslEnabled);
        config.setBoolean(S3_SSE_ENABLED, sseEnabled);
        config.set(S3_STAGING_DIRECTORY, stagingDirectory.getPath());
        config.setBoolean(S3_STREAMING_UPLOAD_ENABLED, s3StreamingUploadEnabled);
        config.setLong(S3_STREAMING_UPLOAD_PART_SIZE, streamingPartSize.toBytes());
    }
}
