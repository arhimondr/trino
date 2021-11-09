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

import io.trino.plugin.hive.s3.TrinoS3FileSystem;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeSink;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSource;
import io.trino.spi.exchange.ExchangeSourceHandle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_ACCESS_KEY;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_SECRET_KEY;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_STREAMING_UPLOAD_ENABLED;

public class DistributedFileSystemExchangeManager
        implements ExchangeManager
{
    private final FileSystem fileSystem;

    @Inject
    public DistributedFileSystemExchangeManager(ExchangeConfig config)
    {
        this.fileSystem = new TrinoS3FileSystem();

        Configuration hadoopConf = new Configuration();
        hadoopConf.set(S3_ACCESS_KEY, config.getS3AwsAccessKey());
        hadoopConf.set(S3_SECRET_KEY, config.getS3AwsSecretKey());
        hadoopConf.set(S3_STREAMING_UPLOAD_ENABLED, "true");
        try {
            this.fileSystem.initialize(new URI(config.getS3Bucket()), hadoopConf);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Exchange create(ExchangeContext context, int outputPartitionCount)
    {
        DistributedFileSystemExchange exchange = new DistributedFileSystemExchange(fileSystem, fileSystem.getWorkingDirectory(), context, outputPartitionCount);
        exchange.initialize();
        return exchange;
    }

    @Override
    public ExchangeSink createSink(ExchangeSinkInstanceHandle handle)
    {
        DistributedFileSystemExchangeSinkInstanceHandle distributedHandle = (DistributedFileSystemExchangeSinkInstanceHandle) handle;
        return new DistributedFileSystemExchangeSink(fileSystem, distributedHandle.getOutputDirectory(), distributedHandle.getOutputPartitionCount());
    }

    @Override
    public ExchangeSource createSource(List<ExchangeSourceHandle> handles)
    {
        List<Path> files = handles.stream()
                .map(DistributedFileSystemExchangeSourceHandle.class::cast)
                .flatMap(handle -> handle.getFiles().stream())
                .collect(toImmutableList());
        return new DistributedFileSystemExchangeSource(fileSystem, files);
    }
}
