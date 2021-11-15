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

import io.trino.plugin.hive.ConfigurationInitializer;
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
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class FileSystemExchangeManager
        implements ExchangeManager
{
    private final Path baseDirectory;
    private final FileSystem fileSystem;

    @Inject
    public FileSystemExchangeManager(ExchangeConfig exchangeConfig, Set<ConfigurationInitializer> configurationInitializers)
    {
        Configuration config = new Configuration(false);
        configurationInitializers.forEach(configurationInitializer -> configurationInitializer.initializeConfiguration(config));

        this.baseDirectory = new Path(exchangeConfig.getBaseDirectory());
        try {
            this.fileSystem = this.baseDirectory.getFileSystem(config);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Exchange create(ExchangeContext context, int outputPartitionCount)
    {
        FileSystemExchange exchange = new FileSystemExchange(fileSystem, baseDirectory, context, outputPartitionCount);
        exchange.initialize();
        return exchange;
    }

    @Override
    public ExchangeSink createSink(ExchangeSinkInstanceHandle handle)
    {
        FileSystemExchangeSinkInstanceHandle fileSystemExchangeSinkInstanceHandle = (FileSystemExchangeSinkInstanceHandle) handle;
        return new FileSystemExchangeSink(fileSystem, fileSystemExchangeSinkInstanceHandle.getOutputDirectory(), fileSystemExchangeSinkInstanceHandle.getOutputPartitionCount());
    }

    @Override
    public ExchangeSource createSource(List<ExchangeSourceHandle> handles)
    {
        List<Path> files = handles.stream()
                .map(FileSystemExchangeSourceHandle.class::cast)
                .flatMap(handle -> handle.getFiles().stream())
                .collect(toImmutableList());
        return new FileSystemExchangeSource(fileSystem, files);
    }
}
