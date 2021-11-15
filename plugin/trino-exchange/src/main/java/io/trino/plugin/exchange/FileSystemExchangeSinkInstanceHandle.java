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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import org.apache.hadoop.fs.Path;

import static java.util.Objects.requireNonNull;

public class FileSystemExchangeSinkInstanceHandle
        implements ExchangeSinkInstanceHandle
{
    private final FileSystemExchangeSinkHandle sinkHandle;
    private final Path outputDirectory;
    private final int outputPartitionCount;

    public FileSystemExchangeSinkInstanceHandle(
            FileSystemExchangeSinkHandle sinkHandle,
            Path outputDirectory,
            int outputPartitionCount)
    {
        this.sinkHandle = requireNonNull(sinkHandle, "sinkHandle is null");
        this.outputDirectory = requireNonNull(outputDirectory, "outputDirectory is null");
        this.outputPartitionCount = outputPartitionCount;
    }

    @JsonCreator
    public FileSystemExchangeSinkInstanceHandle(
            @JsonProperty("sinkHandle") FileSystemExchangeSinkHandle sinkHandle,
            @JsonProperty("outputDirectory") String outputDirectory,
            @JsonProperty("outputPartitionCount") int outputPartitionCount)
    {
        this(sinkHandle, new Path(outputDirectory), outputPartitionCount);
    }

    public FileSystemExchangeSinkHandle getSinkHandle()
    {
        return sinkHandle;
    }

    public Path getOutputDirectory()
    {
        return outputDirectory;
    }

    public int getOutputPartitionCount()
    {
        return outputPartitionCount;
    }

    @JsonProperty("sinkHandle")
    public FileSystemExchangeSinkHandle getJsonSerializableSinkHandle()
    {
        return sinkHandle;
    }

    @JsonProperty("outputDirectory")
    public String getJsonSerializableOutputDirectory()
    {
        return outputDirectory.toString();
    }

    @JsonProperty("outputPartitionCount")
    public int getJsonSerializableOutputPartitionCount()
    {
        return outputPartitionCount;
    }
}
