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
package io.trino.server.testing.exchange;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.exchange.ExchangeSinkHandle;

import java.nio.file.Path;

import static java.util.Objects.requireNonNull;

public class LocalFileSystemExchangeSinkHandle
        implements ExchangeSinkHandle
{
    private final LocalFileSystemExchangeSinkId sinkId;
    private final Path outputDirectory;
    private final int partitionCount;

    @JsonCreator
    public LocalFileSystemExchangeSinkHandle(
            @JsonProperty("sinkId") LocalFileSystemExchangeSinkId sinkId,
            @JsonProperty("outputDirectory") Path outputDirectory,
            @JsonProperty("partitionCount") int partitionCount)
    {
        this.sinkId = requireNonNull(sinkId, "sinkId is null");
        this.outputDirectory = requireNonNull(outputDirectory, "outputDirectory is null");
        this.partitionCount = partitionCount;
    }

    @JsonProperty
    public LocalFileSystemExchangeSinkId getSinkId()
    {
        return sinkId;
    }

    @JsonProperty
    public Path getOutputDirectory()
    {
        return outputDirectory;
    }

    @JsonProperty
    public int getPartitionCount()
    {
        return partitionCount;
    }
}
