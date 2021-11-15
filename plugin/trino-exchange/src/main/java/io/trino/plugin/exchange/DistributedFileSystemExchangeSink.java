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

import io.airlift.log.Logger;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.units.DataSize;
import io.trino.spi.exchange.ExchangeSink;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class DistributedFileSystemExchangeSink
        implements ExchangeSink
{
    private static final Logger log = Logger.get(DistributedFileSystemExchangeSink.class);

    private static final int BUFFER_SIZE_IN_BYTES = toIntExact(DataSize.of(4, KILOBYTE).toBytes());

    private final FileSystem fileSystem;
    private final Path outputDirectory;
    private final int outputPartitionCount;

    @GuardedBy("this")
    private final Map<Integer, SliceOutput> outputs = new HashMap<>();
    @GuardedBy("this")
    private boolean committed;
    @GuardedBy("this")
    private boolean closed;

    public DistributedFileSystemExchangeSink(FileSystem fileSystem, Path outputDirectory, int outputPartitionCount)
    {
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.outputDirectory = requireNonNull(outputDirectory, "outputDirectory is null");
        this.outputPartitionCount = outputPartitionCount;
    }

    @Override
    public synchronized void add(int partitionId, Slice data)
    {
        checkArgument(partitionId < outputPartitionCount, "partition id is expected to be less than %s: %s", outputPartitionCount, partitionId);
        checkState(!committed, "already committed");
        if (closed) {
            return;
        }
        SliceOutput output = outputs.computeIfAbsent(partitionId, this::createOutput);
        output.writeInt(data.length());
        output.writeBytes(data);
    }

    private SliceOutput createOutput(int partitionId)
    {
        Path outputPath = new Path(outputDirectory, partitionId + ".data");
        try {
            return new OutputStreamSliceOutput(fileSystem.create(outputPath), BUFFER_SIZE_IN_BYTES);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return BUFFER_SIZE_IN_BYTES * (long) outputs.size();
    }

    @Override
    public void finish()
    {
        if (closed) {
            return;
        }
        try {
            for (SliceOutput output : outputs.values()) {
                try {
                    output.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            try {
                fileSystem.create(new Path(outputDirectory, "committed")).close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        catch (Throwable t) {
            abort();
            throw t;
        }
        committed = true;
        closed = true;
    }

    @Override
    public void abort()
    {
        if (closed) {
            return;
        }
        closed = true;
        for (SliceOutput output : outputs.values()) {
            try {
                output.close();
            }
            catch (IOException e) {
                log.warn(e, "Error closing output");
            }
        }

        try {
            fileSystem.delete(outputDirectory, true);
        }
        catch (IOException e) {
            log.warn(e, "Error cleaning output directory");
        }
    }
}
