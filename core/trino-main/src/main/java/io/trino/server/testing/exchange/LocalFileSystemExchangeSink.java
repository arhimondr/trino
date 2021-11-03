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

import io.airlift.log.Logger;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.units.DataSize;
import io.trino.spi.exchange.ExchangeSink;

import javax.annotation.concurrent.GuardedBy;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.MoreFiles.deleteDirectoryContents;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.lang.Math.toIntExact;
import static java.nio.file.Files.createFile;
import static java.util.Objects.requireNonNull;

public class LocalFileSystemExchangeSink
        implements ExchangeSink
{
    private static final Logger log = Logger.get(LocalFileSystemExchangeSink.class);

    private static final int BUFFER_SIZE_IN_BYTES = toIntExact(DataSize.of(4, KILOBYTE).toBytes());

    private final Path outputDirectory;
    private final int partitionCount;

    @GuardedBy("this")
    private final Map<Integer, SliceOutput> outputs = new HashMap<>();
    @GuardedBy("this")
    private boolean committed;
    @GuardedBy("this")
    private boolean closed;

    public LocalFileSystemExchangeSink(Path outputDirectory, int partitionCount)
    {
        this.outputDirectory = requireNonNull(outputDirectory, "outputDirectory is null");
        this.partitionCount = partitionCount;
    }

    @Override
    public synchronized void add(int partitionId, Slice data)
    {
        checkArgument(partitionId < partitionCount, "partition id is expected to be less than %s: %s", partitionCount, partitionId);
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
        Path outputPath = outputDirectory.resolve(partitionId + ".data");
        try {
            return new OutputStreamSliceOutput(new FileOutputStream(outputPath.toFile()), BUFFER_SIZE_IN_BYTES);
        }
        catch (FileNotFoundException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized long getSystemMemoryUsage()
    {
        return BUFFER_SIZE_IN_BYTES * (long) outputs.size();
    }

    @Override
    public synchronized void finish()
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
                createFile(outputDirectory.resolve("committed"));
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
            deleteDirectoryContents(outputDirectory);
        }
        catch (IOException e) {
            log.warn(e, "Error cleaning output directory");
        }
    }
}
