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
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.spi.exchange.ExchangeSink;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.concurrent.GuardedBy;
import javax.crypto.SecretKey;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static java.util.Objects.requireNonNull;

public class FileSystemExchangeSink
        implements ExchangeSink
{
    private static final Logger log = Logger.get(FileSystemExchangeSink.class);

    public static final String COMMITTED_MARKER_FILE_NAME = "committed";
    public static final String DATA_FILE_SUFFIX = ".data";

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FileSystemExchangeSink.class).instanceSize();
    private static final int INTEGER_INSTANCE_SIZE = ClassLayout.parseClass(Integer.class).instanceSize();

    private final FileSystemExchangeStorage exchangeStorage;
    private final URI outputDirectory;
    private final int outputPartitionCount;
    private final Optional<SecretKey> secretKey;

    @GuardedBy("this")
    private final Map<Integer, SliceOutput> outputs = new HashMap<>();
    @GuardedBy("this")
    private boolean committed;
    @GuardedBy("this")
    private boolean closed;

    public FileSystemExchangeSink(FileSystemExchangeStorage exchangeStorage, URI outputDirectory, int outputPartitionCount, Optional<SecretKey> secretKey)
    {
        this.exchangeStorage = requireNonNull(exchangeStorage, "exchangeStorage is null");
        this.outputDirectory = requireNonNull(outputDirectory, "outputDirectory is null");
        this.outputPartitionCount = outputPartitionCount;
        this.secretKey = requireNonNull(secretKey, "secretKey is null");
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
        URI outputPath = outputDirectory.resolve(partitionId + DATA_FILE_SUFFIX);
        try {
            return exchangeStorage.createSliceOutput(outputPath, secretKey);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized long getSystemMemoryUsage()
    {
        return INSTANCE_SIZE +
                estimatedSizeOf(outputs, (ignored) -> INTEGER_INSTANCE_SIZE, SliceOutput::getRetainedSize);
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
                exchangeStorage.createFile(outputDirectory.resolve(COMMITTED_MARKER_FILE_NAME));
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
    public synchronized void abort()
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
            exchangeStorage.deleteRecursively(outputDirectory);
        }
        catch (IOException e) {
            log.warn(e, "Error cleaning output directory");
        }
    }
}
