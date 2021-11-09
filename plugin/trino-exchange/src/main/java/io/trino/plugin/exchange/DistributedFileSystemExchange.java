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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeSinkHandle;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceSplitter;
import io.trino.spi.exchange.ExchangeSourceStatistics;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class DistributedFileSystemExchange
        implements Exchange
{
    private static final Pattern PARTITION_FILE_NAME_PATTERN = Pattern.compile("(\\d+)\\.data");

    private final FileSystem fileSystem;
    private final Path baseDirectory;
    private final ExchangeContext exchangeContext;
    private final int outputPartitionCount;

    @GuardedBy("this")
    private final Set<DistributedFileSystemExchangeSinkHandle> allSinks = new HashSet<>();
    @GuardedBy("this")
    private final Set<DistributedFileSystemExchangeSinkHandle> finishedSinks = new HashSet<>();
    private final CompletableFuture<List<ExchangeSourceHandle>> exchangeSourceHandles = new CompletableFuture<>();
    @GuardedBy("this")
    private boolean noMoreSinks;

    public DistributedFileSystemExchange(FileSystem fileSystem, Path baseDirectory, ExchangeContext exchangeContext, int outputPartitionCount)
    {
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.baseDirectory = requireNonNull(baseDirectory, "baseDirectory is null");
        this.exchangeContext = requireNonNull(exchangeContext, "exchangeContext is null");
        this.outputPartitionCount = outputPartitionCount;
    }

    public void initialize()
    {
        try {
            fileSystem.mkdirs(getExchangeDirectory());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public ExchangeSinkHandle addSink(int taskPartitionId)
    {
        DistributedFileSystemExchangeSinkHandle sinkHandle = new DistributedFileSystemExchangeSinkHandle(
                exchangeContext.getQueryId(),
                exchangeContext.getStageId(),
                taskPartitionId);
        allSinks.add(sinkHandle);
        return sinkHandle;
    }

    @Override
    public void noMoreSinks()
    {
        synchronized (this) {
            noMoreSinks = true;
        }
        checkInputReady();
    }

    @Override
    public ExchangeSinkInstanceHandle instantiateSink(ExchangeSinkHandle sinkHandle, int taskAttemptId)
    {
        DistributedFileSystemExchangeSinkHandle distributedFileSystemExchangeSinkHandle = (DistributedFileSystemExchangeSinkHandle) sinkHandle;
        Path outputDirectory = new Path(new Path(getExchangeDirectory(),
                Integer.toString(distributedFileSystemExchangeSinkHandle.getTaskPartitionId())),
                Integer.toString(taskAttemptId));
        try {
            fileSystem.mkdirs(outputDirectory);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return new DistributedFileSystemExchangeSinkInstanceHandle(distributedFileSystemExchangeSinkHandle, outputDirectory, outputPartitionCount);
    }

    @Override
    public void sinkFinished(ExchangeSinkInstanceHandle handle)
    {
        synchronized (this) {
            DistributedFileSystemExchangeSinkInstanceHandle localHandle = (DistributedFileSystemExchangeSinkInstanceHandle) handle;
            finishedSinks.add(localHandle.getSinkHandle());
        }
        checkInputReady();
    }

    private void checkInputReady()
    {
        verify(!Thread.holdsLock(this));
        boolean ready;
        synchronized (this) {
            ready = noMoreSinks && finishedSinks.containsAll(allSinks);
        }
        if (ready) {
            exchangeSourceHandles.complete(createExchangeSourceHandles());
        }
    }

    private synchronized List<ExchangeSourceHandle> createExchangeSourceHandles()
    {
        Multimap<Integer, Path> partitionFilesMap = ArrayListMultimap.create();
        for (DistributedFileSystemExchangeSinkHandle sinkHandle : finishedSinks) {
            Path committedAttemptPath = getCommittedAttemptPath(sinkHandle);
            Map<Integer, Path> partitions = getCommittedPartitions(committedAttemptPath);
            partitions.forEach(partitionFilesMap::put);
        }

        ImmutableList.Builder<ExchangeSourceHandle> result = ImmutableList.builder();
        for (Integer partitionId : partitionFilesMap.keySet()) {
            result.add(new DistributedFileSystemExchangeSourceHandle(partitionId, ImmutableList.copyOf(partitionFilesMap.get(partitionId))));
        }
        return result.build();
    }

    private Path getCommittedAttemptPath(DistributedFileSystemExchangeSinkHandle sinkHandle)
    {
        Path sinkOutputBasePath = new Path(getExchangeDirectory(), Integer.toString(sinkHandle.getTaskPartitionId()));
        try {
            List<Path> attemptPaths = Arrays.stream(fileSystem.listStatus(sinkOutputBasePath))
                    .filter(FileStatus::isDirectory).map(FileStatus::getPath)
                    .collect(toImmutableList());
            checkState(!attemptPaths.isEmpty(), "no attempts found for sink %s", sinkHandle);

            List<Path> committedAttemptPaths = attemptPaths.stream()
                    .filter(this::isCommitted)
                    .collect(toImmutableList());
            checkState(!committedAttemptPaths.isEmpty(), "no committed attempts found for %s", sinkHandle);

            return committedAttemptPaths.get(0);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private boolean isCommitted(Path attemptPath)
    {
        Path commitMarkerFilePath = new Path(attemptPath, "committed");
        try {
            return fileSystem.exists(commitMarkerFilePath);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Map<Integer, Path> getCommittedPartitions(Path committedAttemptPath)
    {
        try {
            RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(committedAttemptPath, false);
            List<Path> partitionFiles = new ArrayList<>();
            while (iterator.hasNext()) {
                Path file = iterator.next().getPath();
                if (file.getName().endsWith(".data")) {
                    partitionFiles.add(file);
                }
            }
            ImmutableMap.Builder<Integer, Path> result = ImmutableMap.builder();
            for (Path partitionFile : partitionFiles) {
                Matcher matcher = PARTITION_FILE_NAME_PATTERN.matcher(partitionFile.getName());
                checkState(matcher.matches(), "unexpected partition file: %s", partitionFile);
                int partitionId = Integer.parseInt(matcher.group(1));
                result.put(partitionId, partitionFile);
            }
            return result.build();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path getExchangeDirectory()
    {
        return new Path(baseDirectory, exchangeContext.getQueryId() + "." + exchangeContext.getStageId());
    }

    @Override
    public CompletableFuture<List<ExchangeSourceHandle>> getSourceHandles()
    {
        return exchangeSourceHandles;
    }

    @Override
    public ExchangeSourceSplitter split(ExchangeSourceHandle handle, long targetSizeInBytes)
    {
        // TODO: replace with correct split implementation
        DistributedFileSystemExchangeSourceHandle distributedHandle = (DistributedFileSystemExchangeSourceHandle) handle;
        Iterator<Path> filesIterator = distributedHandle.getFiles().iterator();
        return new ExchangeSourceSplitter()
        {
            @Override
            public CompletableFuture<?> isBlocked()
            {
                return NOT_BLOCKED;
            }

            @Override
            public Optional<ExchangeSourceHandle> getNext()
            {
                if (filesIterator.hasNext()) {
                    return Optional.of(new DistributedFileSystemExchangeSourceHandle(distributedHandle.getPartitionId(), ImmutableList.of(filesIterator.next())));
                }
                return Optional.empty();
            }

            @Override
            public void close()
            {
            }
        };
    }

    @Override
    public ExchangeSourceStatistics getExchangeSourceStatistics(ExchangeSourceHandle handle)
    {
        DistributedFileSystemExchangeSourceHandle localHandle = (DistributedFileSystemExchangeSourceHandle) handle;
        long sizeInBytes = 0;
        for (Path file : localHandle.getFiles()) {
            try {
                sizeInBytes += fileSystem.getFileStatus(file).getLen();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return new ExchangeSourceStatistics(sizeInBytes);
    }

    @Override
    public void close()
    {
        try {
            fileSystem.delete(getExchangeDirectory(), true);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
