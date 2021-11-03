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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeSinkHandle;
import io.trino.spi.exchange.ExchangeSinkId;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceSplitter;
import io.trino.spi.exchange.ExchangeSourceStatistics;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static java.nio.file.Files.createDirectories;
import static java.util.Objects.requireNonNull;

public class LocalFileSystemExchange
        implements Exchange
{
    private static final Pattern PARTITION_FILE_NAME_PATTERN = Pattern.compile("(\\d+)\\.data");

    private final Path baseDirectory;
    private final ExchangeContext exchangeContext;
    private final int partitionCount;

    @GuardedBy("this")
    private final List<LocalFileSystemExchangeSinkId> allSinks = new ArrayList<>();
    @GuardedBy("this")
    private final List<LocalFileSystemExchangeSinkId> finishedSinks = new ArrayList<>();
    @GuardedBy("this")
    private boolean noMoreSinks;

    private final CompletableFuture<List<ExchangeSourceHandle>> exchangeSourceHandles = new CompletableFuture<>();

    public LocalFileSystemExchange(Path baseDirectory, ExchangeContext exchangeContext, int partitionCount)
    {
        this.baseDirectory = requireNonNull(baseDirectory, "baseDirectory is null");
        this.exchangeContext = requireNonNull(exchangeContext, "exchangeContext is null");
        this.partitionCount = partitionCount;
    }

    public void initialize()
    {
        try {
            createDirectories(getExchangeDirectory());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized ExchangeSinkId addSink(int taskPartitionId)
    {
        LocalFileSystemExchangeSinkId sinkId = new LocalFileSystemExchangeSinkId(
                exchangeContext.getQueryId(),
                exchangeContext.getStageId(),
                taskPartitionId);
        allSinks.add(sinkId);
        return sinkId;
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
    public ExchangeSinkHandle getSinkHandle(ExchangeSinkId sinkId, int taskAttemptId)
    {
        LocalFileSystemExchangeSinkId localSinkId = (LocalFileSystemExchangeSinkId) sinkId;
        Path outputDirectory = getExchangeDirectory()
                .resolve(Integer.toString(localSinkId.getTaskPartitionId()))
                .resolve(Integer.toString(taskAttemptId));
        try {
            createDirectories(outputDirectory);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new LocalFileSystemExchangeSinkHandle(localSinkId, outputDirectory, partitionCount);
    }

    @Override
    public void sinkFinished(ExchangeSinkHandle handle)
    {
        synchronized (this) {
            LocalFileSystemExchangeSinkHandle localHandle = (LocalFileSystemExchangeSinkHandle) handle;
            finishedSinks.add(localHandle.getSinkId());
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
        for (LocalFileSystemExchangeSinkId sinkId : finishedSinks) {
            Path committedAttemptPath = getCommittedAttemptPath(sinkId);
            Map<Integer, Path> partitions = getCommittedPartitions(committedAttemptPath);
            partitions.forEach(partitionFilesMap::put);
        }

        ImmutableList.Builder<ExchangeSourceHandle> result = ImmutableList.builder();
        for (Integer partitionId : partitionFilesMap.keySet()) {
            result.add(new LocalFileSystemExchangeSourceHandle(ImmutableList.copyOf(partitionFilesMap.get(partitionId))));
        }
        return result.build();
    }

    private Path getCommittedAttemptPath(LocalFileSystemExchangeSinkId sinkId)
    {
        Path sinkOutputBasePath = getExchangeDirectory()
                .resolve(Integer.toString(sinkId.getTaskPartitionId()));
        try {
            List<Path> attemptPaths = Files.list(sinkOutputBasePath)
                    .filter(Files::isDirectory)
                    .collect(toImmutableList());
            checkState(!attemptPaths.isEmpty(), "no attempts found for sink %s", sinkId);

            List<Path> committedAttemptPaths = attemptPaths.stream()
                    .filter(LocalFileSystemExchange::isCommitted)
                    .collect(toImmutableList());
            checkState(!committedAttemptPaths.isEmpty(), "no committed attempts found for %s", sinkId);

            return committedAttemptPaths.get(0);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static boolean isCommitted(Path attemptPath)
    {
        Path commitMarkerFilePath = attemptPath.resolve("committed");
        return Files.exists(commitMarkerFilePath);
    }

    private static Map<Integer, Path> getCommittedPartitions(Path committedAttemptPath)
    {
        try {
            List<Path> partitionFiles = Files.list(committedAttemptPath)
                    .filter(path -> path.toString().endsWith(".data"))
                    .collect(toImmutableList());
            ImmutableMap.Builder<Integer, Path> result = ImmutableMap.builder();
            for (Path partitionFile : partitionFiles) {
                Matcher matcher = PARTITION_FILE_NAME_PATTERN.matcher(partitionFile.getFileName().toString());
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
        return baseDirectory.resolve(exchangeContext.getQueryId() + "." + exchangeContext.getStageId());
    }

    @Override
    public CompletableFuture<List<ExchangeSourceHandle>> getSourceHandles()
    {
        return exchangeSourceHandles;
    }

    @Override
    public ExchangeSourceSplitter split(ExchangeSourceHandle handle, long targetSizeInBytes)
    {
        // always split for testing
        LocalFileSystemExchangeSourceHandle localHandle = (LocalFileSystemExchangeSourceHandle) handle;
        Iterator<Path> filesIterator = localHandle.getFiles().iterator();
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
                    return Optional.of(new LocalFileSystemExchangeSourceHandle(ImmutableList.of(filesIterator.next())));
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
        LocalFileSystemExchangeSourceHandle localHandle = (LocalFileSystemExchangeSourceHandle) handle;
        long sizeInBytes = 0;
        for (Path file : localHandle.getFiles()) {
            try {
                sizeInBytes += Files.size(file);
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
            deleteRecursively(getExchangeDirectory());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
