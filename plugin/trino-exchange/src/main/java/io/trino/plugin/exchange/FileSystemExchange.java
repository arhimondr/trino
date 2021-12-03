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

import javax.annotation.concurrent.GuardedBy;
import javax.crypto.SecretKey;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
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
import static io.trino.plugin.exchange.FileSystemExchangeManager.PATH_SEPARATOR;
import static io.trino.plugin.exchange.FileSystemExchangeSink.COMMITTED_MARKER_FILE_NAME;
import static io.trino.plugin.exchange.FileSystemExchangeSink.DATA_FILE_SUFFIX;
import static java.util.Objects.requireNonNull;

public class FileSystemExchange
        implements Exchange
{
    private static final Pattern PARTITION_FILE_NAME_PATTERN = Pattern.compile("(\\d+)\\.data");

    private final URI baseDirectory;
    private final FileSystemExchangeStorage exchangeStorage;
    private final ExchangeContext exchangeContext;
    private final int outputPartitionCount;
    private final Optional<SecretKey> secretKey;

    @GuardedBy("this")
    private final Set<FileSystemExchangeSinkHandle> allSinks = new HashSet<>();
    @GuardedBy("this")
    private final Set<FileSystemExchangeSinkHandle> finishedSinks = new HashSet<>();
    @GuardedBy("this")
    private boolean noMoreSinks;

    private final CompletableFuture<List<ExchangeSourceHandle>> exchangeSourceHandlesFuture = new CompletableFuture<>();
    @GuardedBy("this")
    private boolean exchangeSourceHandlesCreated;

    public FileSystemExchange(URI baseDirectory, FileSystemExchangeStorage exchangeStorage, ExchangeContext exchangeContext, int outputPartitionCount, Optional<SecretKey> secretKey)
    {
        this.baseDirectory = requireNonNull(baseDirectory, "baseDirectory is null");
        this.exchangeStorage = requireNonNull(exchangeStorage, "exchangeStorage is null");
        this.exchangeContext = requireNonNull(exchangeContext, "exchangeContext is null");
        this.outputPartitionCount = outputPartitionCount;
        this.secretKey = requireNonNull(secretKey, "secretKey is null");
    }

    public void initialize()
    {
        try {
            exchangeStorage.createDirectories(getExchangeDirectory());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized ExchangeSinkHandle addSink(int taskPartitionId)
    {
        FileSystemExchangeSinkHandle sinkHandle = new FileSystemExchangeSinkHandle(
                exchangeContext.getQueryId(),
                exchangeContext.getStageId(),
                taskPartitionId,
                secretKey);
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
        FileSystemExchangeSinkHandle fileSystemExchangeSinkHandle = (FileSystemExchangeSinkHandle) sinkHandle;
        URI outputDirectory = getExchangeDirectory()
                .resolve(fileSystemExchangeSinkHandle.getTaskPartitionId() + PATH_SEPARATOR)
                .resolve(taskAttemptId + PATH_SEPARATOR);
        try {
            exchangeStorage.createDirectories(outputDirectory);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return new FileSystemExchangeSinkInstanceHandle(fileSystemExchangeSinkHandle, outputDirectory, outputPartitionCount);
    }

    @Override
    public void sinkFinished(ExchangeSinkInstanceHandle handle)
    {
        synchronized (this) {
            FileSystemExchangeSinkInstanceHandle fileSystemExchangeSinkInstanceHandle = (FileSystemExchangeSinkInstanceHandle) handle;
            finishedSinks.add(fileSystemExchangeSinkInstanceHandle.getSinkHandle());
        }
        checkInputReady();
    }

    private void checkInputReady()
    {
        verify(!Thread.holdsLock(this));
        List<ExchangeSourceHandle> exchangeSourceHandles = null;
        synchronized (this) {
            if (exchangeSourceHandlesCreated) {
                return;
            }
            if (noMoreSinks && finishedSinks.containsAll(allSinks)) {
                // input is ready, create exchange source handles
                exchangeSourceHandles = createExchangeSourceHandles();
                exchangeSourceHandlesCreated = true;
            }
        }
        if (exchangeSourceHandles != null) {
            exchangeSourceHandlesFuture.complete(exchangeSourceHandles);
        }
    }

    private synchronized List<ExchangeSourceHandle> createExchangeSourceHandles()
    {
        Multimap<Integer, URI> partitionFilesMap = ArrayListMultimap.create();
        for (FileSystemExchangeSinkHandle sinkHandle : finishedSinks) {
            URI committedAttemptPath = getCommittedAttemptPath(sinkHandle);
            Map<Integer, URI> partitions = getCommittedPartitions(committedAttemptPath);
            partitions.forEach(partitionFilesMap::put);
        }

        ImmutableList.Builder<ExchangeSourceHandle> result = ImmutableList.builder();
        for (Integer partitionId : partitionFilesMap.keySet()) {
            result.add(new FileSystemExchangeSourceHandle(partitionId, ImmutableList.copyOf(partitionFilesMap.get(partitionId)), secretKey));
        }
        return result.build();
    }

    private URI getCommittedAttemptPath(FileSystemExchangeSinkHandle sinkHandle)
    {
        URI sinkOutputBasePath = getExchangeDirectory()
                .resolve(sinkHandle.getTaskPartitionId() + PATH_SEPARATOR);
        try {
            List<URI> attemptPaths = exchangeStorage.listDirectories(sinkOutputBasePath).collect(toImmutableList());
            checkState(!attemptPaths.isEmpty(), "no attempts found for sink %s", sinkHandle);

            List<URI> committedAttemptPaths = attemptPaths.stream()
                    .filter(this::isCommitted)
                    .collect(toImmutableList());
            checkState(!committedAttemptPaths.isEmpty(), "no committed attempts found for %s", sinkHandle);

            return committedAttemptPaths.get(0);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private boolean isCommitted(URI attemptPath)
    {
        URI commitMarkerFilePath = attemptPath.resolve(COMMITTED_MARKER_FILE_NAME);
        return exchangeStorage.exists(commitMarkerFilePath);
    }

    private Map<Integer, URI> getCommittedPartitions(URI committedAttemptPath)
    {
        try {
            List<URI> partitionFiles = exchangeStorage.listFiles(committedAttemptPath)
                    .filter(file -> file.getPath().endsWith(DATA_FILE_SUFFIX))
                    .collect(toImmutableList());
            ImmutableMap.Builder<Integer, URI> result = ImmutableMap.builder();
            for (URI partitionFile : partitionFiles) {
                Matcher matcher = PARTITION_FILE_NAME_PATTERN.matcher(new File(partitionFile.getPath()).getName());
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

    private URI getExchangeDirectory()
    {
        return baseDirectory.resolve(exchangeContext.getQueryId() + "." + exchangeContext.getStageId() + PATH_SEPARATOR);
    }

    @Override
    public CompletableFuture<List<ExchangeSourceHandle>> getSourceHandles()
    {
        return exchangeSourceHandlesFuture;
    }

    @Override
    public ExchangeSourceSplitter split(ExchangeSourceHandle handle, long targetSizeInBytes)
    {
        // TODO: replace with correct split implementation
        FileSystemExchangeSourceHandle fileSystemExchangeSourceHandle = (FileSystemExchangeSourceHandle) handle;
        Iterator<URI> filesIterator = fileSystemExchangeSourceHandle.getFiles().iterator();
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
                    return Optional.of(new FileSystemExchangeSourceHandle(fileSystemExchangeSourceHandle.getPartitionId(), ImmutableList.of(filesIterator.next()), secretKey));
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
        FileSystemExchangeSourceHandle fileSystemExchangeSourceHandle = (FileSystemExchangeSourceHandle) handle;
        long sizeInBytes = 0;
        for (URI file : fileSystemExchangeSourceHandle.getFiles()) {
            try {
                sizeInBytes += exchangeStorage.size(file);
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
            exchangeStorage.deleteRecursively(getExchangeDirectory());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
