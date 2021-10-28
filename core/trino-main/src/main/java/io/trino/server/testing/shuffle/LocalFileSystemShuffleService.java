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
package io.trino.server.testing.shuffle;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.airlift.log.Logger;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.units.DataSize;
import io.trino.spi.shuffle.Shuffle;
import io.trino.spi.shuffle.ShuffleHandle;
import io.trino.spi.shuffle.ShuffleInput;
import io.trino.spi.shuffle.ShuffleLostTaskOutputListener;
import io.trino.spi.shuffle.ShuffleOutput;
import io.trino.spi.shuffle.ShufflePartitionHandle;
import io.trino.spi.shuffle.ShuffleService;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteDirectoryContents;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.lang.Math.toIntExact;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.createFile;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class LocalFileSystemShuffleService
        implements ShuffleService
{
    private static final Logger log = Logger.get(LocalFileSystemShuffleService.class);

    private static final Pattern PARTITION_FILE_NAME_PATTERN = Pattern.compile("(\\d+)\\.data");

    private final Path baseDirectory;

    public LocalFileSystemShuffleService(Path baseDirectory)
    {
        this.baseDirectory = requireNonNull(baseDirectory, "baseDirectory is null");
    }

    @Override
    public Shuffle create(int partitionCount)
    {
        LocalFileSystemShuffleHandle handle = new LocalFileSystemShuffleHandle(randomUUID().toString(), partitionCount);
        LocalFileSystemShuffle shuffle = new LocalFileSystemShuffle(baseDirectory, handle);
        shuffle.initialize();
        return shuffle;
    }

    @Override
    public List<ShufflePartitionHandle> splitPartition(ShufflePartitionHandle handle, long targetSizeInBytes)
    {
        // always split for testing
        LocalFileSystemShufflePartitionHandle localHandle = (LocalFileSystemShufflePartitionHandle) handle;
        ImmutableList.Builder<ShufflePartitionHandle> result = ImmutableList.builder();
        for (Path file : localHandle.getFiles()) {
            try {
                long fileSize = Files.size(file);
                result.add(new LocalFileSystemShufflePartitionHandle(
                        localHandle.getPartitionId(),
                        fileSize,
                        ImmutableList.of(file)));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return result.build();
    }

    @Override
    public ShuffleOutput createOutput(ShuffleHandle handle, int outputTaskPartitionId)
    {
        LocalFileSystemShuffleHandle localHandle = (LocalFileSystemShuffleHandle) handle;
        String shuffleId = localHandle.getShuffleId();
        Path outputDirectory = baseDirectory.resolve(shuffleId).resolve(Integer.toString(outputTaskPartitionId)).resolve(randomUUID().toString());
        try {
            createDirectories(outputDirectory);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new LocalFileSystemShuffleOutput(outputDirectory, localHandle.getOutputPartitionCount());
    }

    @Override
    public ShuffleInput createInput(List<ShufflePartitionHandle> partitionHandles)
    {
        List<Path> files = partitionHandles.stream()
                .map(LocalFileSystemShufflePartitionHandle.class::cast)
                .flatMap(handle -> handle.getFiles().stream())
                .collect(toImmutableList());
        return new LocalFileSystemShuffleInput(files);
    }

    private static class LocalFileSystemShuffle
            implements Shuffle
    {
        private final Path baseDirectory;
        private final LocalFileSystemShuffleHandle shuffleHandle;

        private final CompletableFuture<List<ShufflePartitionHandle>> inputPartitionHandles = new CompletableFuture<>();
        private final CompletableFuture<?> inputReady = new CompletableFuture<>();

        @GuardedBy("this")
        private Set<Integer> outputTaskPartitionIds;
        @GuardedBy("this")
        private final Set<Integer> finishedOutputTaskPartitionIds = new HashSet<>();

        private LocalFileSystemShuffle(Path baseDirectory, LocalFileSystemShuffleHandle shuffleHandle)
        {
            this.baseDirectory = requireNonNull(baseDirectory, "baseDirectory is null");
            this.shuffleHandle = requireNonNull(shuffleHandle, "shuffleHandle is null");
        }

        public void initialize()
        {
            try {
                createDirectories(getShufflePath());
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public ShuffleHandle getHandle()
        {
            return shuffleHandle;
        }

        @Override
        public void outputTaskFinished(int outputTaskPartitionId)
        {
            synchronized (this) {
                finishedOutputTaskPartitionIds.add(outputTaskPartitionId);
            }
            checkInputReady();
        }

        @Override
        public void noMoreOutputTasks(Set<Integer> outputTaskPartitionIds)
        {
            synchronized (this) {
                checkState(this.outputTaskPartitionIds == null, "outputTaskPartitionIds is already set");
                this.outputTaskPartitionIds = ImmutableSet.copyOf(outputTaskPartitionIds);
            }
            checkInputReady();
        }

        private void checkInputReady()
        {
            verify(!Thread.holdsLock(this));
            boolean ready;
            synchronized (this) {
                ready = outputTaskPartitionIds != null && finishedOutputTaskPartitionIds.containsAll(outputTaskPartitionIds);
            }
            if (ready) {
                inputPartitionHandles.complete(createInputPartitionHandles());
                inputReady.complete(null);
            }
        }

        private synchronized List<ShufflePartitionHandle> createInputPartitionHandles()
        {
            Multimap<Integer, Path> partitionFilesMap = ArrayListMultimap.create();
            for (int taskPartitionId : finishedOutputTaskPartitionIds) {
                Path committedAttemptPath = getCommittedAttemptPath(taskPartitionId);
                Map<Integer, Path> partitions = getCommittedPartitions(committedAttemptPath);
                partitions.forEach(partitionFilesMap::put);
            }

            ImmutableList.Builder<ShufflePartitionHandle> result = ImmutableList.builder();
            for (Integer partitionId : partitionFilesMap.keySet()) {
                List<Path> files = ImmutableList.copyOf(partitionFilesMap.get(partitionId));
                long totalSizeInBytes = files.stream()
                        .mapToLong(path -> {
                            try {
                                return Files.size(path);
                            }
                            catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        })
                        .sum();
                result.add(new LocalFileSystemShufflePartitionHandle(partitionId, totalSizeInBytes, files));
            }
            return result.build();
        }

        private Path getCommittedAttemptPath(int taskPartitionId)
        {
            Path taskOutputBasePath = getTaskOutputBasePath(taskPartitionId);
            try {
                List<Path> attemptPaths = Files.list(taskOutputBasePath)
                        .filter(Files::isDirectory)
                        .collect(toImmutableList());
                checkState(!attemptPaths.isEmpty(), "no attempts found for %s", taskPartitionId);

                List<Path> committedAttemptPaths = attemptPaths.stream()
                        .filter(LocalFileSystemShuffle::isCommitted)
                        .collect(toImmutableList());
                checkState(!committedAttemptPaths.isEmpty(), "no committed attempts found for %s", taskPartitionId);

                return committedAttemptPaths.get(0);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private Path getTaskOutputBasePath(int taskPartitionId)
        {
            return getShufflePath().resolve(Integer.toString(taskPartitionId));
        }

        private Path getShufflePath()
        {
            return baseDirectory.resolve(shuffleHandle.getShuffleId());
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

        @Override
        public void registerLostTaskOutputListener(ShuffleLostTaskOutputListener listener)
        {
        }

        @Override
        public CompletableFuture<List<ShufflePartitionHandle>> getInputPartitionHandles()
        {
            return inputPartitionHandles;
        }

        @Override
        public CompletableFuture<?> isInputReady()
        {
            return inputReady;
        }

        @Override
        public void close()
        {
            try {
                deleteRecursively(getShufflePath());
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static class LocalFileSystemShuffleInput
            implements ShuffleInput
    {
        private static final long BUFFER_SIZE_IN_BYTES = DataSize.of(4, KILOBYTE).toBytes();

        @GuardedBy("this")
        private final Iterator<Path> files;

        @GuardedBy("this")
        private SliceInput sliceInput;
        @GuardedBy("this")
        private boolean closed;

        private LocalFileSystemShuffleInput(List<Path> files)
        {
            this.files = ImmutableList.copyOf(requireNonNull(files, "files is null")).iterator();
        }

        @Override
        public synchronized boolean isFinished()
        {
            return closed || (!files.hasNext() && sliceInput == null);
        }

        @Nullable
        @Override
        public synchronized Slice pool()
        {
            if (isFinished()) {
                return null;
            }

            if (sliceInput != null && !sliceInput.isReadable()) {
                sliceInput.close();
                sliceInput = null;
            }

            if (sliceInput == null) {
                if (files.hasNext()) {
                    Path file = files.next();
                    try {
                        sliceInput = new InputStreamSliceInput(new FileInputStream(file.toFile()));
                    }
                    catch (FileNotFoundException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            }

            if (sliceInput == null) {
                return null;
            }

            if (!sliceInput.isReadable()) {
                sliceInput.close();
                sliceInput = null;
                return null;
            }

            int size = sliceInput.readInt();
            return sliceInput.readSlice(size);
        }

        @Override
        public synchronized long getSystemMemoryUsage()
        {
            return sliceInput != null ? BUFFER_SIZE_IN_BYTES : 0;
        }

        @Override
        public synchronized void close()
        {
            if (!closed) {
                closed = true;
                if (sliceInput != null) {
                    sliceInput.close();
                    sliceInput = null;
                }
            }
        }
    }

    private static class LocalFileSystemShuffleOutput
            implements ShuffleOutput
    {
        private static final int BUFFER_SIZE_IN_BYTES = toIntExact(DataSize.of(4, KILOBYTE).toBytes());

        private final Path outputDirectory;
        private final int outputPartitionCount;

        @GuardedBy("this")
        private final Map<Integer, SliceOutput> outputs = new HashMap<>();
        @GuardedBy("this")
        private boolean committed;
        @GuardedBy("this")
        private boolean closed;

        public LocalFileSystemShuffleOutput(Path outputDirectory, int outputPartitionCount)
        {
            this.outputDirectory = requireNonNull(outputDirectory, "outputDirectory is null");
            this.outputPartitionCount = outputPartitionCount;
        }

        @Override
        public synchronized void add(int partitionId, Slice data)
        {
            checkArgument(partitionId < outputPartitionCount, "unexpected partition id %s, output partition count is set to %s", partitionId, outputPartitionCount);
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
        public synchronized void commit()
        {
            if (closed || committed) {
                return;
            }
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
            committed = true;
        }

        @Override
        public synchronized void close()
        {
            if (!closed) {
                closed = true;
                if (!committed) {
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
        }
    }
}
