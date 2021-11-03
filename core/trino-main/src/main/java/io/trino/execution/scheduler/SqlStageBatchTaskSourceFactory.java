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
package io.trino.execution.scheduler;

import com.google.common.base.VerifyException;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.execution.Lifespan;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.TableExecuteContext;
import io.trino.execution.TableExecuteContextManager;
import io.trino.metadata.Split;
import io.trino.exchange.ShuffleServiceManager;
import io.trino.spi.HostAddress;
import io.trino.spi.QueryId;
import io.trino.spi.exchange.ShufflePartitionHandle;
import io.trino.spi.exchange.ShuffleService;
import io.trino.split.SplitSource;
import io.trino.split.SplitSource.SplitBatch;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.SplitSourceFactory;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;

import javax.inject.Inject;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.LongConsumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.newIdentityHashSet;
import static com.google.common.collect.Sets.union;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.SystemSessionProperties.getBatchExecutionTargetPartitionSize;
import static io.trino.SystemSessionProperties.getBatchExecutionTargetPartitionSplitCount;
import static io.trino.connector.CatalogName.isInternalSystemConnector;
import static io.trino.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static java.util.Objects.requireNonNull;

public class SqlStageBatchTaskSourceFactory
        implements BatchTaskSourceFactory
{
    private static final Logger log = Logger.get(SqlStageBatchTaskSourceFactory.class);

    private final ShuffleServiceManager shuffleServiceManager;
    private final SplitSourceFactory splitSourceFactory;
    private final TableExecuteContextManager tableExecuteContextManager;
    private final int splitBatchSize;

    @Inject
    public SqlStageBatchTaskSourceFactory(
            ShuffleServiceManager shuffleServiceManager,
            SplitSourceFactory splitSourceFactory,
            TableExecuteContextManager tableExecuteContextManager,
            QueryManagerConfig queryManagerConfig)
    {
        this(shuffleServiceManager, splitSourceFactory, tableExecuteContextManager, requireNonNull(queryManagerConfig, "queryManagerConfig is null").getScheduleSplitBatchSize());
    }

    public SqlStageBatchTaskSourceFactory(
            ShuffleServiceManager shuffleServiceManager,
            SplitSourceFactory splitSourceFactory,
            TableExecuteContextManager tableExecuteContextManager,
            int splitBatchSize)
    {
        this.shuffleServiceManager = requireNonNull(shuffleServiceManager, "shuffleServiceManager is null");
        this.splitSourceFactory = requireNonNull(splitSourceFactory, "splitSourceFactory is null");
        this.tableExecuteContextManager = requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");
        this.splitBatchSize = splitBatchSize;
    }

    @Override
    public BatchTaskSource create(
            Session session,
            PlanFragment fragment,
            Multimap<PlanFragmentId, ShufflePartitionHandle> shuffleInputs,
            LongConsumer getSplitTimeRecorder,
            Optional<int[]> bucketToPartitionMap,
            Optional<BucketNodeMap> bucketNodeMap)
    {
        PartitioningHandle partitioning = fragment.getPartitioning();

        if (partitioning.equals(SINGLE_DISTRIBUTION)) {
            return SingleDistributionTaskSource.create(fragment, shuffleInputs);
        }
        else if (partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION) || partitioning.equals(SCALED_WRITER_DISTRIBUTION)) {
            return ArbitraryDistributionTaskSource.create(
                    fragment,
                    shuffleServiceManager.getShuffleService(),
                    shuffleInputs,
                    getBatchExecutionTargetPartitionSize(session));
        }
        else if (partitioning.equals(FIXED_HASH_DISTRIBUTION) || partitioning.getConnectorId().isPresent()) {
            return HashDistributionTaskSource.create(
                    session,
                    fragment,
                    splitSourceFactory,
                    shuffleInputs,
                    splitBatchSize,
                    getSplitTimeRecorder,
                    bucketToPartitionMap,
                    bucketNodeMap);
        }
        else if (partitioning.equals(SOURCE_DISTRIBUTION)) {
            return SourceDistributionTaskSource.create(
                    session,
                    fragment,
                    shuffleInputs,
                    splitSourceFactory,
                    tableExecuteContextManager,
                    splitBatchSize,
                    getSplitTimeRecorder,
                    getBatchExecutionTargetPartitionSplitCount(session));
        }

        throw new IllegalArgumentException("Unexpected partitioning: " + partitioning);
    }

    public static class SingleDistributionTaskSource
            implements BatchTaskSource
    {
        private final Multimap<PlanNodeId, ShufflePartitionHandle> shuffleInputs;

        private boolean finished;

        public static SingleDistributionTaskSource create(PlanFragment fragment, Multimap<PlanFragmentId, ShufflePartitionHandle> shuffleInputs)
        {
            checkArgument(fragment.getPartitionedSources().isEmpty(), "no partitioned sources (table scans) expected, got: %s", fragment.getPartitionedSources());
            return new SingleDistributionTaskSource(getInputsForRemoteSources(fragment.getRemoteSourceNodes(), shuffleInputs));
        }

        public SingleDistributionTaskSource(Multimap<PlanNodeId, ShufflePartitionHandle> shuffleInputs)
        {
            this.shuffleInputs = ImmutableMultimap.copyOf(requireNonNull(shuffleInputs, "shuffleInputs is null"));
        }

        @Override
        public List<BatchTask> getMoreTasks()
        {
            List<BatchTask> result = ImmutableList.of(new BatchTask(0, ImmutableMultimap.of(), shuffleInputs, Optional.empty()));
            finished = true;
            return result;
        }

        @Override
        public boolean isFinished()
        {
            return finished;
        }

        @Override
        public Optional<CatalogName> getTaskCatalog()
        {
            return Optional.empty();
        }

        @Override
        public void close()
        {
        }
    }

    public static class ArbitraryDistributionTaskSource
            implements BatchTaskSource
    {
        private final ShuffleService shuffleService;
        private final Multimap<PlanNodeId, ShufflePartitionHandle> shuffleInputs;
        private final long targetPartitionSizeInBytes;

        private boolean finished;

        public static ArbitraryDistributionTaskSource create(
                PlanFragment fragment,
                ShuffleService shuffleService,
                Multimap<PlanFragmentId, ShufflePartitionHandle> shuffleInputs,
                DataSize targetPartitionSize)
        {
            checkArgument(fragment.getPartitionedSources().isEmpty(), "no partitioned sources (table scans) expected, got: %s", fragment.getPartitionedSources());
            checkArgument(fragment.getRemoteSourceNodes().stream().noneMatch(node -> node.getExchangeType() == REPLICATE), "replicated exchanges are not expected in source distributed stage, got: %s", fragment.getRemoteSourceNodes());

            return new ArbitraryDistributionTaskSource(shuffleService, getInputsForRemoteSources(fragment.getRemoteSourceNodes(), shuffleInputs), targetPartitionSize);
        }

        public ArbitraryDistributionTaskSource(ShuffleService shuffleService, Multimap<PlanNodeId, ShufflePartitionHandle> shuffleInputs, DataSize targetPartitionSize)
        {
            this.shuffleService = requireNonNull(shuffleService, "shuffleService is null");
            this.shuffleInputs = ImmutableListMultimap.copyOf(requireNonNull(shuffleInputs, "shuffleInputs is null"));
            this.targetPartitionSizeInBytes = requireNonNull(targetPartitionSize, "targetPartitionSize is null").toBytes();
        }

        @Override
        public List<BatchTask> getMoreTasks()
        {
            ImmutableList.Builder<BatchTask> result = ImmutableList.builder();
            int currentPartitionId = 0;

            ImmutableListMultimap.Builder<PlanNodeId, ShufflePartitionHandle> assignedShuffleInputs = ImmutableListMultimap.builder();
            long assignedShuffleInputSize = 0;

            for (Map.Entry<PlanNodeId, ShufflePartitionHandle> input : shuffleInputs.entries()) {
                PlanNodeId planNodeId = input.getKey();
                ShufflePartitionHandle originalPartitionHandle = input.getValue();
                checkArgument(originalPartitionHandle.getPartitionId() == 0, "unexpected partition id: %s", originalPartitionHandle.getPartitionId());

                List<ShufflePartitionHandle> partitionHandles;
                if (originalPartitionHandle.getPartitionDataSizeInBytes() > targetPartitionSizeInBytes) {
                    partitionHandles = shuffleService.splitPartition(originalPartitionHandle, targetPartitionSizeInBytes);
                }
                else {
                    partitionHandles = ImmutableList.of(originalPartitionHandle);
                }

                for (ShufflePartitionHandle handle : partitionHandles) {
                    if (assignedShuffleInputSize != 0 && assignedShuffleInputSize + handle.getPartitionDataSizeInBytes() > targetPartitionSizeInBytes) {
                        result.add(new BatchTask(currentPartitionId++, ImmutableListMultimap.of(), assignedShuffleInputs.build(), Optional.empty()));
                        assignedShuffleInputs = ImmutableListMultimap.builder();
                        assignedShuffleInputSize = 0;
                    }

                    assignedShuffleInputs.put(planNodeId, handle);
                    assignedShuffleInputSize += handle.getPartitionDataSizeInBytes();
                }
            }

            if (assignedShuffleInputSize != 0) {
                result.add(new BatchTask(currentPartitionId, ImmutableListMultimap.of(), assignedShuffleInputs.build(), Optional.empty()));
            }

            finished = true;
            return result.build();
        }

        @Override
        public boolean isFinished()
        {
            return finished;
        }

        @Override
        public Optional<CatalogName> getTaskCatalog()
        {
            return Optional.empty();
        }

        @Override
        public void close()
        {
        }
    }

    public static class HashDistributionTaskSource
            implements BatchTaskSource
    {
        private final Map<PlanNodeId, SplitSource> splitSources;
        private final Multimap<PlanNodeId, ShufflePartitionHandle> partitionedShuffleInputs;
        private final Multimap<PlanNodeId, ShufflePartitionHandle> broadcastShuffleInputs;
        private final int splitBatchSize;
        private final LongConsumer getSplitTimeRecorder;
        private final Optional<int[]> bucketToPartitionMap;
        private final Optional<BucketNodeMap> bucketNodeMap;
        private final Optional<CatalogName> catalog;

        private boolean finished;
        private boolean closed;

        public static HashDistributionTaskSource create(
                Session session,
                PlanFragment fragment,
                SplitSourceFactory splitSourceFactory,
                Multimap<PlanFragmentId, ShufflePartitionHandle> shuffleInputs,
                int splitBatchSize,
                LongConsumer getSplitTimeRecorder,
                Optional<int[]> bucketToPartitionMap,
                Optional<BucketNodeMap> bucketNodeMap)
        {
            checkArgument(bucketNodeMap.isPresent() || fragment.getPartitionedSources().isEmpty(), "bucketNodeMap is expected to be set");
            Map<PlanNodeId, SplitSource> splitSources = splitSourceFactory.createSplitSources(session, fragment);
            return new HashDistributionTaskSource(
                    splitSources,
                    getPartitionedInputs(fragment, shuffleInputs),
                    getBroadcastInputs(fragment, shuffleInputs),
                    splitBatchSize,
                    getSplitTimeRecorder,
                    bucketToPartitionMap,
                    bucketNodeMap,
                    fragment.getPartitioning().getConnectorId());
        }

        public HashDistributionTaskSource(
                Map<PlanNodeId, SplitSource> splitSources,
                Multimap<PlanNodeId, ShufflePartitionHandle> partitionedShuffleInputs,
                Multimap<PlanNodeId, ShufflePartitionHandle> broadcastShuffleInputs,
                int splitBatchSize,
                LongConsumer getSplitTimeRecorder,
                Optional<int[]> bucketToPartitionMap,
                Optional<BucketNodeMap> bucketNodeMap,
                Optional<CatalogName> catalog)
        {
            this.splitSources = ImmutableMap.copyOf(requireNonNull(splitSources, "splitSources is null"));
            this.partitionedShuffleInputs = ImmutableListMultimap.copyOf(requireNonNull(partitionedShuffleInputs, "partitionedShuffleInputs is null"));
            this.broadcastShuffleInputs = ImmutableListMultimap.copyOf(requireNonNull(broadcastShuffleInputs, "broadcastShuffleInputs is null"));
            this.splitBatchSize = splitBatchSize;
            this.getSplitTimeRecorder = requireNonNull(getSplitTimeRecorder, "getSplitTimeRecorder is null");
            this.bucketToPartitionMap = requireNonNull(bucketToPartitionMap, "bucketToPartitionMap is null");
            this.bucketNodeMap = requireNonNull(bucketNodeMap, "bucketNodeMap is null");
            checkArgument(splitSources.isEmpty() || bucketNodeMap.isPresent(), "splitToBucketFunction must be set when split sources are present");
            this.catalog = requireNonNull(catalog, "catalog is null");
        }

        @Override
        public List<BatchTask> getMoreTasks()
        {
            if (finished || closed) {
                return ImmutableList.of();
            }

            Map<Integer, Multimap<PlanNodeId, Split>> partitionToSplitsMap = new HashMap<>();
            Map<Integer, HostAddress> partitionToNodeMap = new HashMap<>();
            if (!splitSources.isEmpty()) {
                for (Map.Entry<PlanNodeId, SplitSource> entry : splitSources.entrySet()) {
                    PlanNodeId scanNodeId = entry.getKey();
                    SplitSource splitSource = entry.getValue();
                    BucketNodeMap bucketNodeMap = this.bucketNodeMap
                            .orElseThrow(() -> new VerifyException("bucket to node map is expected to be present"));
                    while (!splitSource.isFinished()) {
                        ListenableFuture<SplitBatch> splitBatchFuture = splitSource.getNextBatch(NOT_PARTITIONED, Lifespan.taskWide(), splitBatchSize);

                        long start = System.nanoTime();
                        addSuccessCallback(splitBatchFuture, () -> getSplitTimeRecorder.accept(start));

                        SplitBatch splitBatch = getFutureValue(splitBatchFuture);

                        for (Split split : splitBatch.getSplits()) {
                            int bucket = bucketNodeMap.getBucket(split);
                            int partition;
                            if (bucketToPartitionMap.isPresent()) {
                                partition = bucketToPartitionMap.get()[bucket];
                            }
                            else {
                                verify(partitionedShuffleInputs.isEmpty(), "bucket to partition map is expected to be set when partitioned inputs are present");
                                partition = bucket;
                            }

                            if (!bucketNodeMap.isDynamic()) {
                                partitionToNodeMap.put(partition, bucketNodeMap.getAssignedNode(split).get().getHostAndPort());
                            }

                            Multimap<PlanNodeId, Split> partitionSplits = partitionToSplitsMap.computeIfAbsent(partition, (p) -> ArrayListMultimap.create());
                            partitionSplits.put(scanNodeId, split);
                        }

                        if (splitBatch.isLastBatch()) {
                            splitSource.close();
                            break;
                        }
                    }
                }
            }

            Map<Integer, Multimap<PlanNodeId, ShufflePartitionHandle>> partitionToShuffleInputsMap = new HashMap<>();
            for (Map.Entry<PlanNodeId, ShufflePartitionHandle> entry : partitionedShuffleInputs.entries()) {
                PlanNodeId planNodeId = entry.getKey();
                ShufflePartitionHandle partitionHandle = entry.getValue();
                int partition = partitionHandle.getPartitionId();
                Multimap<PlanNodeId, ShufflePartitionHandle> partitionInputs = partitionToShuffleInputsMap.computeIfAbsent(partition, (p) -> ArrayListMultimap.create());
                partitionInputs.put(planNodeId, partitionHandle);
            }

            // TODO implement small tasks merging

            int taskPartitionId = 0;
            ImmutableList.Builder<BatchTask> result = ImmutableList.builder();
            for (Integer partition : union(partitionToSplitsMap.keySet(), partitionToShuffleInputsMap.keySet())) {
                Multimap<PlanNodeId, Split> splits = partitionToSplitsMap.getOrDefault(partition, ImmutableMultimap.of());
                Multimap<PlanNodeId, ShufflePartitionHandle> inputs = ImmutableListMultimap.<PlanNodeId, ShufflePartitionHandle>builder()
                        .putAll(partitionToShuffleInputsMap.getOrDefault(partition, ImmutableMultimap.of()))
                        .putAll(broadcastShuffleInputs)
                        .build();
                Optional<Set<HostAddress>> hostRequirement = Optional.ofNullable(partitionToNodeMap.get(partition)).map(ImmutableSet::of);
                result.add(new BatchTask(taskPartitionId++, splits, inputs, hostRequirement));
            }

            finished = true;
            return result.build();
        }

        @Override
        public boolean isFinished()
        {
            return finished;
        }

        @Override
        public Optional<CatalogName> getTaskCatalog()
        {
            return catalog;
        }

        @Override
        public void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            for (SplitSource splitSource : splitSources.values()) {
                try {
                    splitSource.close();
                }
                catch (RuntimeException e) {
                    log.error(e, "Error closing split source");
                }
            }
        }
    }

    public static class SourceDistributionTaskSource
            implements BatchTaskSource
    {
        private final QueryId queryId;
        private final PlanNodeId partitionedSourceNodeId;
        private final TableExecuteContextManager tableExecuteContextManager;
        private final SplitSource splitSource;
        private final Multimap<PlanNodeId, ShufflePartitionHandle> broadcastInputs;
        private final int splitBatchSize;
        private final LongConsumer getSplitTimeRecorder;
        private final Optional<CatalogName> catalogName;
        private final int targetPartitionSplitCount;

        private final Queue<Split> remotelyAccessibleSplitBuffer = new ArrayDeque<>();
        private final Map<HostAddress, Set<Split>> locallyAccessibleSplitBuffer = new HashMap<>();

        private int currentPartitionId;
        private boolean finished;
        private boolean closed;

        public static SourceDistributionTaskSource create(
                Session session,
                PlanFragment fragment,
                Multimap<PlanFragmentId, ShufflePartitionHandle> shuffleInputs,
                SplitSourceFactory splitSourceFactory,
                TableExecuteContextManager tableExecuteContextManager,
                int splitBatchSize,
                LongConsumer getSplitTimeRecorder,
                int targetPartitionSplitCount)
        {
            checkArgument(fragment.getPartitionedSources().size() == 1, "single partitioned source is expected, got: %s", fragment.getPartitionedSources());

            List<RemoteSourceNode> remoteSourceNodes = fragment.getRemoteSourceNodes();
            checkArgument(remoteSourceNodes.stream().allMatch(node -> node.getExchangeType() == REPLICATE), "only replicated exchanges are expected in source distributed stage, got: %s", remoteSourceNodes);

            PlanNodeId partitionedSourceNodeId = getOnlyElement(fragment.getPartitionedSources());
            Map<PlanNodeId, SplitSource> splitSources = splitSourceFactory.createSplitSources(session, fragment);
            SplitSource splitSource = splitSources.get(partitionedSourceNodeId);

            Optional<CatalogName> catalogName = Optional.of(splitSource.getCatalogName())
                    .filter(catalog -> !isInternalSystemConnector(catalog));

            return new SourceDistributionTaskSource(
                    session.getQueryId(),
                    partitionedSourceNodeId,
                    tableExecuteContextManager,
                    splitSource,
                    getBroadcastInputs(fragment, shuffleInputs),
                    splitBatchSize,
                    getSplitTimeRecorder,
                    catalogName,
                    targetPartitionSplitCount);
        }

        public SourceDistributionTaskSource(
                QueryId queryId,
                PlanNodeId partitionedSourceNodeId,
                TableExecuteContextManager tableExecuteContextManager,
                SplitSource splitSource,
                Multimap<PlanNodeId, ShufflePartitionHandle> broadcastInputs,
                int splitBatchSize,
                LongConsumer getSplitTimeRecorder,
                Optional<CatalogName> catalogName,
                int targetPartitionSplitCount)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.partitionedSourceNodeId = requireNonNull(partitionedSourceNodeId, "partitionedSourceNodeId is null");
            this.tableExecuteContextManager = requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");
            this.splitSource = requireNonNull(splitSource, "splitSource is null");
            this.broadcastInputs = ImmutableListMultimap.copyOf(requireNonNull(broadcastInputs, "broadcastInputs is null"));
            this.splitBatchSize = splitBatchSize;
            this.getSplitTimeRecorder = requireNonNull(getSplitTimeRecorder, "getSplitTimeRecorder is null");
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            checkArgument(targetPartitionSplitCount > 0, "targetPartitionSplitCount must be positive: %s", targetPartitionSplitCount);
            this.targetPartitionSplitCount = targetPartitionSplitCount;
        }

        @Override
        public List<BatchTask> getMoreTasks()
        {
            if (finished || closed) {
                return ImmutableList.of();
            }

            while (true) {
                if (remotelyAccessibleSplitBuffer.size() >= targetPartitionSplitCount) {
                    ImmutableList.Builder<Split> splits = ImmutableList.builder();
                    for (int i = 0; i < targetPartitionSplitCount; i++) {
                        splits.add(remotelyAccessibleSplitBuffer.poll());
                    }
                    return ImmutableList.of(
                            new BatchTask(
                                    currentPartitionId++,
                                    ImmutableListMultimap.<PlanNodeId, Split>builder().putAll(partitionedSourceNodeId, splits.build()).build(),
                                    broadcastInputs,
                                    Optional.empty()));
                }
                for (HostAddress remoteHost : locallyAccessibleSplitBuffer.keySet()) {
                    Set<Split> hostSplits = locallyAccessibleSplitBuffer.get(remoteHost);
                    if (hostSplits.size() >= targetPartitionSplitCount) {
                        List<Split> splits = removeN(hostSplits, targetPartitionSplitCount);
                        locallyAccessibleSplitBuffer.values().forEach(values -> splits.forEach(values::remove));
                        return ImmutableList.of(
                                new BatchTask(
                                        currentPartitionId++,
                                        ImmutableListMultimap.<PlanNodeId, Split>builder().putAll(partitionedSourceNodeId, splits).build(),
                                        broadcastInputs,
                                        Optional.of(ImmutableSet.of(remoteHost))));
                    }
                }

                if (splitSource.isFinished()) {
                    break;
                }

                ListenableFuture<SplitBatch> splitBatchFuture = splitSource.getNextBatch(NOT_PARTITIONED, Lifespan.taskWide(), splitBatchSize);

                long start = System.nanoTime();
                addSuccessCallback(splitBatchFuture, () -> getSplitTimeRecorder.accept(start));

                List<Split> splits = getFutureValue(splitBatchFuture).getSplits();

                for (Split split : splits) {
                    if (split.isRemotelyAccessible()) {
                        remotelyAccessibleSplitBuffer.add(split);
                    }
                    else {
                        List<HostAddress> addresses = split.getAddresses();
                        checkArgument(!addresses.isEmpty(), "split is not remotely accessible but the list of addresses is empty");
                        for (HostAddress hostAddress : addresses) {
                            locallyAccessibleSplitBuffer.computeIfAbsent(hostAddress, key -> newIdentityHashSet()).add(split);
                        }
                    }
                }
            }

            Optional<List<Object>> tableExecuteSplitsInfo = splitSource.getTableExecuteSplitsInfo();

            // Here we assume that we can get non-empty tableExecuteSplitsInfo only for queries which facilitate single split source.
            tableExecuteSplitsInfo.ifPresent(info -> {
                TableExecuteContext tableExecuteContext = tableExecuteContextManager.getTableExecuteContextForQuery(queryId);
                tableExecuteContext.setSplitsInfo(info);
            });

            finished = true;
            splitSource.close();

            ImmutableList.Builder<BatchTask> result = ImmutableList.builder();

            if (!remotelyAccessibleSplitBuffer.isEmpty()) {
                result.add(new BatchTask(
                        currentPartitionId++,
                        ImmutableListMultimap.<PlanNodeId, Split>builder().putAll(partitionedSourceNodeId, ImmutableList.copyOf(remotelyAccessibleSplitBuffer)).build(),
                        broadcastInputs,
                        Optional.empty()));
                remotelyAccessibleSplitBuffer.clear();
            }

            if (!locallyAccessibleSplitBuffer.isEmpty()) {
                for (HostAddress remoteHost : locallyAccessibleSplitBuffer.keySet()) {
                    List<Split> splits = ImmutableList.copyOf(locallyAccessibleSplitBuffer.get(remoteHost));
                    if (!splits.isEmpty()) {
                        locallyAccessibleSplitBuffer.values().forEach(values -> splits.forEach(values::remove));
                        result.add(new BatchTask(
                                currentPartitionId++,
                                ImmutableListMultimap.<PlanNodeId, Split>builder().putAll(partitionedSourceNodeId, splits).build(),
                                broadcastInputs,
                                Optional.of(ImmutableSet.of(remoteHost))));
                    }
                }
                locallyAccessibleSplitBuffer.clear();
            }

            return result.build();
        }

        private static <T> List<T> removeN(Collection<T> collection, int n)
        {
            ImmutableList.Builder<T> result = ImmutableList.builder();
            Iterator<T> iterator = collection.iterator();
            for (int i = 0; i < n && iterator.hasNext(); i++) {
                T item = iterator.next();
                iterator.remove();
                result.add(item);
            }
            return result.build();
        }

        @Override
        public boolean isFinished()
        {
            return finished;
        }

        @Override
        public Optional<CatalogName> getTaskCatalog()
        {
            return catalogName;
        }

        @Override
        public void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            splitSource.close();
        }
    }

    private static Multimap<PlanNodeId, ShufflePartitionHandle> getBroadcastInputs(PlanFragment fragment, Multimap<PlanFragmentId, ShufflePartitionHandle> shuffleInputs)
    {
        return getInputsForRemoteSources(
                fragment.getRemoteSourceNodes().stream()
                        .filter(remoteSource -> remoteSource.getExchangeType() == REPLICATE)
                        .collect(toImmutableList()),
                shuffleInputs);
    }

    private static Multimap<PlanNodeId, ShufflePartitionHandle> getPartitionedInputs(PlanFragment fragment, Multimap<PlanFragmentId, ShufflePartitionHandle> shuffleInputs)
    {
        return getInputsForRemoteSources(
                fragment.getRemoteSourceNodes().stream()
                        .filter(remoteSource -> remoteSource.getExchangeType() != REPLICATE)
                        .collect(toImmutableList()),
                shuffleInputs);
    }

    private static Multimap<PlanNodeId, ShufflePartitionHandle> getInputsForRemoteSources(
            List<RemoteSourceNode> remoteSources,
            Multimap<PlanFragmentId, ShufflePartitionHandle> shuffleInputs)
    {
        ImmutableListMultimap.Builder<PlanNodeId, ShufflePartitionHandle> result = ImmutableListMultimap.builder();
        for (RemoteSourceNode remoteSource : remoteSources) {
            for (PlanFragmentId fragmentId : remoteSource.getSourceFragmentIds()) {
                Collection<ShufflePartitionHandle> shufflePartitionHandles = requireNonNull(shuffleInputs.get(fragmentId), () -> "shuffle input is missing for fragment: " + fragmentId);
                if (remoteSource.getExchangeType() == GATHER || remoteSource.getExchangeType() == REPLICATE) {
                    checkArgument(shufflePartitionHandles.size() <= 1, "at most 1 partition handle is expected, got: %s", shufflePartitionHandles);
                }
                result.putAll(remoteSource.getId(), shufflePartitionHandles);
            }
        }
        return result.build();
    }
}
