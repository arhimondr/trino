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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.metadata.Split;
import io.trino.spi.exchange.ShufflePartitionHandle;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static java.util.Objects.requireNonNull;

public class TestingBatchTaskSourceFactory
        implements BatchTaskSourceFactory
{
    private final Optional<CatalogName> catalog;
    private final List<Split> splits;
    private final int tasksPerBatch;

    public TestingBatchTaskSourceFactory(Optional<CatalogName> catalog, List<Split> splits, int tasksPerBatch)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.splits = ImmutableList.copyOf(requireNonNull(splits, "splits is null"));
        this.tasksPerBatch = tasksPerBatch;
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
        List<PlanNodeId> partitionedSources = fragment.getPartitionedSources();
        checkArgument(partitionedSources.size() == 1, "single partitioned source is expected");

        return new TestingBatchTaskSource(
                catalog,
                splits,
                tasksPerBatch,
                getOnlyElement(partitionedSources),
                getInputsForRemoteSources(fragment.getRemoteSourceNodes(), shuffleInputs));
    }

    private static Multimap<PlanNodeId, ShufflePartitionHandle> getInputsForRemoteSources(
            List<RemoteSourceNode> remoteSources,
            Multimap<PlanFragmentId, ShufflePartitionHandle> shuffleInputs)
    {
        ImmutableListMultimap.Builder<PlanNodeId, ShufflePartitionHandle> result = ImmutableListMultimap.builder();
        for (RemoteSourceNode remoteSource : remoteSources) {
            checkArgument(remoteSource.getExchangeType() == REPLICATE, "expected exchange type to be REPLICATE, got: %s", remoteSource.getExchangeType());
            for (PlanFragmentId fragmentId : remoteSource.getSourceFragmentIds()) {
                Collection<ShufflePartitionHandle> shufflePartitionHandles = requireNonNull(shuffleInputs.get(fragmentId), () -> "shuffle input is missing for fragment: " + fragmentId);
                checkArgument(shufflePartitionHandles.size() == 1, "single partition handle is expected, got: %s", shufflePartitionHandles);
                result.putAll(remoteSource.getId(), shufflePartitionHandles);
            }
        }
        return result.build();
    }

    public static class TestingBatchTaskSource
            implements BatchTaskSource
    {
        private final Optional<CatalogName> catalog;
        private final Iterator<Split> splits;
        private final int tasksPerBatch;
        private final PlanNodeId tableScanPlanNodeId;
        private final Multimap<PlanNodeId, ShufflePartitionHandle> shuffleInputs;

        private final AtomicInteger nextPartitionId = new AtomicInteger();

        public TestingBatchTaskSource(
                Optional<CatalogName> catalog,
                List<Split> splits,
                int tasksPerBatch,
                PlanNodeId tableScanPlanNodeId,
                Multimap<PlanNodeId, ShufflePartitionHandle> shuffleInputs)
        {
            this.catalog = requireNonNull(catalog, "catalog is null");
            this.splits = ImmutableList.copyOf(requireNonNull(splits, "splits is null")).iterator();
            this.tasksPerBatch = tasksPerBatch;
            this.tableScanPlanNodeId = requireNonNull(tableScanPlanNodeId, "tableScanPlanNodeId is null");
            this.shuffleInputs = ImmutableListMultimap.copyOf(requireNonNull(shuffleInputs, "shuffleInputs is null"));
        }

        @Override
        public List<BatchTask> getMoreTasks()
        {
            checkState(!isFinished(), "already finished");

            ImmutableList.Builder<BatchTask> result = ImmutableList.builder();
            for (int i = 0; i < tasksPerBatch; i++) {
                if (isFinished()) {
                    break;
                }
                Split split = splits.next();
                BatchTask task = new BatchTask(
                        nextPartitionId.getAndIncrement(),
                        ImmutableListMultimap.of(tableScanPlanNodeId, split),
                        shuffleInputs,
                        Optional.empty());
                result.add(task);
            }

            return result.build();
        }

        @Override
        public boolean isFinished()
        {
            return !splits.hasNext();
        }

        @Override
        public Optional<CatalogName> getTaskCatalog()
        {
            return catalog;
        }

        @Override
        public void close()
        {
        }
    }
}
