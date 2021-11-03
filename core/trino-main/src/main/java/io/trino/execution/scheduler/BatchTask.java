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

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.spi.exchange.ShufflePartitionHandle;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class BatchTask
{
    private final int partitionId;
    private final Multimap<PlanNodeId, Split> splits;
    private final Multimap<PlanNodeId, ShufflePartitionHandle> shuffleInputs;
    private final Optional<Set<HostAddress>> hostsRequirement;

    public BatchTask(
            int partitionId,
            Multimap<PlanNodeId, Split> splits,
            Multimap<PlanNodeId, ShufflePartitionHandle> shuffleInputs,
            Optional<Set<HostAddress>> hostsRequirement)
    {
        this.partitionId = partitionId;
        this.splits = ImmutableMultimap.copyOf(requireNonNull(splits, "splits is null"));
        this.shuffleInputs = ImmutableMultimap.copyOf(requireNonNull(shuffleInputs, "shuffleInputs is null"));
        this.hostsRequirement = requireNonNull(hostsRequirement, "hostsRequirement is null").map(ImmutableSet::copyOf);
    }

    public int getPartitionId()
    {
        return partitionId;
    }

    public Multimap<PlanNodeId, Split> getSplits()
    {
        return splits;
    }

    public Multimap<PlanNodeId, ShufflePartitionHandle> getShuffleInputs()
    {
        return shuffleInputs;
    }

    public Optional<Set<HostAddress>> getHostsRequirement()
    {
        return hostsRequirement;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BatchTask task = (BatchTask) o;
        return partitionId == task.partitionId && Objects.equals(splits, task.splits) && Objects.equals(shuffleInputs, task.shuffleInputs) && Objects.equals(hostsRequirement, task.hostsRequirement);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionId, splits, shuffleInputs, hostsRequirement);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partitionId", partitionId)
                .add("splits", splits)
                .add("shuffleInputs", shuffleInputs)
                .add("hostsRequirement", hostsRequirement)
                .toString();
    }
}
