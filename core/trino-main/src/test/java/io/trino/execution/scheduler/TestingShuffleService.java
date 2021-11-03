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
import io.trino.execution.scheduler.TestingShuffle.TestingShufflePartitionHandle;
import io.trino.spi.exchange.Shuffle;
import io.trino.spi.exchange.ShuffleHandle;
import io.trino.spi.exchange.ShuffleInput;
import io.trino.spi.exchange.ShuffleOutput;
import io.trino.spi.exchange.ShufflePartitionHandle;
import io.trino.spi.exchange.ShuffleService;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterators.cycle;
import static com.google.common.collect.Iterators.limit;
import static java.lang.Math.toIntExact;

public class TestingShuffleService
        implements ShuffleService
{
    private final boolean splitPartitionsEnabled;

    public TestingShuffleService(boolean splitPartitionsEnabled)
    {
        this.splitPartitionsEnabled = splitPartitionsEnabled;
    }

    @Override
    public Shuffle create(int partitionCount)
    {
        return new TestingShuffle();
    }

    @Override
    public List<ShufflePartitionHandle> splitPartition(ShufflePartitionHandle handle, long targetSizeInBytes)
    {
        if (!splitPartitionsEnabled) {
            return ImmutableList.of(handle);
        }
        checkArgument(targetSizeInBytes > 0, "targetSizeInBytes must be positive: %s", targetSizeInBytes);
        TestingShufflePartitionHandle testingPartitionHandle = (TestingShufflePartitionHandle) handle;
        long currentSize = testingPartitionHandle.getPartitionDataSizeInBytes();
        int fullPartitions = toIntExact(currentSize / targetSizeInBytes);
        long reminder = currentSize % targetSizeInBytes;
        return ImmutableList.<ShufflePartitionHandle>builder()
                .addAll(limit(cycle(new TestingShufflePartitionHandle(testingPartitionHandle.getPartitionId(), targetSizeInBytes)), fullPartitions))
                .add(new TestingShufflePartitionHandle(testingPartitionHandle.getPartitionId(), reminder))
                .build();
    }

    @Override
    public ShuffleOutput createOutput(ShuffleHandle handle, int outputTaskPartitionId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ShuffleInput createInput(List<ShufflePartitionHandle> partitionHandles)
    {
        throw new UnsupportedOperationException();
    }
}
