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
import com.google.common.collect.ImmutableSet;
import io.trino.spi.shuffle.Shuffle;
import io.trino.spi.shuffle.ShuffleHandle;
import io.trino.spi.shuffle.ShuffleLostTaskOutputListener;
import io.trino.spi.shuffle.ShufflePartitionHandle;

import javax.annotation.concurrent.GuardedBy;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.Sets.newConcurrentHashSet;

public class TestingShuffle
        implements Shuffle
{
    private final Set<Integer> finishedTasks = newConcurrentHashSet();
    @GuardedBy("this")
    private Set<Integer> allTasks;
    private final List<ShuffleLostTaskOutputListener> lostTaskListeners = new CopyOnWriteArrayList<>();
    private final CompletableFuture<List<ShufflePartitionHandle>> inputPartitionHandles = new CompletableFuture<>();
    @GuardedBy("this")
    private CompletableFuture<?> inputReady = new CompletableFuture<>();

    @Override
    public ShuffleHandle getHandle()
    {
        return new TestingShuffleHandle();
    }

    @Override
    public void outputTaskFinished(int outputTaskPartitionId)
    {
        finishedTasks.add(outputTaskPartitionId);
    }

    public Set<Integer> getFinishedTasks()
    {
        return ImmutableSet.copyOf(finishedTasks);
    }

    @Override
    public synchronized void noMoreOutputTasks(Set<Integer> outputTaskPartitionIds)
    {
        this.allTasks = ImmutableSet.copyOf(outputTaskPartitionIds);
    }

    public synchronized Set<Integer> getAllTasks()
    {
        return allTasks;
    }

    @Override
    public void registerLostTaskOutputListener(ShuffleLostTaskOutputListener listener)
    {
        lostTaskListeners.add(listener);
    }

    public void reportLostTaskOutput(Set<Integer> partitions)
    {
        for (ShuffleLostTaskOutputListener listener : lostTaskListeners) {
            listener.outputLost(partitions);
        }
    }

    @Override
    public CompletableFuture<List<ShufflePartitionHandle>> getInputPartitionHandles()
    {
        return inputPartitionHandles;
    }

    public void setInputPartitionHandles(List<ShufflePartitionHandle> handles)
    {
        inputPartitionHandles.complete(ImmutableList.copyOf(handles));
    }

    @Override
    public synchronized CompletableFuture<?> isInputReady()
    {
        return inputReady;
    }

    public synchronized void setInputReady()
    {
        inputReady.complete(null);
    }

    public synchronized void resetInputReady()
    {
        inputReady = new CompletableFuture<>();
    }

    @Override
    public void close()
    {
    }

    public static class TestingShuffleHandle
            implements ShuffleHandle
    {
    }

    public static class TestingShufflePartitionHandle
            implements ShufflePartitionHandle
    {
        private final int partitionId;
        private final long sizeInBytes;

        public TestingShufflePartitionHandle(int partitionId, long sizeInBytes)
        {
            this.partitionId = partitionId;
            this.sizeInBytes = sizeInBytes;
        }

        @Override
        public int getPartitionId()
        {
            return partitionId;
        }

        @Override
        public long getPartitionDataSizeInBytes()
        {
            return sizeInBytes;
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
            TestingShufflePartitionHandle that = (TestingShufflePartitionHandle) o;
            return partitionId == that.partitionId && sizeInBytes == that.sizeInBytes;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(partitionId, sizeInBytes);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("partitionId", partitionId)
                    .add("sizeInBytes", sizeInBytes)
                    .toString();
        }
    }
}
