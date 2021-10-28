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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.shuffle.Shuffle;
import io.trino.spi.shuffle.ShuffleHandle;
import io.trino.spi.shuffle.ShuffleInput;
import io.trino.spi.shuffle.ShuffleOutput;
import io.trino.spi.shuffle.ShufflePartitionHandle;
import io.trino.spi.shuffle.ShuffleService;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestShuffleService
{
    private ShuffleService shuffleService;

    @BeforeClass
    public void init()
            throws Exception
    {
        shuffleService = createShuffleService();
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
            throws Exception
    {
        if (shuffleService != null) {
            shuffleService = null;
        }
    }

    protected abstract ShuffleService createShuffleService();

    @Test
    public void testHappyPath()
            throws Exception
    {
        Shuffle shuffle = shuffleService.create(2);
        shuffle.noMoreOutputTasks(ImmutableSet.of(0, 1, 2));

        writeData(
                shuffle.getHandle(),
                0,
                ImmutableListMultimap.of(
                        0, "0-0-0",
                        1, "0-1-0",
                        0, "0-0-1",
                        1, "0-1-1"),
                true);
        writeData(
                shuffle.getHandle(),
                0,
                ImmutableListMultimap.of(
                        0, "0-0-0",
                        1, "0-1-0",
                        0, "0-0-1",
                        1, "0-1-1"),
                true);
        writeData(
                shuffle.getHandle(),
                0,
                ImmutableListMultimap.of(
                        0, "failed",
                        1, "another failed"),
                false);
        shuffle.outputTaskFinished(0);

        writeData(
                shuffle.getHandle(),
                1,
                ImmutableListMultimap.of(
                        0, "1-0-0",
                        1, "1-1-0",
                        0, "1-0-1",
                        1, "1-1-1"),
                true);
        writeData(
                shuffle.getHandle(),
                1,
                ImmutableListMultimap.of(
                        0, "1-0-0",
                        1, "1-1-0",
                        0, "1-0-1",
                        1, "1-1-1"),
                true);
        writeData(
                shuffle.getHandle(),
                1,
                ImmutableListMultimap.of(
                        0, "more failed",
                        1, "another failed"),
                false);
        shuffle.outputTaskFinished(1);

        writeData(
                shuffle.getHandle(),
                2,
                ImmutableListMultimap.of(
                        0, "2-0-0",
                        1, "2-1-0"),
                true);
        shuffle.outputTaskFinished(2);

        CompletableFuture<List<ShufflePartitionHandle>> inputPartitionHandlesFuture = shuffle.getInputPartitionHandles();
        assertTrue(inputPartitionHandlesFuture.isDone());
        assertTrue(shuffle.isInputReady().isDone());

        List<ShufflePartitionHandle> partitionHandles = inputPartitionHandlesFuture.get();
        assertThat(partitionHandles).hasSize(2);

        Map<Integer, ShufflePartitionHandle> partitions = partitionHandles.stream()
                .collect(toImmutableMap(ShufflePartitionHandle::getPartitionId, Function.identity()));

        assertThat(readData(partitions.get(0)))
                .containsExactlyInAnyOrder("0-0-0", "0-0-1", "1-0-0", "1-0-1", "2-0-0");

        assertThat(readData(partitions.get(1)))
                .containsExactlyInAnyOrder("0-1-0", "0-1-1", "1-1-0", "1-1-1", "2-1-0");

        shuffle.close();
    }

    private void writeData(ShuffleHandle handle, int outputPartition, Multimap<Integer, String> data, boolean commit)
    {
        try (ShuffleOutput output = shuffleService.createOutput(handle, outputPartition)) {
            data.forEach((key, value) -> {
                output.add(key, Slices.utf8Slice(value));
            });
            if (commit) {
                output.commit();
            }
        }
    }

    private List<String> readData(ShufflePartitionHandle handle)
    {
        return readData(ImmutableList.of(handle));
    }

    private List<String> readData(List<ShufflePartitionHandle> handles)
    {
        ImmutableList.Builder<String> result = ImmutableList.builder();
        try (ShuffleInput input = shuffleService.createInput(handles)) {
            while (!input.isFinished()) {
                Slice data = input.pool();
                if (data != null) {
                    result.add(data.toStringUtf8());
                }
            }
        }
        return result.build();
    }
}
