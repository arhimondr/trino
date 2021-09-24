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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.buffer.PageCodecMarker;
import io.trino.execution.buffer.SerializedPage;
import io.trino.spi.QueryId;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestStreamingExchangeClientBuffer
{
    private List<TaskId> tasks;
    private List<SerializedPage> pages;

    @BeforeClass
    public void beforeClass()
    {
        StageId stageId = new StageId(new QueryId("query"), 0);
        tasks = ImmutableList.of(new TaskId(stageId, 0, 0), new TaskId(stageId, 1, 0));
        pages = ImmutableList.of(createPage("page1"), createPage("page-2"), createPage("page-_3"));
    }

    @AfterClass(alwaysRun = true)
    public void afterClass()
            throws InterruptedException
    {
        pages = null;
        tasks = null;
    }

    @Test
    public void testHappyPath()
    {
        try (StreamingExchangeClientBuffer buffer = new StreamingExchangeClientBuffer(directExecutor(), DataSize.of(1, KILOBYTE))) {
            assertFalse(buffer.isFinished());
            assertFalse(buffer.isBlocked().isDone());
            assertNull(buffer.pollPage());

            buffer.addTask(tasks.get(0));
            assertFalse(buffer.isFinished());
            assertFalse(buffer.isBlocked().isDone());
            assertNull(buffer.pollPage());

            buffer.addTask(tasks.get(1));
            assertFalse(buffer.isFinished());
            assertFalse(buffer.isBlocked().isDone());
            assertNull(buffer.pollPage());

            buffer.noMoreTasks();
            assertFalse(buffer.isFinished());
            assertFalse(buffer.isBlocked().isDone());
            assertNull(buffer.pollPage());

            buffer.addPages(tasks.get(0), ImmutableList.of(pages.get(0)));
            assertEquals(buffer.getBufferedPageCount(), 1);
            assertEquals(buffer.getRetainedSizeInBytes(), pages.get(0).getRetainedSizeInBytes());
            assertEquals(buffer.getMaxRetainedSizeInBytes(), pages.get(0).getRetainedSizeInBytes());
            assertEquals(buffer.getRemainingBufferCapacityInBytes(), DataSize.of(1, KILOBYTE).toBytes() - pages.get(0).getRetainedSizeInBytes());
            assertFalse(buffer.isFinished());
            assertTrue(buffer.isBlocked().isDone());
            assertPageEquals(buffer.pollPage(), pages.get(0));
            assertEquals(buffer.getRetainedSizeInBytes(), 0);
            assertEquals(buffer.getMaxRetainedSizeInBytes(), pages.get(0).getRetainedSizeInBytes());
            assertEquals(buffer.getRemainingBufferCapacityInBytes(), DataSize.of(1, KILOBYTE).toBytes());
            assertFalse(buffer.isFinished());
            assertFalse(buffer.isBlocked().isDone());

            buffer.taskFinished(tasks.get(0));
            assertFalse(buffer.isFinished());
            assertFalse(buffer.isBlocked().isDone());

            buffer.addPages(tasks.get(1), ImmutableList.of(pages.get(1), pages.get(2)));
            assertEquals(buffer.getBufferedPageCount(), 2);
            assertEquals(buffer.getRetainedSizeInBytes(), pages.get(1).getRetainedSizeInBytes() + pages.get(2).getRetainedSizeInBytes());
            assertEquals(buffer.getMaxRetainedSizeInBytes(), pages.get(1).getRetainedSizeInBytes() + pages.get(2).getRetainedSizeInBytes());
            assertEquals(buffer.getRemainingBufferCapacityInBytes(), DataSize.of(1, KILOBYTE).toBytes() - pages.get(1).getRetainedSizeInBytes() - pages.get(2).getRetainedSizeInBytes());
            assertFalse(buffer.isFinished());
            assertTrue(buffer.isBlocked().isDone());
            assertPageEquals(buffer.pollPage(), pages.get(1));
            assertPageEquals(buffer.pollPage(), pages.get(2));
            assertFalse(buffer.isFinished());
            assertFalse(buffer.isBlocked().isDone());
            assertEquals(buffer.getRetainedSizeInBytes(), 0);
            assertEquals(buffer.getMaxRetainedSizeInBytes(), pages.get(1).getRetainedSizeInBytes() + pages.get(2).getRetainedSizeInBytes());
            assertEquals(buffer.getRemainingBufferCapacityInBytes(), DataSize.of(1, KILOBYTE).toBytes());

            buffer.taskFinished(tasks.get(1));
            assertTrue(buffer.isFinished());
            assertTrue(buffer.isBlocked().isDone());
        }
    }

    @Test
    public void testClose()
    {
        StreamingExchangeClientBuffer buffer = new StreamingExchangeClientBuffer(directExecutor(), DataSize.of(1, KILOBYTE));
        buffer.addTask(tasks.get(0));
        buffer.addTask(tasks.get(1));

        assertFalse(buffer.isFinished());
        assertFalse(buffer.isBlocked().isDone());
        assertNull(buffer.pollPage());

        buffer.close();

        assertTrue(buffer.isFinished());
        assertTrue(buffer.isBlocked().isDone());
        assertNull(buffer.pollPage());
    }

    @Test
    public void testIsFinished()
    {
        try (StreamingExchangeClientBuffer buffer = new StreamingExchangeClientBuffer(directExecutor(), DataSize.of(1, KILOBYTE))) {
            assertFalse(buffer.isFinished());
            assertFalse(buffer.isBlocked().isDone());

            buffer.noMoreTasks();

            assertTrue(buffer.isFinished());
            assertTrue(buffer.isBlocked().isDone());
        }

        try (StreamingExchangeClientBuffer buffer = new StreamingExchangeClientBuffer(directExecutor(), DataSize.of(1, KILOBYTE))) {
            assertFalse(buffer.isFinished());
            assertFalse(buffer.isBlocked().isDone());

            buffer.addTask(tasks.get(0));
            buffer.noMoreTasks();

            assertFalse(buffer.isFinished());
            assertFalse(buffer.isBlocked().isDone());

            buffer.taskFinished(tasks.get(0));

            assertTrue(buffer.isFinished());
            assertTrue(buffer.isBlocked().isDone());
        }

        try (StreamingExchangeClientBuffer buffer = new StreamingExchangeClientBuffer(directExecutor(), DataSize.of(1, KILOBYTE))) {
            assertFalse(buffer.isFinished());
            assertFalse(buffer.isBlocked().isDone());

            buffer.addTask(tasks.get(0));

            assertFalse(buffer.isFinished());
            assertFalse(buffer.isBlocked().isDone());

            RuntimeException error = new RuntimeException();
            buffer.taskFailed(tasks.get(0), error);

            assertTrue(buffer.isFinished());
            assertTrue(buffer.isBlocked().isDone());
            assertThatThrownBy(buffer::pollPage).isEqualTo(error);
        }
    }

    @Test
    public void testMultipleBlockedFutures()
    {
        try (StreamingExchangeClientBuffer buffer = new StreamingExchangeClientBuffer(directExecutor(), DataSize.of(1, KILOBYTE))) {
            assertFalse(buffer.isFinished());

            ListenableFuture<Void> blocked1 = buffer.isBlocked();
            ListenableFuture<Void> blocked2 = buffer.isBlocked();
            ListenableFuture<Void> blocked3 = buffer.isBlocked();

            assertFalse(blocked1.isDone());
            assertFalse(blocked2.isDone());
            assertFalse(blocked3.isDone());

            blocked3.cancel(true);
            assertFalse(blocked1.isDone());
            assertFalse(blocked2.isDone());

            buffer.noMoreTasks();

            assertTrue(buffer.isFinished());
            assertTrue(blocked1.isDone());
            assertTrue(blocked2.isDone());
        }
    }

    private static SerializedPage createPage(String value)
    {
        return new SerializedPage(utf8Slice(value), PageCodecMarker.MarkerSet.empty(), 1, value.length());
    }

    private static void assertPageEquals(SerializedPage actual, SerializedPage expected)
    {
        assertEquals(actual.getPositionCount(), expected.getPositionCount());
        assertEquals(actual.getUncompressedSizeInBytes(), expected.getUncompressedSizeInBytes());
        assertEquals(actual.getPageCodecMarkers(), expected.getPageCodecMarkers());
        assertEquals(actual.getSlice(), expected.getSlice());
    }
}
