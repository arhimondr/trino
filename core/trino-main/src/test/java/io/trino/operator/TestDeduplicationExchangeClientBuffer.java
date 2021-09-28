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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.buffer.PageCodecMarker;
import io.trino.execution.buffer.SerializedPage;
import io.trino.spi.TrinoException;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestDeduplicationExchangeClientBuffer
{
    private static final DataSize ONE_KB = DataSize.of(1, KILOBYTE);

    @Test
    public void testIsBlocked()
    {
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            ListenableFuture<Void> blocked = buffer.isBlocked();
            assertFalse(blocked.isDone());
            buffer.close();
            assertTrue(blocked.isDone());
        }

        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            ListenableFuture<Void> blocked = buffer.isBlocked();
            assertFalse(blocked.isDone());
            buffer.noMoreTasks();
            assertTrue(blocked.isDone());
        }

        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            ListenableFuture<Void> blocked = buffer.isBlocked();
            assertFalse(blocked.isDone());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertFalse(blocked.isDone());

            buffer.taskFinished(taskId);
            assertFalse(blocked.isDone());

            buffer.noMoreTasks();
            assertTrue(blocked.isDone());
        }

        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            ListenableFuture<Void> blocked = buffer.isBlocked();
            assertFalse(blocked.isDone());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertFalse(blocked.isDone());

            buffer.noMoreTasks();
            assertFalse(blocked.isDone());

            buffer.taskFinished(taskId);
            assertTrue(blocked.isDone());
        }

        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            ListenableFuture<Void> blocked = buffer.isBlocked();
            assertFalse(blocked.isDone());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertFalse(blocked.isDone());

            buffer.taskFailed(taskId, new RuntimeException());
            assertFalse(blocked.isDone());

            buffer.noMoreTasks();
            assertTrue(blocked.isDone());
        }

        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            ListenableFuture<Void> blocked = buffer.isBlocked();
            assertFalse(blocked.isDone());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertFalse(blocked.isDone());

            buffer.noMoreTasks();
            assertFalse(blocked.isDone());

            buffer.taskFailed(taskId, new RuntimeException());
            assertTrue(blocked.isDone());
        }
    }

    @Test
    public void testPollPage()
    {
        testPollPages(ImmutableListMultimap.of(), ImmutableMap.of(), ImmutableList.of());
        testPollPages(
                ImmutableListMultimap.of(
                        createTaskId(0, 0),
                        createPage("p0a0v0")),
                ImmutableMap.of(),
                ImmutableList.of("p0a0v0"));
        testPollPages(
                ImmutableListMultimap.of(
                        createTaskId(0, 0),
                        createPage("p0a0v0"),
                        createTaskId(0, 1),
                        createPage("p0a1v0")),
                ImmutableMap.of(),
                ImmutableList.of("p0a1v0"));
        testPollPages(
                ImmutableListMultimap.of(
                        createTaskId(0, 0),
                        createPage("p0a0v0"),
                        createTaskId(1, 0),
                        createPage("p1a0v0"),
                        createTaskId(0, 1),
                        createPage("p0a1v0")),
                ImmutableMap.of(),
                ImmutableList.of("p0a1v0"));
        testPollPages(
                ImmutableListMultimap.of(
                        createTaskId(0, 0),
                        createPage("p0a0v0"),
                        createTaskId(1, 0),
                        createPage("p1a0v0"),
                        createTaskId(0, 1),
                        createPage("p0a1v0")),
                ImmutableMap.of(
                        createTaskId(2, 0),
                        new RuntimeException("error")),
                ImmutableList.of("p0a1v0"));
        RuntimeException error = new RuntimeException("error");
        assertThatThrownBy(() -> testPollPages(
                ImmutableListMultimap.of(
                        createTaskId(0, 0),
                        createPage("p0a0v0"),
                        createTaskId(1, 0),
                        createPage("p1a0v0"),
                        createTaskId(0, 1),
                        createPage("p0a1v0")),
                ImmutableMap.of(
                        createTaskId(2, 2),
                        error),
                ImmutableList.of("p0a1v0"))).isEqualTo(error);
        assertThatThrownBy(() -> testPollPages(
                ImmutableListMultimap.of(
                        createTaskId(0, 0),
                        createPage("p0a0v0"),
                        createTaskId(1, 0),
                        createPage("p1a0v0"),
                        createTaskId(0, 1),
                        createPage("p0a1v0")),
                ImmutableMap.of(
                        createTaskId(0, 1),
                        error),
                ImmutableList.of("p0a1v0"))).isEqualTo(error);
    }

    private void testPollPages(Multimap<TaskId, SerializedPage> pages, Map<TaskId, RuntimeException> failures, List<String> expectedValues)
    {
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            for (TaskId taskId : Sets.union(pages.keySet(), failures.keySet())) {
                buffer.addTask(taskId);
            }
            for (Map.Entry<TaskId, SerializedPage> page : pages.entries()) {
                buffer.addPages(page.getKey(), ImmutableList.of(page.getValue()));
            }
            for (Map.Entry<TaskId, RuntimeException> failure : failures.entrySet()) {
                buffer.taskFailed(failure.getKey(), failure.getValue());
            }
            for (TaskId taskId : Sets.difference(pages.keySet(), failures.keySet())) {
                buffer.taskFinished(taskId);
            }
            buffer.noMoreTasks();
            assertContainsPages(buffer, expectedValues);
        }
    }

    private static void assertContainsPages(ExchangeClientBuffer buffer, List<String> values)
    {
        List<String> actual = new ArrayList<>();
        while (true) {
            SerializedPage page = buffer.pollPage();
            if (page == null) {
                break;
            }
            actual.add(page.getSlice().toStringUtf8());
        }
        assertTrue(buffer.isFinished());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(values);
    }

    @Test
    public void testRemovePagesForPreviousAttempts()
    {
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            assertEquals(buffer.getRetainedSizeInBytes(), 0);

            TaskId task1 = createTaskId(0, 0);
            TaskId task2 = createTaskId(1, 0);
            TaskId task3 = createTaskId(0, 1);

            SerializedPage page1 = createPage("textofrandomlength");
            SerializedPage page2 = createPage("textwithdifferentlength");
            SerializedPage page3 = createPage("smalltext");

            buffer.addTask(task1);
            buffer.addPages(task1, ImmutableList.of(page1));
            buffer.addTask(task2);
            buffer.addPages(task2, ImmutableList.of(page2));

            assertEquals(buffer.getRetainedSizeInBytes(), page1.getRetainedSizeInBytes() + page2.getRetainedSizeInBytes());

            buffer.addTask(task3);
            assertEquals(buffer.getRetainedSizeInBytes(), 0);

            buffer.addPages(task3, ImmutableList.of(page3));
            assertEquals(buffer.getRetainedSizeInBytes(), page3.getRetainedSizeInBytes());
        }
    }

    @Test
    public void testBufferOverflow()
    {
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), DataSize.of(100, BYTE), RetryPolicy.QUERY)) {
            TaskId task = createTaskId(0, 0);

            buffer.addTask(task);
            SerializedPage page1 = createPage("1234");
            buffer.addPages(task, ImmutableList.of(page1));

            assertFalse(buffer.isFinished());
            assertFalse(buffer.isBlocked().isDone());
            assertEquals(buffer.getRetainedSizeInBytes(), page1.getRetainedSizeInBytes());

            SerializedPage page2 = createPage("123456789");
            buffer.addPages(task, ImmutableList.of(page2));
            assertTrue(buffer.isFinished());
            assertTrue(buffer.isBlocked().isDone());
            assertEquals(buffer.getRetainedSizeInBytes(), 0);
            assertEquals(buffer.getBufferedPageCount(), 0);

            assertThatThrownBy(buffer::pollPage)
                    .isInstanceOf(TrinoException.class)
                    .hasMessage("Retries for queries with large result set currently unsupported");
        }
    }

    @Test
    public void testIsFinished()
    {
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            assertFalse(buffer.isFinished());
            buffer.close();
            assertTrue(buffer.isFinished());
        }

        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            assertFalse(buffer.isFinished());
            buffer.noMoreTasks();
            assertTrue(buffer.isFinished());
        }

        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            assertFalse(buffer.isFinished());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertFalse(buffer.isFinished());

            buffer.taskFinished(taskId);
            assertFalse(buffer.isFinished());

            buffer.noMoreTasks();
            assertTrue(buffer.isFinished());
        }

        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            assertFalse(buffer.isFinished());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertFalse(buffer.isFinished());

            buffer.noMoreTasks();
            assertFalse(buffer.isFinished());

            buffer.taskFinished(taskId);
            assertTrue(buffer.isFinished());
        }

        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            assertFalse(buffer.isFinished());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertFalse(buffer.isFinished());

            buffer.taskFailed(taskId, new RuntimeException());
            assertFalse(buffer.isFinished());

            buffer.noMoreTasks();
            assertTrue(buffer.isFinished());
        }

        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            assertFalse(buffer.isFinished());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertFalse(buffer.isFinished());

            buffer.noMoreTasks();
            assertFalse(buffer.isFinished());

            buffer.taskFailed(taskId, new RuntimeException());
            assertTrue(buffer.isFinished());
        }

        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            assertFalse(buffer.isFinished());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            buffer.addPages(taskId, ImmutableList.of(createPage("page")));
            assertFalse(buffer.isFinished());

            buffer.noMoreTasks();
            assertFalse(buffer.isFinished());

            buffer.taskFailed(taskId, new RuntimeException());
            assertTrue(buffer.isFinished());
        }

        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            assertFalse(buffer.isFinished());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            buffer.addPages(taskId, ImmutableList.of(createPage("page")));
            assertFalse(buffer.isFinished());

            buffer.noMoreTasks();
            assertFalse(buffer.isFinished());

            buffer.taskFinished(taskId);
            assertFalse(buffer.isFinished());

            assertNotNull(buffer.pollPage());
            assertTrue(buffer.isFinished());
        }

        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            assertFalse(buffer.isFinished());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            buffer.addPages(taskId, ImmutableList.of(createPage("page")));
            assertFalse(buffer.isFinished());

            buffer.taskFinished(taskId);
            assertFalse(buffer.isFinished());

            buffer.noMoreTasks();
            assertFalse(buffer.isFinished());

            assertNotNull(buffer.pollPage());
            assertTrue(buffer.isFinished());
        }
    }

    @Test
    public void testRemainingBufferCapacity()
    {
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            assertFalse(buffer.isFinished());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            SerializedPage page = createPage("page");
            buffer.addPages(taskId, ImmutableList.of(page));

            assertEquals(buffer.getRemainingBufferCapacityInBytes(), ONE_KB.toBytes() - page.getRetainedSizeInBytes());
        }
    }

    private static TaskId createTaskId(int partition, int attempt)
    {
        return new TaskId(new StageId("query", 0), partition, attempt);
    }

    private static SerializedPage createPage(String value)
    {
        return new SerializedPage(utf8Slice(value), PageCodecMarker.MarkerSet.empty(), 1, value.length());
    }
}
