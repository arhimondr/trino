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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.Session;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogName;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.Lifespan;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.RemoteTaskFactory;
import io.trino.execution.SqlStageExecution;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.TaskState;
import io.trino.execution.TestingRemoteTaskFactory;
import io.trino.execution.TestingRemoteTaskFactory.TestingRemoteTask;
import io.trino.execution.scheduler.TestingNodeSelectorFactory.TestingNodeSupplier;
import io.trino.execution.scheduler.TestingShuffle.TestingShufflePartitionHandle;
import io.trino.failuredetector.NoOpFailureDetector;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.spi.QueryId;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.shuffle.Shuffle;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import io.trino.util.FinalizerService;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.operator.RetryPolicy.TASK;
import static io.trino.operator.StageExecutionDescriptor.ungroupedExecution;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TestingSplit.createRemoteSplit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestBatchTaskScheduler
{
    private static final Session SESSION = testSessionBuilder().build();

    private static final StageId STAGE_ID = new StageId(new QueryId("query"), 0);
    private static final PlanFragmentId FRAGMENT_ID = new PlanFragmentId("0");
    private static final PlanFragmentId SOURCE_FRAGMENT_ID_1 = new PlanFragmentId("1");
    private static final PlanFragmentId SOURCE_FRAGMENT_ID_2 = new PlanFragmentId("2");
    private static final PlanNodeId TABLE_SCAN_NODE_ID = new PlanNodeId("table_scan_id");

    private static final CatalogName CATALOG = new CatalogName("catalog");

    private static final InternalNode NODE_1 = new InternalNode("node-1", URI.create("local://127.0.0.1:8080"), NodeVersion.UNKNOWN, false);
    private static final InternalNode NODE_2 = new InternalNode("node-2", URI.create("local://127.0.0.1:8081"), NodeVersion.UNKNOWN, false);
    private static final InternalNode NODE_3 = new InternalNode("node-3", URI.create("local://127.0.0.1:8082"), NodeVersion.UNKNOWN, false);

    private FinalizerService finalizerService;
    private NodeTaskMap nodeTaskMap;

    @BeforeClass
    public void beforeClass()
    {
        finalizerService = new FinalizerService();
        finalizerService.start();
        nodeTaskMap = new NodeTaskMap(finalizerService);
    }

    @AfterClass(alwaysRun = true)
    public void afterClass()
    {
        nodeTaskMap = null;
        if (finalizerService != null) {
            finalizerService.destroy();
            finalizerService = null;
        }
    }

    @Test
    public void testHappyPath()
            throws Exception
    {
        TestingRemoteTaskFactory remoteTaskFactory = new TestingRemoteTaskFactory();
        TestingBatchTaskSourceFactory taskSourceFactory = createTaskSourceFactory(5, 2);
        TestingNodeSupplier nodeSupplier = TestingNodeSupplier.create(ImmutableMap.of(
                NODE_1, ImmutableList.of(CATALOG),
                NODE_2, ImmutableList.of(CATALOG),
                NODE_3, ImmutableList.of(CATALOG)));

        TestingShuffle outputShuffle = new TestingShuffle();

        TestingShuffle sourceShuffle1 = new TestingShuffle();
        TestingShuffle sourceShuffle2 = new TestingShuffle();

        BatchTaskScheduler scheduler = createBatchTaskScheduler(
                remoteTaskFactory,
                taskSourceFactory,
                createNodeAllocator(nodeSupplier),
                Optional.empty(),
                Optional.of(outputShuffle),
                ImmutableMap.of(SOURCE_FRAGMENT_ID_1, sourceShuffle1, SOURCE_FRAGMENT_ID_2, sourceShuffle2),
                2);

        ListenableFuture<Void> blocked = scheduler.isBlocked();
        assertTrue(blocked.isDone());

        scheduler.schedule();

        blocked = scheduler.isBlocked();
        // blocked on input shuffle
        assertFalse(blocked.isDone());

        sourceShuffle1.setInputPartitionHandles(ImmutableList.of(new TestingShufflePartitionHandle(0, 1)));
        // still blocked on the second shuffle
        assertFalse(blocked.isDone());
        assertFalse(scheduler.isBlocked().isDone());

        sourceShuffle2.setInputPartitionHandles(ImmutableList.of(new TestingShufflePartitionHandle(0, 1)));
        // now unblocked
        assertTrue(blocked.isDone());
        assertTrue(scheduler.isBlocked().isDone());

        scheduler.schedule();

        blocked = scheduler.isBlocked();
        // blocked on input available signal
        assertFalse(blocked.isDone());

        sourceShuffle2.setInputReady();
        // still blocked on the first shuffle
        assertFalse(blocked.isDone());
        assertFalse(scheduler.isBlocked().isDone());

        sourceShuffle1.setInputReady();
        // now unblocked
        assertTrue(blocked.isDone());
        assertTrue(scheduler.isBlocked().isDone());

        // schedule tasks now
        scheduler.schedule();

        blocked = scheduler.isBlocked();
        // blocked on node allocation
        assertFalse(blocked.isDone());

        // not all tasks have been enumerated yet
        assertNull(outputShuffle.getAllTasks());

        Map<TaskId, TestingRemoteTask> tasks = remoteTaskFactory.getTasks();
        // one task per node
        assertThat(tasks).hasSize(3);
        assertThat(tasks).containsKey(getTaskId(0, 0));
        assertThat(tasks).containsKey(getTaskId(1, 0));
        assertThat(tasks).containsKey(getTaskId(2, 0));

        TestingRemoteTask task = tasks.get(getTaskId(0, 0));
        // fail task for partition 0
        task.fail(new RuntimeException("some failure"));

        assertTrue(blocked.isDone());
        assertTrue(scheduler.isBlocked().isDone());

        // schedule more tasks
        scheduler.schedule();

        tasks = remoteTaskFactory.getTasks();
        assertThat(tasks).hasSize(4);
        assertThat(tasks).containsKey(getTaskId(3, 0));

        blocked = scheduler.isBlocked();
        // blocked on task scheduling
        assertFalse(blocked.isDone());

        // finish some task
        assertThat(tasks).containsKey(getTaskId(1, 0));
        tasks.get(getTaskId(1, 0)).finish();

        assertTrue(blocked.isDone());
        assertTrue(scheduler.isBlocked().isDone());
        assertThat(outputShuffle.getFinishedTasks()).contains(1);

        // this will schedule failed task
        scheduler.schedule();

        blocked = scheduler.isBlocked();
        // blocked on task scheduling
        assertFalse(blocked.isDone());

        tasks = remoteTaskFactory.getTasks();
        assertThat(tasks).hasSize(5);
        assertThat(tasks).containsKey(getTaskId(0, 1));

        // finish some task
        tasks = remoteTaskFactory.getTasks();
        assertThat(tasks).containsKey(getTaskId(3, 0));
        tasks.get(getTaskId(3, 0)).finish();
        assertThat(outputShuffle.getFinishedTasks()).contains(1, 3);

        assertTrue(blocked.isDone());

        // oops, input become unavailable
        sourceShuffle2.resetInputReady();

        scheduler.schedule();

        blocked = scheduler.isBlocked();
        // blocked on input now
        assertFalse(blocked.isDone());

        // input is ready now
        sourceShuffle2.setInputReady();
        assertTrue(blocked.isDone());

        // schedule the last task
        scheduler.schedule();

        tasks = remoteTaskFactory.getTasks();
        assertThat(tasks).hasSize(6);
        assertThat(tasks).containsKey(getTaskId(4, 0));

        // not finished yet, will be finished when all tasks succeed
        assertFalse(scheduler.isFinished());

        blocked = scheduler.isBlocked();
        // blocked on task scheduling
        assertFalse(blocked.isDone());

        tasks = remoteTaskFactory.getTasks();
        assertThat(tasks).containsKey(getTaskId(4, 0));
        // finish remaining tasks
        tasks.get(getTaskId(0, 1)).finish();
        tasks.get(getTaskId(2, 0)).finish();
        tasks.get(getTaskId(4, 0)).finish();

        // now it's not blocked and finished
        assertTrue(blocked.isDone());
        assertTrue(scheduler.isBlocked().isDone());

        assertThat(outputShuffle.getFinishedTasks()).contains(0, 1, 2, 3, 4);

        assertTrue(scheduler.isFinished());
    }

    @Test
    public void testOutputResultConsumer()
            throws Exception
    {
        TestingRemoteTaskFactory remoteTaskFactory = new TestingRemoteTaskFactory();
        TestingBatchTaskSourceFactory taskSourceFactory = createTaskSourceFactory(2, 1);
        TestingNodeSupplier nodeSupplier = TestingNodeSupplier.create(ImmutableMap.of(
                NODE_1, ImmutableList.of(CATALOG),
                NODE_2, ImmutableList.of(CATALOG)));

        TestingResultConsumer resultConsumer = new TestingResultConsumer();

        TestingShuffle sourceShuffle1 = new TestingShuffle();
        TestingShuffle sourceShuffle2 = new TestingShuffle();

        BatchTaskScheduler scheduler = createBatchTaskScheduler(
                remoteTaskFactory,
                taskSourceFactory,
                createNodeAllocator(nodeSupplier),
                Optional.of(resultConsumer),
                Optional.empty(),
                ImmutableMap.of(SOURCE_FRAGMENT_ID_1, sourceShuffle1, SOURCE_FRAGMENT_ID_2, sourceShuffle2),
                2);

        sourceShuffle1.setInputPartitionHandles(ImmutableList.of(new TestingShufflePartitionHandle(0, 1)));
        sourceShuffle1.setInputReady();
        sourceShuffle2.setInputPartitionHandles(ImmutableList.of(new TestingShufflePartitionHandle(0, 1)));
        sourceShuffle2.setInputReady();
        assertTrue(scheduler.isBlocked().isDone());

        scheduler.schedule();
        assertFalse(scheduler.isBlocked().isDone());

        assertThat(resultConsumer.getSourceTasks().get(FRAGMENT_ID)).contains(getTaskId(0, 0), getTaskId(1, 0));

        remoteTaskFactory.getTasks().get(getTaskId(0, 0)).fail(new RuntimeException("some exception"));

        assertTrue(scheduler.isBlocked().isDone());
        scheduler.schedule();
        assertFalse(scheduler.isBlocked().isDone());

        assertThat(resultConsumer.getSourceTasks().get(FRAGMENT_ID)).contains(getTaskId(0, 0), getTaskId(1, 0), getTaskId(0, 1));
    }

    @Test
    public void testTaskFailure()
            throws Exception
    {
        TestingRemoteTaskFactory remoteTaskFactory = new TestingRemoteTaskFactory();
        TestingBatchTaskSourceFactory taskSourceFactory = createTaskSourceFactory(3, 1);
        TestingNodeSupplier nodeSupplier = TestingNodeSupplier.create(ImmutableMap.of(
                NODE_1, ImmutableList.of(CATALOG),
                NODE_2, ImmutableList.of(CATALOG)));

        TestingShuffle sourceShuffle1 = new TestingShuffle();
        TestingShuffle sourceShuffle2 = new TestingShuffle();

        NodeAllocator nodeAllocator = createNodeAllocator(nodeSupplier);
        BatchTaskScheduler scheduler = createBatchTaskScheduler(
                remoteTaskFactory,
                taskSourceFactory,
                nodeAllocator,
                Optional.of(new TestingResultConsumer()),
                Optional.empty(),
                ImmutableMap.of(SOURCE_FRAGMENT_ID_1, sourceShuffle1, SOURCE_FRAGMENT_ID_2, sourceShuffle2),
                0);

        sourceShuffle1.setInputPartitionHandles(ImmutableList.of(new TestingShufflePartitionHandle(0, 1)));
        sourceShuffle1.setInputReady();
        sourceShuffle2.setInputPartitionHandles(ImmutableList.of(new TestingShufflePartitionHandle(0, 1)));
        sourceShuffle2.setInputReady();
        assertTrue(scheduler.isBlocked().isDone());

        scheduler.schedule();

        ListenableFuture<Void> blocked = scheduler.isBlocked();
        // waiting on node acquisition
        assertFalse(blocked.isDone());

        ListenableFuture<InternalNode> acquireNode1 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG), Optional.empty()));
        ListenableFuture<InternalNode> acquireNode2 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG), Optional.empty()));

        remoteTaskFactory.getTasks().get(getTaskId(0, 0)).fail(new RuntimeException("some failure"));

        assertTrue(blocked.isDone());
        assertTrue(acquireNode1.isDone());
        assertTrue(acquireNode2.isDone());

        assertThatThrownBy(scheduler::schedule)
                .hasMessageContaining("some failure");

        assertTrue(scheduler.isBlocked().isDone());
        assertFalse(scheduler.isFinished());
    }

    @Test
    public void testReportTaskFailure()
            throws Exception
    {
        TestingRemoteTaskFactory remoteTaskFactory = new TestingRemoteTaskFactory();
        TestingBatchTaskSourceFactory taskSourceFactory = createTaskSourceFactory(2, 1);
        TestingNodeSupplier nodeSupplier = TestingNodeSupplier.create(ImmutableMap.of(
                NODE_1, ImmutableList.of(CATALOG),
                NODE_2, ImmutableList.of(CATALOG)));

        TestingShuffle sourceShuffle1 = new TestingShuffle();
        TestingShuffle sourceShuffle2 = new TestingShuffle();

        NodeAllocator nodeAllocator = createNodeAllocator(nodeSupplier);
        BatchTaskScheduler scheduler = createBatchTaskScheduler(
                remoteTaskFactory,
                taskSourceFactory,
                nodeAllocator,
                Optional.of(new TestingResultConsumer()),
                Optional.empty(),
                ImmutableMap.of(SOURCE_FRAGMENT_ID_1, sourceShuffle1, SOURCE_FRAGMENT_ID_2, sourceShuffle2),
                1);

        sourceShuffle1.setInputPartitionHandles(ImmutableList.of(new TestingShufflePartitionHandle(0, 1)));
        sourceShuffle1.setInputReady();
        sourceShuffle2.setInputPartitionHandles(ImmutableList.of(new TestingShufflePartitionHandle(0, 1)));
        sourceShuffle2.setInputReady();
        assertTrue(scheduler.isBlocked().isDone());

        scheduler.schedule();

        ListenableFuture<Void> blocked = scheduler.isBlocked();
        // waiting for tasks to finish
        assertFalse(blocked.isDone());

        scheduler.reportTaskFailure(getTaskId(0, 0), new RuntimeException("some failure"));
        assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 0)).getTaskStatus().getState(), TaskState.FAILED);

        assertTrue(blocked.isDone());
        scheduler.schedule();

        assertThat(remoteTaskFactory.getTasks()).containsKey(getTaskId(0, 1));

        remoteTaskFactory.getTasks().get(getTaskId(0, 1)).finish();
        remoteTaskFactory.getTasks().get(getTaskId(1, 0)).finish();

        assertTrue(scheduler.isBlocked().isDone());
        assertTrue(scheduler.isFinished());
    }

    @Test
    public void testLostTaskOutput()
            throws Exception
    {
        TestingRemoteTaskFactory remoteTaskFactory = new TestingRemoteTaskFactory();
        TestingBatchTaskSourceFactory taskSourceFactory = createTaskSourceFactory(2, 1);
        TestingNodeSupplier nodeSupplier = TestingNodeSupplier.create(ImmutableMap.of(
                NODE_1, ImmutableList.of(CATALOG),
                NODE_2, ImmutableList.of(CATALOG)));

        TestingShuffle outputShuffle = new TestingShuffle();

        TestingShuffle sourceShuffle1 = new TestingShuffle();
        TestingShuffle sourceShuffle2 = new TestingShuffle();

        NodeAllocator nodeAllocator = createNodeAllocator(nodeSupplier);
        BatchTaskScheduler scheduler = createBatchTaskScheduler(
                remoteTaskFactory,
                taskSourceFactory,
                nodeAllocator,
                Optional.empty(),
                Optional.of(outputShuffle),
                ImmutableMap.of(SOURCE_FRAGMENT_ID_1, sourceShuffle1, SOURCE_FRAGMENT_ID_2, sourceShuffle2),
                1);

        sourceShuffle1.setInputPartitionHandles(ImmutableList.of(new TestingShufflePartitionHandle(0, 1)));
        sourceShuffle1.setInputReady();
        sourceShuffle2.setInputPartitionHandles(ImmutableList.of(new TestingShufflePartitionHandle(0, 1)));
        sourceShuffle2.setInputReady();
        assertTrue(scheduler.isBlocked().isDone());

        scheduler.schedule();

        ListenableFuture<Void> blocked = scheduler.isBlocked();
        // waiting for tasks to finish
        assertFalse(blocked.isDone());

        remoteTaskFactory.getTasks().get(getTaskId(0, 0)).finish();
        remoteTaskFactory.getTasks().get(getTaskId(1, 0)).finish();

        assertTrue(scheduler.isBlocked().isDone());
        assertTrue(scheduler.isFinished());

        outputShuffle.reportLostTaskOutput(ImmutableSet.of(1));

        assertTrue(scheduler.isBlocked().isDone());
        assertFalse(scheduler.isFinished());

        scheduler.schedule();

        assertThat(remoteTaskFactory.getTasks()).containsKey(getTaskId(1, 1));

        remoteTaskFactory.getTasks().get(getTaskId(1, 1)).finish();

        assertTrue(scheduler.isBlocked().isDone());
        assertTrue(scheduler.isFinished());
    }

    @Test
    public void testCancellation()
            throws Exception
    {
        testCancellation(true);
        testCancellation(false);
    }

    private void testCancellation(boolean abort)
            throws Exception
    {
        TestingRemoteTaskFactory remoteTaskFactory = new TestingRemoteTaskFactory();
        TestingBatchTaskSourceFactory taskSourceFactory = createTaskSourceFactory(3, 1);
        TestingNodeSupplier nodeSupplier = TestingNodeSupplier.create(ImmutableMap.of(
                NODE_1, ImmutableList.of(CATALOG),
                NODE_2, ImmutableList.of(CATALOG)));

        TestingShuffle sourceShuffle1 = new TestingShuffle();
        TestingShuffle sourceShuffle2 = new TestingShuffle();

        NodeAllocator nodeAllocator = createNodeAllocator(nodeSupplier);
        BatchTaskScheduler scheduler = createBatchTaskScheduler(
                remoteTaskFactory,
                taskSourceFactory,
                nodeAllocator,
                Optional.of(new TestingResultConsumer()),
                Optional.empty(),
                ImmutableMap.of(SOURCE_FRAGMENT_ID_1, sourceShuffle1, SOURCE_FRAGMENT_ID_2, sourceShuffle2),
                0);

        sourceShuffle1.setInputPartitionHandles(ImmutableList.of(new TestingShufflePartitionHandle(0, 1)));
        sourceShuffle1.setInputReady();
        sourceShuffle2.setInputPartitionHandles(ImmutableList.of(new TestingShufflePartitionHandle(0, 1)));
        sourceShuffle2.setInputReady();
        assertTrue(scheduler.isBlocked().isDone());

        scheduler.schedule();

        ListenableFuture<Void> blocked = scheduler.isBlocked();
        // waiting on node acquisition
        assertFalse(blocked.isDone());

        ListenableFuture<InternalNode> acquireNode1 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG), Optional.empty()));
        ListenableFuture<InternalNode> acquireNode2 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG), Optional.empty()));

        if (abort) {
            scheduler.abort();
        }
        else {
            scheduler.cancel();
        }

        assertTrue(blocked.isDone());
        assertTrue(acquireNode1.isDone());
        assertTrue(acquireNode2.isDone());

        scheduler.schedule();

        assertTrue(scheduler.isBlocked().isDone());
        assertFalse(scheduler.isFinished());
    }

    private BatchTaskScheduler createBatchTaskScheduler(
            RemoteTaskFactory remoteTaskFactory,
            BatchTaskSourceFactory taskSourceFactory,
            NodeAllocator nodeAllocator,
            Optional<ResultsConsumer> outputResultsConsumer,
            Optional<Shuffle> outputShuffle,
            Map<PlanFragmentId, Shuffle> inputShuffles,
            int retryAttempts)
    {
        return BatchTaskScheduler.create(
                SESSION,
                createSqlStageExecution(remoteTaskFactory),
                new NoOpFailureDetector(),
                taskSourceFactory,
                nodeAllocator,
                outputResultsConsumer,
                outputShuffle,
                Optional.empty(),
                inputShuffles,
                Optional.empty(),
                Optional.empty(),
                retryAttempts);
    }

    private SqlStageExecution createSqlStageExecution(RemoteTaskFactory remoteTaskFactory)
    {
        PlanFragment fragment = createPlanFragment();
        return SqlStageExecution.createSqlStageExecution(
                STAGE_ID,
                fragment,
                ImmutableMap.of(),
                remoteTaskFactory,
                SESSION,
                false,
                nodeTaskMap,
                directExecutor(),
                new SplitSchedulerStats());
    }

    private PlanFragment createPlanFragment()
    {
        Symbol probeColumnSymbol = new Symbol("probe_column");
        Symbol buildColumnSymbol = new Symbol("build_column");
        TableScanNode tableScan = new TableScanNode(
                TABLE_SCAN_NODE_ID,
                TEST_TABLE_HANDLE,
                ImmutableList.of(probeColumnSymbol),
                ImmutableMap.of(probeColumnSymbol, new TestingColumnHandle("column")),
                TupleDomain.none(),
                Optional.empty(),
                false,
                Optional.empty());
        RemoteSourceNode remoteSource = new RemoteSourceNode(
                new PlanNodeId("remote_source_id"),
                ImmutableList.of(SOURCE_FRAGMENT_ID_1, SOURCE_FRAGMENT_ID_2),
                ImmutableList.of(buildColumnSymbol),
                Optional.empty(),
                REPLICATE,
                TASK);
        return new PlanFragment(
                FRAGMENT_ID,
                new JoinNode(
                        new PlanNodeId("join_id"),
                        INNER,
                        tableScan,
                        remoteSource,
                        ImmutableList.of(),
                        tableScan.getOutputSymbols(),
                        remoteSource.getOutputSymbols(),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(REPLICATED),
                        Optional.empty(),
                        ImmutableMap.of(),
                        Optional.empty()),
                ImmutableMap.of(probeColumnSymbol, VARCHAR, buildColumnSymbol, VARCHAR),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(TABLE_SCAN_NODE_ID),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(probeColumnSymbol, buildColumnSymbol)),
                ungroupedExecution(),
                StatsAndCosts.empty(),
                Optional.empty());
    }

    private static TestingBatchTaskSourceFactory createTaskSourceFactory(int splitCount, int taskPerBatch)
    {
        return new TestingBatchTaskSourceFactory(Optional.of(CATALOG), createSplits(splitCount), taskPerBatch);
    }

    private static List<Split> createSplits(int count)
    {
        return ImmutableList.copyOf(limit(cycle(new Split(CATALOG, createRemoteSplit(), Lifespan.taskWide())), count));
    }

    private NodeAllocator createNodeAllocator(TestingNodeSupplier nodeSupplier)
    {
        NodeScheduler nodeScheduler = new NodeScheduler(new TestingNodeSelectorFactory(NODE_1, nodeSupplier));
        return new FixedCountNodeAllocator(nodeScheduler, SESSION, 1);
    }

    private static TaskId getTaskId(int partitionId, int attemptId)
    {
        return new TaskId(STAGE_ID, partitionId, attemptId);
    }
}
