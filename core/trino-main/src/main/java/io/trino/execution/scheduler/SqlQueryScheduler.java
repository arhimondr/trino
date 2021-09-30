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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.graph.Traverser;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.log.Logger;
import io.airlift.stats.TimeStat;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.execution.BasicStageStats;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.QueryState;
import io.trino.execution.QueryStateMachine;
import io.trino.execution.RemoteTask;
import io.trino.execution.RemoteTaskFactory;
import io.trino.execution.SqlStageExecution;
import io.trino.execution.StageId;
import io.trino.execution.StageInfo;
import io.trino.execution.TableInfo;
import io.trino.execution.TaskId;
import io.trino.execution.TaskStatus;
import io.trino.execution.scheduler.SqlQueryScheduler.StreamingScheduler;
import io.trino.failuredetector.FailureDetector;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableProperties;
import io.trino.metadata.TableSchema;
import io.trino.server.DynamicFilterService;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPartitionHandle;
import io.trino.split.SplitSource;
import io.trino.sql.planner.NodePartitionMap;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.SplitSourceFactory;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.TableScanNode;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.trino.SystemSessionProperties.getConcurrentLifespansPerNode;
import static io.trino.SystemSessionProperties.getWriterMinSize;
import static io.trino.connector.CatalogName.isInternalSystemConnector;
import static io.trino.execution.BasicStageStats.aggregateBasicStageStats;
import static io.trino.execution.SqlStageExecution.createSqlStageExecution;
import static io.trino.execution.scheduler.SourcePartitionedScheduler.newSourcePartitionedSchedulerAsStageScheduler;
import static io.trino.execution.scheduler.StreamingStageExecution.State.ABORTED;
import static io.trino.execution.scheduler.StreamingStageExecution.State.CANCELED;
import static io.trino.execution.scheduler.StreamingStageExecution.State.FAILED;
import static io.trino.execution.scheduler.StreamingStageExecution.State.FINISHED;
import static io.trino.execution.scheduler.StreamingStageExecution.State.FLUSHING;
import static io.trino.execution.scheduler.StreamingStageExecution.State.RUNNING;
import static io.trino.execution.scheduler.StreamingStageExecution.State.SCHEDULED;
import static io.trino.execution.scheduler.StreamingStageExecution.createStreamingStageExecution;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static io.trino.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.trino.util.Failures.checkCondition;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toCollection;

public class SqlQueryScheduler
{
    private static final Logger log = Logger.get(SqlQueryScheduler.class);

    private final QueryStateMachine queryStateMachine;
    private final NodePartitioningManager nodePartitioningManager;
    private final NodeScheduler nodeScheduler;
    private final int splitBatchSize;
    private final ExecutorService executor;
    private final ScheduledExecutorService schedulerExecutor;
    private final FailureDetector failureDetector;
    private final ExecutionPolicy executionPolicy;
    private final SplitSchedulerStats schedulerStats;
    private final DynamicFilterService dynamicFilterService;
    private final SplitSourceFactory splitSourceFactory;

    private final StageExecutions stageExecutions;

    @GuardedBy("this")
    private boolean started;
    @GuardedBy("this")
    private ResultsConsumer coordinatorResultConsumer;
    @GuardedBy("this")
    private Map<PlanFragmentId, OutputBufferManager> coordinatorOutputBuffers;

    private final AtomicReference<CoordinatorScheduler> coordinatorScheduler = new AtomicReference<>();
    private final AtomicReference<StreamingScheduler> streamingScheduler = new AtomicReference<>();

    public SqlQueryScheduler(
            QueryStateMachine queryStateMachine,
            SubPlan plan,
            NodePartitioningManager nodePartitioningManager,
            NodeScheduler nodeScheduler,
            RemoteTaskFactory remoteTaskFactory,
            boolean summarizeTaskInfo,
            int splitBatchSize,
            ExecutorService queryExecutor,
            ScheduledExecutorService schedulerExecutor,
            FailureDetector failureDetector,
            NodeTaskMap nodeTaskMap,
            ExecutionPolicy executionPolicy,
            SplitSchedulerStats schedulerStats,
            DynamicFilterService dynamicFilterService,
            Metadata metadata,
            SplitSourceFactory splitSourceFactory)
    {
        this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
        this.splitBatchSize = splitBatchSize;
        this.executor = requireNonNull(queryExecutor, "queryExecutor is null");
        this.schedulerExecutor = requireNonNull(schedulerExecutor, "schedulerExecutor is null");
        this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
        this.executionPolicy = requireNonNull(executionPolicy, "executionPolicy is null");
        this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
        this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
        this.splitSourceFactory = requireNonNull(splitSourceFactory, "splitSourceFactory is null");

        stageExecutions = new StageExecutions(
                queryStateMachine,
                queryStateMachine.getSession(),
                metadata,
                remoteTaskFactory,
                nodeTaskMap,
                queryExecutor,
                schedulerStats,
                plan,
                summarizeTaskInfo);
        stageExecutions.initialize();
    }

    public synchronized void start()
    {
        if (started) {
            return;
        }
        started = true;

        if (queryStateMachine.isDone()) {
            return;
        }

        coordinatorScheduler.set(
                new CoordinatorScheduler(
                        queryStateMachine,
                        nodeScheduler,
                        stageExecutions,
                        failureDetector,
                        schedulerExecutor,
                        dynamicFilterService,
                        streamingScheduler));

        CoordinatorScheduler coordinatorScheduler =

                StreamingScheduler streamingScheduler = createStreamingScheduler(new ResultsConsumer()
    {

    });
        scheduler.set(streamingScheduler);
        executor.submit(streamingScheduler::schedule);
    }

    public synchronized void cancelStage(StageId stageId)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            StreamingScheduler scheduler = this.scheduler.get();
            if (scheduler != null) {
                scheduler.cancelStage(stageId);
            }
        }
    }

    public synchronized void abort()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            StreamingScheduler scheduler = this.scheduler.get();
            if (scheduler != null) {
                scheduler.abort();
            }
            stages.values().forEach(SqlStageExecution::transitionToFinished);
        }
    }

    public BasicStageStats getBasicStageStats()
    {
        return stageExecutions.getBasicStageStats();
    }

    public StageInfo getStageInfo()
    {
        return stageExecutions.getStageInfo();
    }

    public long getUserMemoryReservation()
    {
        return stageExecutions.getUserMemoryReservation();
    }

    public long getTotalMemoryReservation()
    {
        return stageExecutions.getTotalMemoryReservation();
    }

    public Duration getTotalCpuTime()
    {
        return stageExecutions.getTotalCpuTime();
    }

    private static class StageExecutions
    {
        private final QueryStateMachine queryStateMachine;
        private final Map<StageId, SqlStageExecution> allStages;
        private final List<SqlStageExecution> coordinatorStages;
        private final List<SqlStageExecution> distributedStages;
        private final StageId rootStageId;
        private final Map<StageId, Set<StageId>> stageLineage;

        public StageExecutions(
                QueryStateMachine queryStateMachine,
                Session session,
                Metadata metadata,
                RemoteTaskFactory taskFactory,
                NodeTaskMap nodeTaskMap,
                ExecutorService executor,
                SplitSchedulerStats schedulerStats,
                SubPlan planTree,
                boolean summarizeTaskInfo)
        {
            this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");

            ImmutableMap.Builder<StageId, SqlStageExecution> allStages = ImmutableMap.builder();
            ImmutableList.Builder<SqlStageExecution> coordinatorStages = ImmutableList.builder();
            ImmutableList.Builder<SqlStageExecution> distributedStages = ImmutableList.builder();
            StageId rootStageId = null;
            ImmutableMap.Builder<StageId, Set<StageId>> stageLineage = ImmutableMap.builder();
            for (SubPlan planNode : Traverser.forTree(SubPlan::getChildren).breadthFirst(planTree)) {
                PlanFragment fragment = planNode.getFragment();
                SqlStageExecution stageExecution = createSqlStageExecution(
                        getStageId(session.getQueryId(), fragment.getId()),
                        fragment,
                        extractTableInfo(session, metadata, fragment),
                        taskFactory,
                        session,
                        summarizeTaskInfo,
                        nodeTaskMap,
                        executor,
                        schedulerStats);
                StageId stageId = stageExecution.getStageId();
                allStages.put(stageId, stageExecution);
                if (fragment.getPartitioning().isCoordinatorOnly()) {
                    coordinatorStages.add(stageExecution);
                }
                else {
                    distributedStages.add(stageExecution);
                }
                if (rootStageId == null) {
                    rootStageId = stageId;
                }
                stageLineage.put(
                        stageId,
                        planNode.getChildren().stream()
                                .map(stage -> getStageId(session.getQueryId(), stage.getFragment().getId()))
                                .collect(toImmutableSet()));
            }
            this.allStages = allStages.build();
            this.coordinatorStages = coordinatorStages.build();
            this.distributedStages = distributedStages.build();
            this.rootStageId = requireNonNull(rootStageId, "rootStageId is null");
            this.stageLineage = stageLineage.build();
        }

        private static Map<PlanNodeId, TableInfo> extractTableInfo(Session session, Metadata metadata, PlanFragment fragment)
        {
            return searchFrom(fragment.getRoot())
                    .where(TableScanNode.class::isInstance)
                    .findAll()
                    .stream()
                    .map(TableScanNode.class::cast)
                    .collect(toImmutableMap(PlanNode::getId, node -> getTableInfo(session, metadata, node)));
        }

        private static TableInfo getTableInfo(Session session, Metadata metadata, TableScanNode node)
        {
            TableSchema tableSchema = metadata.getTableSchema(session, node.getTable());
            TableProperties tableProperties = metadata.getTableProperties(session, node.getTable());
            return new TableInfo(tableSchema.getQualifiedName(), tableProperties.getPredicate());
        }

        private static StageId getStageId(QueryId queryId, PlanFragmentId fragmentId)
        {
            // TODO: refactor fragment id to be based on an integer
            return new StageId(queryId, parseInt(fragmentId.toString()));
        }

        // this is a separate method to ensure that the `this` reference is not leaked during construction
        public void initialize()
        {
            // when query is done or any time a stage completes, attempt to transition query to "final query info ready"
            queryStateMachine.addStateChangeListener(newState -> {
                if (newState.isDone()) {
                    allStages.values().forEach(SqlStageExecution::finish);
                    queryStateMachine.updateQueryInfo(Optional.ofNullable(getStageInfo()));
                }
            });
            for (SqlStageExecution stage : allStages.values()) {
                stage.addFinalStageInfoListener(status -> queryStateMachine.updateQueryInfo(Optional.ofNullable(getStageInfo())));
            }
        }

        public List<SqlStageExecution> getCoordinatorStages()
        {
            return coordinatorStages;
        }

        public List<SqlStageExecution> getDistributedStages()
        {
            return distributedStages;
        }

        public SqlStageExecution getByFragmentId(PlanFragmentId fragmentId)
        {
            return requireNonNull(allStages.get(getStageId(queryStateMachine.getQueryId(), fragmentId)), () -> "stage not found for fragmentId: " + fragmentId);
        }

        public SqlStageExecution getByStageId(StageId stageId)
        {
            return requireNonNull(allStages.get(stageId), () -> "stage not found: " + stageId);
        }

        public BasicStageStats getBasicStageStats()
        {
            List<BasicStageStats> stageStats = allStages.values().stream()
                    .map(SqlStageExecution::getBasicStageStats)
                    .collect(toImmutableList());

            return aggregateBasicStageStats(stageStats);
        }

        public StageInfo getStageInfo()
        {
            Map<StageId, StageInfo> stageInfos = allStages.values().stream()
                    .map(SqlStageExecution::getStageInfo)
                    .collect(toImmutableMap(StageInfo::getStageId, identity()));

            return buildStageInfo(rootStageId, stageInfos);
        }

        private StageInfo buildStageInfo(StageId stageId, Map<StageId, StageInfo> stageInfos)
        {
            StageInfo parent = stageInfos.get(stageId);
            checkArgument(parent != null, "No stageInfo for %s", parent);
            List<StageInfo> childStages = stageLineage.get(stageId).stream()
                    .map(childStageId -> buildStageInfo(childStageId, stageInfos))
                    .collect(toImmutableList());
            if (childStages.isEmpty()) {
                return parent;
            }
            return new StageInfo(
                    parent.getStageId(),
                    parent.getState(),
                    parent.getPlan(),
                    parent.getTypes(),
                    parent.getStageStats(),
                    parent.getTasks(),
                    childStages,
                    parent.getTables(),
                    parent.getFailureCause());
        }

        public long getUserMemoryReservation()
        {
            return allStages.values().stream()
                    .mapToLong(SqlStageExecution::getUserMemoryReservation)
                    .sum();
        }

        public long getTotalMemoryReservation()
        {
            return allStages.values().stream()
                    .mapToLong(SqlStageExecution::getTotalMemoryReservation)
                    .sum();
        }

        public Duration getTotalCpuTime()
        {
            long millis = allStages.values().stream()
                    .mapToLong(stage -> stage.getTotalCpuTime().toMillis())
                    .sum();
            return new Duration(millis, MILLISECONDS);
        }
    }

    private static class OutputResultConsumer
            implements ResultsConsumer
    {
        private final QueryStateMachine queryStateMachine;

        private OutputResultConsumer(QueryStateMachine queryStateMachine)
        {
            this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
        }

        @Override
        public void addSourceTask(PlanFragmentId fragmentId, RemoteTask task)
        {
            Map<TaskId, URI> bufferLocations = ImmutableMap.of(
                    task.getTaskId(),
                    uriBuilderFrom(task.getTaskStatus().getSelf())
                            .appendPath("results")
                            .appendPath("0").build());
            queryStateMachine.updateOutputLocations(bufferLocations, false);
        }

        @Override
        public void noMoreSourceTasks(PlanFragmentId fragmentId)
        {
            queryStateMachine.updateOutputLocations(ImmutableMap.of(), true);
        }
    }

    private static class CoordinatorScheduler
    {
        private final QueryStateMachine queryStateMachine;
        private final NodeScheduler nodeScheduler;
        private final Map<PlanFragmentId, OutputBufferManager> outputBuffers;
        private final Map<PlanFragmentId, Optional<int[]>> bucketToPartition;
        private final ResultsConsumer resultsConsumer;
        private final List<StreamingStageExecution> executions;
        private final AtomicBoolean scheduled = new AtomicBoolean();

        public CoordinatorScheduler(
                QueryStateMachine queryStateMachine,
                NodeScheduler nodeScheduler,
                StageExecutions stageExecutions,
                FailureDetector failureDetector,
                Executor executor,
                DynamicFilterService dynamicFilterService,
                AtomicReference<StreamingScheduler> streamingScheduler)
        {
            this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
            this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");

            outputBuffers = createCoordinatorOutputBuffers(stageExecutions);
            bucketToPartition = createBucketToPartition(stageExecutions);

            ResultsConsumer parent = new OutputResultConsumer(queryStateMachine);
            // create executions
            ImmutableList.Builder<StreamingStageExecution> executionsBuilder = ImmutableList.builder();
            List<SqlStageExecution> coordinatorStages = stageExecutions.getCoordinatorStages();
            for (SqlStageExecution stageExecution : coordinatorStages) {
                StreamingStageExecution execution = createStreamingStageExecution(
                        stageExecution,
                        outputBuffers,
                        parent,
                        failureDetector,
                        executor,
                        // always single consumer for coordinator tasks
                        Optional.of(new int[1]));
                executionsBuilder.add(execution);
                parent = execution;
            }
            resultsConsumer = parent;
            executions = executionsBuilder.build();

            // attach listeners
            for (int i = 0; i < executions.size(); i++) {
                StreamingStageExecution execution = executions.get(i);
                StreamingStageExecution upstreamCoordinatorStageExecution = i < executions.size() - 1 ? executions.get(i + 1) : null;

                // verify only single upstream coordinator stage
                if (upstreamCoordinatorStageExecution != null) {
                    List<RemoteSourceNode> remoteSources = execution.getFragment().getRemoteSourceNodes();
                    verify(remoteSources.size() == 1, "expected single remote source: %s", remoteSources);
                    List<PlanFragmentId> sourceFragmentIds = remoteSources.get(0).getSourceFragmentIds();
                    verify(sourceFragmentIds.size() == 1, "expected single source fragment: %s", sourceFragmentIds);
                    PlanFragmentId sourceFragmentId = sourceFragmentIds.get(0);
                    PlanFragmentId expectedSourceFragmentId = upstreamCoordinatorStageExecution.getFragment().getId();
                    verify(expectedSourceFragmentId.equals(sourceFragmentId), "unexpected sourceFragmentId %s, expected %s ", sourceFragmentId, expectedSourceFragmentId);
                }

                execution.addStateChangeListener(newState -> {
                    if (newState == FLUSHING || newState.isDone()) {
                        if (upstreamCoordinatorStageExecution != null) {
                            upstreamCoordinatorStageExecution.cancel();
                        }
                        else if (streamingScheduler.get() != null) {
                            streamingScheduler.get().cancel();
                        }
                    }
                });

                boolean root = i == 0;
                if (root) {
                    // finish query if the root execution has finished
                    execution.addStateChangeListener(state -> {
                        if (state == FINISHED) {
                            queryStateMachine.transitionToFinishing();
                        }
                        else if (state == CANCELED) {
                            // output stage was canceled
                            queryStateMachine.transitionToCanceled();
                        }
                    });
                }

                execution.addStateChangeListener(state -> {
                    if (queryStateMachine.isDone()) {
                        return;
                    }
                    if (!state.canScheduleMoreTasks()) {
                        dynamicFilterService.stageCannotScheduleMoreTasks(execution.getStageId(), execution.getAllTasks().size());
                    }
                    if (state == FAILED) {
                        // if coordinator stage failed - transition to failure
                        RuntimeException failureCause = execution.getFailureCause()
                                .map(ExecutionFailureInfo::toException)
                                .orElseGet(() -> new VerifyException(format("stage execution for stage %s is failed by failure cause is not present", execution.getStageId())));
                        queryStateMachine.transitionToFailed(failureCause);
                        stageExecutions.getByStageId(execution.getStageId()).fail(failureCause);
                    }
                    else if (state == ABORTED) {
                        // this should never happen, since abort can only be triggered in query clean up after the query is finished
                        queryStateMachine.transitionToFailed(new TrinoException(GENERIC_INTERNAL_ERROR, "Query stage was aborted"));
                    }
                });
            }
        }

        public void schedule()
        {
            if (!scheduled.compareAndSet(false, true)) {
                return;
            }

            InternalNode coordinator = nodeScheduler.createNodeSelector(queryStateMachine.getSession(), Optional.empty()).selectCurrentNode();
            for (StreamingStageExecution execution : executions) {
                execution.scheduleTask(
                        coordinator,
                        0,
                        ImmutableMultimap.of(),
                        ImmutableMultimap.of());
                execution.schedulingComplete();
            }

            queryStateMachine.addOutputTaskFailureListener((taskId, failure) -> {
                // TODO hook in task failure listeners
            });
        }

        public Map<PlanFragmentId, OutputBufferManager> getOutputBuffers()
        {
            return outputBuffers;
        }

        public Map<PlanFragmentId, Optional<int[]>> getBucketToPartition()
        {
            return bucketToPartition;
        }

        public ResultsConsumer getResultsConsumer()
        {
            return resultsConsumer;
        }

        private static Map<PlanFragmentId, OutputBufferManager> createCoordinatorOutputBuffers(StageExecutions stageExecutions)
        {
            List<SqlStageExecution> coordinatorStages = stageExecutions.getCoordinatorStages();
            List<SqlStageExecution> distributedStages = stageExecutions.getDistributedStages();
            ImmutableMap.Builder<PlanFragmentId, OutputBufferManager> coordinatorOutputBuffers = ImmutableMap.builder();
            if (!coordinatorStages.isEmpty()) {
                for (int i = 0; i < coordinatorStages.size(); i++) {
                    SqlStageExecution stage = coordinatorStages.get(i);
                    PlanFragment fragment = stage.getFragment();
                    boolean isOutputStage = i == 0;
                    if (isOutputStage) {
                        PartitioningHandle partitioningHandle = fragment.getPartitioningScheme().getPartitioning().getHandle();
                        checkArgument(partitioningHandle.isSingleNode(), "partitioning is expected to be single node: " + partitioningHandle);
                        coordinatorOutputBuffers.put(fragment.getId(), new PartitionedOutputBufferManager(partitioningHandle, 1));
                    }
                    List<PlanFragmentId> inputFragmentIds = fragment.getRemoteSourceNodes().stream()
                            .map(RemoteSourceNode::getSourceFragmentIds)
                            .flatMap(List::stream)
                            .collect(toImmutableList());
                    for (PlanFragmentId inputFragmentId : inputFragmentIds) {
                        PlanFragment inputFragment = stageExecutions.getByFragmentId(inputFragmentId).getFragment();
                        PartitioningHandle partitioningHandle = inputFragment.getPartitioningScheme().getPartitioning().getHandle();
                        checkArgument(partitioningHandle.isSingleNode(), "partitioning is expected to be single node: " + partitioningHandle);
                        coordinatorOutputBuffers.put(inputFragment.getId(), new PartitionedOutputBufferManager(partitioningHandle, 1));
                    }
                    boolean isLastCoordinatorStage = i == coordinatorStages.size() - 1;
                    if (isLastCoordinatorStage) {
                        verify(distributedStages.size() >= inputFragmentIds.size(), "%s < %s", distributedStages.size(), inputFragmentIds.size());
                        Set<PlanFragmentId> expectedTopDistributedFragments = ImmutableSet.copyOf(inputFragmentIds);
                        Set<PlanFragmentId> actualTopDistributedFragments = distributedStages.subList(0, inputFragmentIds.size()).stream()
                                .map(SqlStageExecution::getFragment)
                                .map(PlanFragment::getId)
                                .collect(toImmutableSet());
                        verify(actualTopDistributedFragments.equals(expectedTopDistributedFragments), "%s != %s", actualTopDistributedFragments, expectedTopDistributedFragments);
                    }
                }
            }
            else {
                verify(!distributedStages.isEmpty(), "query is expected to have at least one stage");
                PlanFragment fragment = distributedStages.get(0).getFragment();
                PartitioningHandle partitioningHandle = fragment.getPartitioningScheme().getPartitioning().getHandle();
                checkArgument(partitioningHandle.isSingleNode(), "partitioning is expected to be single node: " + partitioningHandle);
                coordinatorOutputBuffers.put(fragment.getId(), new PartitionedOutputBufferManager(partitioningHandle, 1));

                // verify that indeed there's only a single output fragment
                List<PlanFragmentId> inputFragmentIds = fragment.getRemoteSourceNodes().stream()
                        .map(RemoteSourceNode::getSourceFragmentIds)
                        .flatMap(List::stream)
                        .collect(toImmutableList());
                verify(distributedStages.size() - 1 >= inputFragmentIds.size(), "%s < %s", distributedStages.size() - 1, inputFragmentIds.size());
                Set<PlanFragmentId> expectedTopDistributedFragments = ImmutableSet.copyOf(inputFragmentIds);
                Set<PlanFragmentId> actualTopDistributedFragments = distributedStages.subList(1, inputFragmentIds.size() + 1).stream()
                        .map(SqlStageExecution::getFragment)
                        .map(PlanFragment::getId)
                        .collect(toImmutableSet());
                verify(actualTopDistributedFragments.equals(expectedTopDistributedFragments), "%s != %s", actualTopDistributedFragments, expectedTopDistributedFragments);
            }
            return coordinatorOutputBuffers.build();
        }

        private static Map<PlanFragmentId, Optional<int[]>> createBucketToPartition(StageExecutions stageExecutions)
        {
            List<SqlStageExecution> coordinatorStages = stageExecutions.getCoordinatorStages();
            if (!coordinatorStages.isEmpty()) {
                SqlStageExecution lastCoordinatorStage = coordinatorStages.get(coordinatorStages.size() - 1);
                List<PlanFragmentId> inputFragmentIds = lastCoordinatorStage.getFragment().getRemoteSourceNodes().stream()
                        .map(RemoteSourceNode::getSourceFragmentIds)
                        .flatMap(List::stream)
                        .collect(toImmutableList());
                return inputFragmentIds.stream()
                        .collect(toImmutableMap(id -> id, id -> Optional.of(new int[1])));
            }
            else {
                List<SqlStageExecution> distributedStages = stageExecutions.getDistributedStages();
                verify(!distributedStages.isEmpty(), "query is expected to have at least one stage");
                SqlStageExecution outputStage = distributedStages.get(0);
                return ImmutableMap.of(outputStage.getFragment().getId(), Optional.of(new int[1]));
            }
        }
    }

    private static class StreamingScheduler
    {
        private final QueryStateMachine queryStateMachine;
        private final SplitSchedulerStats schedulerStats;

        private final ExecutionSchedule executionSchedule;
        private final Map<StageId, StageScheduler> stageSchedulers;
        private final List<StreamingStageExecution> stageExecutions;

        private final AtomicBoolean started = new AtomicBoolean();

        public StreamingScheduler(
                QueryStateMachine queryStateMachine,
                SplitSchedulerStats schedulerStats,
                NodePartitioningManager nodePartitioningManager,
                StageExecutions stageExecutions,
                CoordinatorScheduler coordinatorScheduler,
                ExecutionPolicy executionPolicy)
        {
            this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
            this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");

            Session session = queryStateMachine.getSession();
            Map<PartitioningHandle, NodePartitionMap> partitioningCacheMap = new HashMap<>();
            Function<PartitioningHandle, NodePartitionMap> partitioningCache = partitioningHandle ->
                    partitioningCacheMap.computeIfAbsent(partitioningHandle, handle -> nodePartitioningManager.getNodePartitioningMap(session, handle));

            Map<PlanFragmentId, Optional<int[]>> bucketToPartitionMap = createBucketToPartitionMap(
                    coordinatorScheduler.getBucketToPartition(),
                    stageExecutions.getDistributedStages(),
                    partitioningCache);
            Map<PlanFragmentId, OutputBufferManager> outputBufferManagers = createOutputBufferManagers(
                    coordinatorScheduler.getOutputBuffers(),
                    stageExecutions.getDistributedStages(),
                    bucketToPartitionMap);

            ImmutableList.Builder<StreamingStageExecution> executionsBuilder = ImmutableList.builder();
            ImmutableMap.Builder<StageId, StageScheduler> schedulersBuilder = ImmutableMap.builder();
            StreamingStageExecution rootExecution = createStreamingExecution(
                    plan,
                    outputBufferManagers,
                    bucketToPartitionMap,
                    partitioningCache,
                    // adapt result consumer, do not send "no more tasks prematurely"
                    coordinatorScheduler.getResultsConsumer(),
                    executionsBuilder,
                    schedulersBuilder);
            List<StreamingStageExecution> executions = executionsBuilder.build();
            executionSchedule = executionPolicy.createExecutionSchedule(executions);

            // TODO
            rootExecution.addStateChangeListener(state -> {
                if (state == FINISHED) {
                    queryStateMachine.transitionToFinishing();
                }
                else if (state == CANCELED) {
                    // output stage was canceled
                    queryStateMachine.transitionToCanceled();
                }
            });

            Set<StageId> finishedStages = newConcurrentHashSet();
            for (StreamingStageExecution execution : executions) {
                // TODO
                execution.addStateChangeListener(state -> {
                    if (queryStateMachine.isDone()) {
                        return;
                    }
                    if (!state.canScheduleMoreTasks()) {
                        dynamicFilterService.stageCannotScheduleMoreTasks(execution.getStageId(), execution.getAllTasks().size());
                    }
                    if (state == FAILED) {
                        RuntimeException failureCause = execution.getFailureCause()
                                .map(ExecutionFailureInfo::toException)
                                .orElseGet(() -> new VerifyException(format("stage execution for stage %s is failed by failure cause is not present", execution.getStageId())));
                        stages.get(execution.getFragment().getId()).transitionToFailed(failureCause);
                        queryStateMachine.transitionToFailed(failureCause);
                    }
                    else if (state == ABORTED) {
                        // this should never happen, since abort can only be triggered in query clean up after the query is finished
                        queryStateMachine.transitionToFailed(new TrinoException(GENERIC_INTERNAL_ERROR, "Query stage was aborted"));
                    }
                    else if (queryStateMachine.getQueryState() == QueryState.STARTING) {
                        // if the stage has at least one task, we are running
                        if (!execution.getAllTasks().isEmpty()) {
                            queryStateMachine.transitionToRunning();
                        }
                    }
                    else if (state.isDone() && !state.isFailure()) {
                        finishedStages.add(execution.getStageId());
                        // Once all remotely scheduled stages complete it should be safe to transition stage execution
                        // to the finished state as at this point no further task retries are expected
                        // This is needed to make explain analyze work that requires final stage info to be available before the
                        // explain analyze stage is finished
                        if (finishedStages.containsAll(remotelyScheduledStages)) {
                            stages.values().stream()
                                    .filter(stage -> finishedStages.contains(stage.getStageId()))
                                    .forEach(SqlStageExecution::transitionToFinished);
                        }
                    }
                });
            }

            return new StreamingScheduler(
                    queryStateMachine,
                    executionSchedule,
                    schedulersBuilder.build(),
                    schedulerStats,
                    executions);
        }

        private StreamingStageExecution createStreamingExecution(
                SubPlan plan,
                Map<PlanFragmentId, OutputBufferManager> outputBufferManagers,
                Map<PlanFragmentId, Optional<int[]>> bucketToPartitionMap,
                Function<PartitioningHandle, NodePartitionMap> partitioningCache,
                ResultsConsumer resultsConsumer,
                ImmutableList.Builder<StreamingStageExecution> executions,
                ImmutableMap.Builder<StageId, StageScheduler> schedulers)
        {
            PlanFragment fragment = plan.getFragment();
            StreamingStageExecution execution = createStreamingStageExecution(
                    stages.get(fragment.getId()),
                    outputBufferManagers,
                    resultsConsumer,
                    failureDetector,
                    executor,
                    bucketToPartitionMap.get(fragment.getId()));
            executions.add(execution);

            Map<PlanNodeId, SplitSource> splitSources = splitSourceFactory.createSplitSources(queryStateMachine.getSession(), fragment);
            // ensure split sources are always closed
            execution.addStateChangeListener(state -> {
                if (state.isDone()) {
                    closeSplitSources(splitSources.values());
                }
            });

            ImmutableList.Builder<StreamingStageExecution> childExecutionsBuilder = ImmutableList.builder();
            for (SubPlan child : plan.getChildren()) {
                childExecutionsBuilder.add(createStreamingExecution(
                        child,
                        outputBufferManagers,
                        bucketToPartitionMap,
                        partitioningCache,
                        execution,
                        executions,
                        schedulers));
            }
            ImmutableList<StreamingStageExecution> childExecutions = childExecutionsBuilder.build();
            StageScheduler scheduler = createStageScheduler(
                    execution,
                    splitSources,
                    childExecutions,
                    partitioningCache);
            schedulers.put(execution.getStageId(), scheduler);

            // TODO
            execution.addStateChangeListener(newState -> {
                if (newState == FLUSHING || newState.isDone()) {
                    childExecutions.forEach(StreamingStageExecution::cancel);
                }
            });

            return execution;
        }

        private static void closeSplitSources(Collection<SplitSource> splitSources)
        {
            for (SplitSource source : splitSources) {
                try {
                    source.close();
                }
                catch (Throwable t) {
                    log.warn(t, "Error closing split source");
                }
            }
        }

        private StageScheduler createStageScheduler(
                StreamingStageExecution stageExecution,
                Map<PlanNodeId, SplitSource> splitSources,
                List<StreamingStageExecution> childStages,
                Function<PartitioningHandle, NodePartitionMap> partitioningCache)
        {
            Session session = queryStateMachine.getSession();
            PlanFragment fragment = stageExecution.getFragment();
            PartitioningHandle partitioningHandle = fragment.getPartitioning();
            if (partitioningHandle.equals(SOURCE_DISTRIBUTION)) {
                // nodes are selected dynamically based on the constraints of the splits and the system load
                Entry<PlanNodeId, SplitSource> entry = Iterables.getOnlyElement(splitSources.entrySet());
                PlanNodeId planNodeId = entry.getKey();
                SplitSource splitSource = entry.getValue();
                Optional<CatalogName> catalogName = Optional.of(splitSource.getCatalogName())
                        .filter(catalog -> !isInternalSystemConnector(catalog));
                NodeSelector nodeSelector = nodeScheduler.createNodeSelector(session, catalogName);
                SplitPlacementPolicy placementPolicy = new DynamicSplitPlacementPolicy(nodeSelector, stageExecution::getAllTasks);

                checkArgument(!fragment.getStageExecutionDescriptor().isStageGroupedExecution());

                return newSourcePartitionedSchedulerAsStageScheduler(
                        stageExecution,
                        planNodeId,
                        splitSource,
                        placementPolicy,
                        splitBatchSize,
                        dynamicFilterService,
                        () -> childStages.stream().anyMatch(StreamingStageExecution::isAnyTaskBlocked));
            }
            else if (partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
                Supplier<Collection<TaskStatus>> sourceTasksProvider = () -> childStages.stream()
                        .map(StreamingStageExecution::getTaskStatuses)
                        .flatMap(List::stream)
                        .collect(toImmutableList());
                Supplier<Collection<TaskStatus>> writerTasksProvider = stageExecution::getTaskStatuses;

                ScaledWriterScheduler scheduler = new ScaledWriterScheduler(
                        stageExecution,
                        sourceTasksProvider,
                        writerTasksProvider,
                        nodeScheduler.createNodeSelector(session, Optional.empty()),
                        schedulerExecutor,
                        getWriterMinSize(session));

                whenAllStages(childStages, StreamingStageExecution.State::isDone)
                        .addListener(scheduler::finish, directExecutor());

                return scheduler;
            }
            else {
                if (!splitSources.isEmpty()) {
                    // contains local source
                    List<PlanNodeId> schedulingOrder = fragment.getPartitionedSources();
                    Optional<CatalogName> catalogName = partitioningHandle.getConnectorId();
                    checkArgument(catalogName.isPresent(), "No connector ID for partitioning handle: %s", partitioningHandle);
                    List<ConnectorPartitionHandle> connectorPartitionHandles;
                    boolean groupedExecutionForStage = fragment.getStageExecutionDescriptor().isStageGroupedExecution();
                    if (groupedExecutionForStage) {
                        connectorPartitionHandles = nodePartitioningManager.listPartitionHandles(session, partitioningHandle);
                        checkState(!ImmutableList.of(NOT_PARTITIONED).equals(connectorPartitionHandles));
                    }
                    else {
                        connectorPartitionHandles = ImmutableList.of(NOT_PARTITIONED);
                    }

                    BucketNodeMap bucketNodeMap;
                    List<InternalNode> stageNodeList;
                    if (fragment.getRemoteSourceNodes().stream().allMatch(node -> node.getExchangeType() == REPLICATE)) {
                        // no remote source
                        boolean dynamicLifespanSchedule = fragment.getStageExecutionDescriptor().isDynamicLifespanSchedule();
                        bucketNodeMap = nodePartitioningManager.getBucketNodeMap(session, partitioningHandle, dynamicLifespanSchedule);

                        // verify execution is consistent with planner's decision on dynamic lifespan schedule
                        verify(bucketNodeMap.isDynamic() == dynamicLifespanSchedule);

                        stageNodeList = new ArrayList<>(nodeScheduler.createNodeSelector(session, catalogName).allNodes());
                        Collections.shuffle(stageNodeList);
                    }
                    else {
                        // cannot use dynamic lifespan schedule
                        verify(!fragment.getStageExecutionDescriptor().isDynamicLifespanSchedule());

                        // remote source requires nodePartitionMap
                        NodePartitionMap nodePartitionMap = partitioningCache.apply(partitioningHandle);
                        if (groupedExecutionForStage) {
                            checkState(connectorPartitionHandles.size() == nodePartitionMap.getBucketToPartition().length);
                        }
                        stageNodeList = nodePartitionMap.getPartitionToNode();
                        bucketNodeMap = nodePartitionMap.asBucketNodeMap();
                    }

                    return new FixedSourcePartitionedScheduler(
                            stageExecution,
                            splitSources,
                            fragment.getStageExecutionDescriptor(),
                            schedulingOrder,
                            stageNodeList,
                            bucketNodeMap,
                            splitBatchSize,
                            getConcurrentLifespansPerNode(session),
                            nodeScheduler.createNodeSelector(session, catalogName),
                            connectorPartitionHandles,
                            dynamicFilterService);
                }
                else {
                    // all sources are remote
                    NodePartitionMap nodePartitionMap = partitioningCache.apply(partitioningHandle);
                    List<InternalNode> partitionToNode = nodePartitionMap.getPartitionToNode();
                    // todo this should asynchronously wait a standard timeout period before failing
                    checkCondition(!partitionToNode.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");
                    return new FixedCountScheduler(stageExecution, partitionToNode);
                }
            }
        }

        private static ListenableFuture<Void> whenAllStages(Collection<StreamingStageExecution> stages, Predicate<StreamingStageExecution.State> predicate)
        {
            checkArgument(!stages.isEmpty(), "stages is empty");
            Set<StageId> stageIds = stages.stream()
                    .map(StreamingStageExecution::getStageId)
                    .collect(toCollection(Sets::newConcurrentHashSet));
            SettableFuture<Void> future = SettableFuture.create();

            for (StreamingStageExecution stage : stages) {
                stage.addStateChangeListener(state -> {
                    if (predicate.test(state) && stageIds.remove(stage.getStageId()) && stageIds.isEmpty()) {
                        future.set(null);
                    }
                });
            }

            return future;
        }

        private static Map<PlanFragmentId, OutputBufferManager> createOutputBufferManagers(
                Map<PlanFragmentId, OutputBufferManager> coordinatorBuffers,
                Collection<SqlStageExecution> stageExecutions,
                Map<PlanFragmentId, Optional<int[]>> bucketToPartitionMap)
        {
            ImmutableMap.Builder<PlanFragmentId, OutputBufferManager> result = ImmutableMap.builder();
            result.putAll(coordinatorBuffers);
            for (SqlStageExecution stageExecution : stageExecutions) {
                PlanFragmentId fragmentId = stageExecution.getFragment().getId();
                PartitioningHandle partitioningHandle = stageExecution.getFragment().getPartitioningScheme().getPartitioning().getHandle();
                OutputBufferManager outputBufferManager;
                if (partitioningHandle.equals(FIXED_BROADCAST_DISTRIBUTION)) {
                    verify(!coordinatorBuffers.containsKey(fragmentId));
                    outputBufferManager = new BroadcastOutputBufferManager();
                }
                else if (partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
                    verify(!coordinatorBuffers.containsKey(fragmentId));
                    outputBufferManager = new ScaledOutputBufferManager();
                }
                else {
                    Optional<int[]> bucketToPartition = bucketToPartitionMap.get(fragmentId);
                    checkArgument(bucketToPartition.isPresent(), "bucketToPartition is expected to be present for fragment: %s", fragmentId);
                    int partitionCount = Ints.max(bucketToPartition.get()) + 1;
                    outputBufferManager = new PartitionedOutputBufferManager(partitioningHandle, partitionCount);
                    OutputBufferManager coordinatorAssignedBuffers = coordinatorBuffers.get(fragmentId);
                    verify(outputBufferManager.getOutputBuffers().equals(coordinatorAssignedBuffers.getOutputBuffers()));
                }
                if (!coordinatorBuffers.containsKey(fragmentId)) {
                    result.put(fragmentId, outputBufferManager);
                }
            }
            return result.build();
        }

        private static Map<PlanFragmentId, Optional<int[]>> createBucketToPartitionMap(
                Map<PlanFragmentId, Optional<int[]>> coordinatorMappings,
                List<SqlStageExecution> distributedStages,
                Function<PartitioningHandle, NodePartitionMap> partitioningCache)
        {
            ImmutableMap.Builder<PlanFragmentId, Optional<int[]>> result = ImmutableMap.builder();
            result.putAll(coordinatorMappings);
            for (SqlStageExecution stage : distributedStages) {
                PlanFragment fragment = stage.getFragment();
                Optional<int[]> bucketToPartition = getBucketToPartition(fragment.getPartitioning(), partitioningCache, fragment.getRoot(), fragment.getRemoteSourceNodes());
                for (RemoteSourceNode remoteSourceNode : stage.getFragment().getRemoteSourceNodes()) {
                    for (PlanFragmentId sourceFragment : remoteSourceNode.getSourceFragmentIds()) {
                        result.put(sourceFragment, bucketToPartition);
                    }
                }
            }
            return result.build();
        }

        private static Optional<int[]> getBucketToPartition(
                PartitioningHandle partitioningHandle,
                Function<PartitioningHandle, NodePartitionMap> partitioningCache,
                PlanNode fragmentRoot,
                List<RemoteSourceNode> remoteSourceNodes)
        {
            if (partitioningHandle.equals(SOURCE_DISTRIBUTION) || partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
                return Optional.of(new int[1]);
            }
            else if (searchFrom(fragmentRoot).where(node -> node instanceof TableScanNode).findFirst().isPresent()) {
                if (remoteSourceNodes.stream().allMatch(node -> node.getExchangeType() == REPLICATE)) {
                    return Optional.empty();
                }
                else {
                    // remote source requires nodePartitionMap
                    NodePartitionMap nodePartitionMap = partitioningCache.apply(partitioningHandle);
                    return Optional.of(nodePartitionMap.getBucketToPartition());
                }
            }
            else {
                NodePartitionMap nodePartitionMap = partitioningCache.apply(partitioningHandle);
                List<InternalNode> partitionToNode = nodePartitionMap.getPartitionToNode();
                // todo this should asynchronously wait a standard timeout period before failing
                checkCondition(!partitionToNode.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");
                return Optional.of(nodePartitionMap.getBucketToPartition());
            }
        }

        public void schedule()
        {
            checkState(started.compareAndSet(false, true), "already started");

            try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
                while (!executionSchedule.isFinished()) {
                    List<ListenableFuture<Void>> blockedStages = new ArrayList<>();
                    for (StreamingStageExecution stage : executionSchedule.getStagesToSchedule()) {
                        stage.beginScheduling();

                        // perform some scheduling work
                        ScheduleResult result = stageSchedulers.get(stage.getStageId())
                                .schedule();

                        // modify parent and children based on the results of the scheduling
                        if (result.isFinished()) {
                            stage.schedulingComplete();
                        }
                        else if (!result.getBlocked().isDone()) {
                            blockedStages.add(result.getBlocked());
                        }
                        schedulerStats.getSplitsScheduledPerIteration().add(result.getSplitsScheduled());
                        if (result.getBlockedReason().isPresent()) {
                            switch (result.getBlockedReason().get()) {
                                case WRITER_SCALING:
                                    // no-op
                                    break;
                                case WAITING_FOR_SOURCE:
                                    schedulerStats.getWaitingForSource().update(1);
                                    break;
                                case SPLIT_QUEUES_FULL:
                                    schedulerStats.getSplitQueuesFull().update(1);
                                    break;
                                case MIXED_SPLIT_QUEUES_FULL_AND_WAITING_FOR_SOURCE:
                                case NO_ACTIVE_DRIVER_GROUP:
                                    break;
                                default:
                                    throw new UnsupportedOperationException("Unknown blocked reason: " + result.getBlockedReason().get());
                            }
                        }
                    }

                    // wait for a state change and then schedule again
                    if (!blockedStages.isEmpty()) {
                        try (TimeStat.BlockTimer timer = schedulerStats.getSleepTime().time()) {
                            tryGetFutureValue(whenAnyComplete(blockedStages), 1, SECONDS);
                        }
                        for (ListenableFuture<Void> blockedStage : blockedStages) {
                            blockedStage.cancel(true);
                        }
                    }
                }

                for (StreamingStageExecution stage : stageExecutions) {
                    StreamingStageExecution.State state = stage.getState();
                    if (state != SCHEDULED && state != RUNNING && state != FLUSHING && !state.isDone()) {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Scheduling is complete, but stage %s is in state %s", stage.getStageId(), state));
                    }
                }
            }
            catch (Throwable t) {
                queryStateMachine.transitionToFailed(t);
                throw t;
            }
            finally {
                RuntimeException closeError = new RuntimeException();
                for (StageScheduler scheduler : stageSchedulers.values()) {
                    try {
                        scheduler.close();
                    }
                    catch (Throwable t) {
                        queryStateMachine.transitionToFailed(t);
                        // Self-suppression not permitted
                        if (closeError != t) {
                            closeError.addSuppressed(t);
                        }
                    }
                }
                if (closeError.getSuppressed().length > 0) {
                    throw closeError;
                }
            }
        }

        public void cancelStage(StageId stageId)
        {
            for (StreamingStageExecution execution : stageExecutions) {
                if (execution.getStageId().equals(stageId)) {
                    execution.cancel();
                    break;
                }
            }
        }

        public void cancel()
        {
            stageExecutions.forEach(StreamingStageExecution::cancel);
        }

        public void abort()
        {
            stageExecutions.forEach(StreamingStageExecution::abort);
        }
    }
}
