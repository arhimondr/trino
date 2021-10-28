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
import io.trino.exchange.ExchangeManagerRegistry;
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
import io.trino.execution.StateMachine;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.TableExecuteContextManager;
import io.trino.execution.TableInfo;
import io.trino.execution.TaskFailureListener;
import io.trino.execution.TaskId;
import io.trino.execution.TaskManager;
import io.trino.execution.TaskStatus;
import io.trino.failuredetector.FailureDetector;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableProperties;
import io.trino.metadata.TableSchema;
import io.trino.operator.RetryPolicy;
import io.trino.server.DynamicFilterService;
import io.trino.spi.ErrorCode;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPartitionHandle;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeManager;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Lists.reverse;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.trino.SystemSessionProperties.getConcurrentLifespansPerNode;
import static io.trino.SystemSessionProperties.getHashPartitionCount;
import static io.trino.SystemSessionProperties.getRetryAttempts;
import static io.trino.SystemSessionProperties.getRetryDelay;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.SystemSessionProperties.getWriterMinSize;
import static io.trino.connector.CatalogName.isInternalSystemConnector;
import static io.trino.execution.BasicStageStats.aggregateBasicStageStats;
import static io.trino.execution.QueryState.FINISHING;
import static io.trino.execution.SqlStageExecution.createSqlStageExecution;
import static io.trino.execution.scheduler.PipelinedStageExecution.State.ABORTED;
import static io.trino.execution.scheduler.PipelinedStageExecution.State.CANCELED;
import static io.trino.execution.scheduler.PipelinedStageExecution.State.FAILED;
import static io.trino.execution.scheduler.PipelinedStageExecution.State.FINISHED;
import static io.trino.execution.scheduler.PipelinedStageExecution.State.FLUSHING;
import static io.trino.execution.scheduler.PipelinedStageExecution.State.RUNNING;
import static io.trino.execution.scheduler.PipelinedStageExecution.State.SCHEDULED;
import static io.trino.execution.scheduler.PipelinedStageExecution.createPipelinedStageExecution;
import static io.trino.execution.scheduler.SourcePartitionedScheduler.newSourcePartitionedSchedulerAsStageScheduler;
import static io.trino.spi.ErrorType.USER_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static io.trino.spi.StandardErrorCode.REMOTE_TASK_FAILED;
import static io.trino.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.Failures.toFailure;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
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
    private final TableExecuteContextManager tableExecuteContextManager;
    private final SplitSourceFactory splitSourceFactory;
    private final ExchangeManagerRegistry exchangeManagerRegistry;
    private final TaskSourceFactory taskSourceFactory;

    private final StageExecutions stages;
    private final CoordinatorStagesScheduler coordinatorStagesScheduler;

    private final RetryPolicy retryPolicy;
    private final int maxRetryAttempts;
    private final AtomicInteger currentAttempt = new AtomicInteger();
    private final Duration retryDelay;

    private final AtomicBoolean started = new AtomicBoolean();

    @GuardedBy("this")
    private final AtomicReference<DistributedStagesScheduler> distributedStagesScheduler = new AtomicReference<>();
    @GuardedBy("this")
    private Future<Void> distributedStagesSchedulingTask;

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
            TableExecuteContextManager tableExecuteContextManager,
            Metadata metadata,
            SplitSourceFactory splitSourceFactory,
            TaskManager coordinatorTaskManager,
            ExchangeManagerRegistry exchangeManagerRegistry,
            TaskSourceFactory taskSourceFactory)
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
        this.tableExecuteContextManager = requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");
        this.splitSourceFactory = requireNonNull(splitSourceFactory, "splitSourceFactory is null");
        this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
        this.taskSourceFactory = requireNonNull(taskSourceFactory, "taskSourceFactory is null");

        stages = StageExecutions.create(
                queryStateMachine,
                queryStateMachine.getSession(),
                metadata,
                remoteTaskFactory,
                nodeTaskMap,
                queryExecutor,
                schedulerStats,
                plan,
                summarizeTaskInfo);

        coordinatorStagesScheduler = CoordinatorStagesScheduler.create(
                queryStateMachine,
                nodeScheduler,
                stages,
                failureDetector,
                schedulerExecutor,
                dynamicFilterService,
                distributedStagesScheduler,
                coordinatorTaskManager);

        retryPolicy = getRetryPolicy(queryStateMachine.getSession());
        maxRetryAttempts = getRetryAttempts(queryStateMachine.getSession());
        retryDelay = getRetryDelay(queryStateMachine.getSession());
    }

    public synchronized void start()
    {
        if (!started.compareAndSet(false, true)) {
            return;
        }

        if (queryStateMachine.isDone()) {
            return;
        }

        // when query is done or any time a stage completes, attempt to transition query to "final query info ready"
        queryStateMachine.addStateChangeListener(state -> {
            if (!state.isDone()) {
                return;
            }

            DistributedStagesScheduler distributedStagesScheduler;
            // synchronize to wait on distributed scheduler creation if it is currently in process
            synchronized (this) {
                distributedStagesScheduler = this.distributedStagesScheduler.get();
            }

            if (state == QueryState.FINISHED) {
                coordinatorStagesScheduler.cancel();
                if (distributedStagesScheduler != null) {
                    distributedStagesScheduler.cancel();
                }
                stages.finish();
            }
            else if (state == QueryState.FAILED) {
                coordinatorStagesScheduler.abort();
                if (distributedStagesScheduler != null) {
                    distributedStagesScheduler.abort();
                }
                stages.abort();
            }

            queryStateMachine.updateQueryInfo(Optional.ofNullable(getStageInfo()));
        });

        coordinatorStagesScheduler.schedule();

        Optional<DistributedStagesScheduler> distributedStagesScheduler = createDistributedStagesScheduler(currentAttempt.get());
        distributedStagesScheduler.ifPresent(scheduler -> distributedStagesSchedulingTask = executor.submit(scheduler::schedule, null));
    }

    private synchronized Optional<DistributedStagesScheduler> createDistributedStagesScheduler(int attempt)
    {
        if (queryStateMachine.isDone()) {
            return Optional.empty();
        }

        DistributedStagesScheduler distributedStagesScheduler;
        switch (retryPolicy) {
            case TASK:
                ExchangeManager exchangeManager = exchangeManagerRegistry.getExchangeManager();
                distributedStagesScheduler = FaultTolerantDistributedStagesScheduler.create(
                        queryStateMachine,
                        stages,
                        failureDetector,
                        taskSourceFactory,
                        exchangeManager,
                        nodePartitioningManager,
                        coordinatorStagesScheduler.getTaskLifecycleListener(),
                        maxRetryAttempts,
                        schedulerExecutor,
                        schedulerStats,
                        nodeScheduler);
                break;
            case QUERY:
            case NONE:
                distributedStagesScheduler = PipelinedDistributedStagesScheduler.create(
                        queryStateMachine,
                        schedulerStats,
                        nodeScheduler,
                        nodePartitioningManager,
                        stages,
                        coordinatorStagesScheduler,
                        executionPolicy,
                        failureDetector,
                        schedulerExecutor,
                        splitSourceFactory,
                        splitBatchSize,
                        dynamicFilterService,
                        tableExecuteContextManager,
                        retryPolicy,
                        attempt);
                break;
            default:
                throw new IllegalArgumentException("Unexpected retry policy: " + retryPolicy);
        }

        this.distributedStagesScheduler.set(distributedStagesScheduler);
        distributedStagesScheduler.addStateChangeListener(state -> {
            if (queryStateMachine.getQueryState() == QueryState.STARTING && state != DistributedStagesSchedulerState.PLANNED) {
                queryStateMachine.transitionToRunning();
            }

            if (state.isDone() && !state.isFailure()) {
                stages.getDistributedStagesInTopologicalOrder().forEach(execution -> stages.get(execution.getStageId()).finish());
            }

            if (stages.getCoordinatorStagesInTopologicalOrder().isEmpty()) {
                if (state == DistributedStagesSchedulerState.FINISHED) {
                    queryStateMachine.transitionToFinishing();
                }
                else if (state == DistributedStagesSchedulerState.CANCELED) {
                    // output stage was canceled
                    queryStateMachine.transitionToCanceled();
                }
            }

            if (state == DistributedStagesSchedulerState.FAILED) {
                StageFailureInfo stageFailureInfo = distributedStagesScheduler.getFailureCause()
                        .orElseGet(() -> new StageFailureInfo(toFailure(new VerifyException("distributedStagesScheduler is failed but failure cause is not present")), Optional.empty()));
                ErrorCode errorCode = stageFailureInfo.getFailureInfo().getErrorCode();
                if (retryPolicy != RetryPolicy.QUERY || currentAttempt.get() >= maxRetryAttempts || (errorCode != null && errorCode.getType() == USER_ERROR)) {
                    stages.getDistributedStagesInTopologicalOrder().forEach(stage -> {
                        if (stageFailureInfo.getFailedStageId().isPresent() && stageFailureInfo.getFailedStageId().get().equals(stage.getStageId())) {
                            stage.fail(stageFailureInfo.getFailureInfo().toException());
                        }
                        else {
                            stage.abort();
                        }
                    });
                    queryStateMachine.transitionToFailed(stageFailureInfo.getFailureInfo().toException());
                }
                else {
                    currentAttempt.incrementAndGet();
                    scheduleRetryWithDelay(retryDelay);
                }
            }
        });
        return Optional.of(distributedStagesScheduler);
    }

    private void scheduleRetryWithDelay(Duration delay)
    {
        try {
            schedulerExecutor.schedule(this::scheduleRetry, delay.toMillis(), MILLISECONDS);
        }
        catch (Throwable t) {
            queryStateMachine.transitionToFailed(t);
        }
    }

    private synchronized void scheduleRetry()
    {
        try {
            checkState(distributedStagesSchedulingTask != null, "schedulingTask is expected to be set");

            // give current scheduler some time to terminate, usually it is expected to be done right away
            distributedStagesSchedulingTask.get(5, MINUTES);

            Optional<DistributedStagesScheduler> distributedStagesScheduler = createDistributedStagesScheduler(currentAttempt.get());
            distributedStagesScheduler.ifPresent(scheduler -> distributedStagesSchedulingTask = executor.submit(scheduler::schedule, null));
        }
        catch (Throwable t) {
            queryStateMachine.transitionToFailed(t);
        }
    }

    public synchronized void cancelStage(StageId stageId)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            coordinatorStagesScheduler.cancelStage(stageId);
            DistributedStagesScheduler distributedStagesScheduler = this.distributedStagesScheduler.get();
            if (distributedStagesScheduler != null) {
                distributedStagesScheduler.cancelStage(stageId);
            }
        }
    }

    public synchronized void abort()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            coordinatorStagesScheduler.abort();
            DistributedStagesScheduler distributedStagesScheduler = this.distributedStagesScheduler.get();
            if (distributedStagesScheduler != null) {
                distributedStagesScheduler.abort();
            }
        }
    }

    public BasicStageStats getBasicStageStats()
    {
        return stages.getBasicStageStats();
    }

    public StageInfo getStageInfo()
    {
        return stages.getStageInfo();
    }

    public long getUserMemoryReservation()
    {
        return stages.getUserMemoryReservation();
    }

    public long getTotalMemoryReservation()
    {
        return stages.getTotalMemoryReservation();
    }

    public Duration getTotalCpuTime()
    {
        return stages.getTotalCpuTime();
    }

    private static class StageExecutions
    {
        private final QueryStateMachine queryStateMachine;
        private final Map<StageId, SqlStageExecution> allStages;
        private final List<SqlStageExecution> coordinatorStagesInTopologicalOrder;
        private final List<SqlStageExecution> distributedStagesInTopologicalOrder;
        private final StageId rootStageId;
        private final Map<StageId, Set<StageId>> children;
        private final Map<StageId, StageId> parents;

        private static StageExecutions create(
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
            ImmutableMap.Builder<StageId, SqlStageExecution> allStages = ImmutableMap.builder();
            ImmutableList.Builder<SqlStageExecution> coordinatorStagesInTopologicalOrder = ImmutableList.builder();
            ImmutableList.Builder<SqlStageExecution> distributedStagesInTopologicalOrder = ImmutableList.builder();
            StageId rootStageId = null;
            ImmutableMap.Builder<StageId, Set<StageId>> children = ImmutableMap.builder();
            ImmutableMap.Builder<StageId, StageId> parents = ImmutableMap.builder();
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
                    coordinatorStagesInTopologicalOrder.add(stageExecution);
                }
                else {
                    distributedStagesInTopologicalOrder.add(stageExecution);
                }
                if (rootStageId == null) {
                    rootStageId = stageId;
                }
                Set<StageId> sourceStages = planNode.getChildren().stream()
                        .map(stage -> getStageId(session.getQueryId(), stage.getFragment().getId()))
                        .collect(toImmutableSet());
                children.put(stageId, sourceStages);
                sourceStages.forEach(child -> parents.put(child, stageId));
            }
            StageExecutions stageExecutions = new StageExecutions(
                    queryStateMachine,
                    allStages.build(),
                    coordinatorStagesInTopologicalOrder.build(),
                    distributedStagesInTopologicalOrder.build(),
                    rootStageId,
                    children.build(),
                    parents.build());
            stageExecutions.initialize();
            return stageExecutions;
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

        private StageExecutions(
                QueryStateMachine queryStateMachine,
                Map<StageId, SqlStageExecution> allStages,
                List<SqlStageExecution> coordinatorStagesInTopologicalOrder,
                List<SqlStageExecution> distributedStagesInTopologicalOrder,
                StageId rootStageId,
                Map<StageId, Set<StageId>> children,
                Map<StageId, StageId> parents)
        {
            this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
            this.allStages = ImmutableMap.copyOf(requireNonNull(allStages, "allStages is null"));
            this.coordinatorStagesInTopologicalOrder = ImmutableList.copyOf(requireNonNull(coordinatorStagesInTopologicalOrder, "coordinatorStagesInTopologicalOrder is null"));
            this.distributedStagesInTopologicalOrder = ImmutableList.copyOf(requireNonNull(distributedStagesInTopologicalOrder, "distributedStagesInTopologicalOrder is null"));
            this.rootStageId = requireNonNull(rootStageId, "rootStageId is null");
            this.children = ImmutableMap.copyOf(requireNonNull(children, "children is null"));
            this.parents = ImmutableMap.copyOf(requireNonNull(parents, "parents is null"));
        }

        // this is a separate method to ensure that the `this` reference is not leaked during construction
        private void initialize()
        {
            for (SqlStageExecution stage : allStages.values()) {
                stage.addFinalStageInfoListener(status -> queryStateMachine.updateQueryInfo(Optional.ofNullable(getStageInfo())));
            }
        }

        public void finish()
        {
            allStages.values().forEach(SqlStageExecution::finish);
        }

        public void abort()
        {
            allStages.values().forEach(SqlStageExecution::abort);
        }

        public List<SqlStageExecution> getCoordinatorStagesInTopologicalOrder()
        {
            return coordinatorStagesInTopologicalOrder;
        }

        public List<SqlStageExecution> getDistributedStagesInTopologicalOrder()
        {
            return distributedStagesInTopologicalOrder;
        }

        public SqlStageExecution getOutputStage()
        {
            return allStages.get(rootStageId);
        }

        public SqlStageExecution get(PlanFragmentId fragmentId)
        {
            return get(getStageId(queryStateMachine.getQueryId(), fragmentId));
        }

        public SqlStageExecution get(StageId stageId)
        {
            return requireNonNull(allStages.get(stageId), () -> "stage not found: " + stageId);
        }

        public Set<SqlStageExecution> getChildren(PlanFragmentId fragmentId)
        {
            return getChildren(getStageId(queryStateMachine.getQueryId(), fragmentId));
        }

        public Set<SqlStageExecution> getChildren(StageId stageId)
        {
            return children.get(stageId).stream()
                    .map(this::get)
                    .collect(toImmutableSet());
        }

        public Optional<SqlStageExecution> getParent(PlanFragmentId fragmentId)
        {
            return getParent(getStageId(queryStateMachine.getQueryId(), fragmentId));
        }

        public Optional<SqlStageExecution> getParent(StageId stageId)
        {
            return Optional.ofNullable(parents.get(stageId)).map(allStages::get);
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
            List<StageInfo> childStages = children.get(stageId).stream()
                    .map(childStageId -> buildStageInfo(childStageId, stageInfos))
                    .collect(toImmutableList());
            if (childStages.isEmpty()) {
                return parent;
            }
            return new StageInfo(
                    parent.getStageId(),
                    parent.getState(),
                    parent.getPlan(),
                    parent.isCoordinatorOnly(),
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

    private static class QueryOutputTaskLifecycleListener
            implements TaskLifecycleListener
    {
        private final QueryStateMachine queryStateMachine;

        private QueryOutputTaskLifecycleListener(QueryStateMachine queryStateMachine)
        {
            this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
        }

        @Override
        public void taskCreated(PlanFragmentId fragmentId, RemoteTask task)
        {
            Map<TaskId, URI> bufferLocations = ImmutableMap.of(
                    task.getTaskId(),
                    uriBuilderFrom(task.getTaskStatus().getSelf())
                            .appendPath("results")
                            .appendPath("0").build());
            queryStateMachine.updateOutputLocations(bufferLocations, false);
        }

        @Override
        public void noMoreTasks(PlanFragmentId fragmentId)
        {
            queryStateMachine.updateOutputLocations(ImmutableMap.of(), true);
        }
    }

    private static class CoordinatorStagesScheduler
    {
        private static final int[] SINGLE_PARTITION = new int[] {0};

        private final QueryStateMachine queryStateMachine;
        private final NodeScheduler nodeScheduler;
        private final Map<PlanFragmentId, OutputBufferManager> outputBuffersForStagesConsumedByCoordinator;
        private final Map<PlanFragmentId, Optional<int[]>> bucketToPartitionForStagesConsumedByCoordinator;
        private final TaskLifecycleListener taskLifecycleListener;
        private final StageExecutions stages;
        private final List<PipelinedStageExecution> executions;
        private final DynamicFilterService dynamicFilterService;
        private final AtomicReference<DistributedStagesScheduler> distributedStagesScheduler;
        private final TaskManager coordinatorTaskManager;

        private final AtomicBoolean scheduled = new AtomicBoolean();

        public static CoordinatorStagesScheduler create(
                QueryStateMachine queryStateMachine,
                NodeScheduler nodeScheduler,
                StageExecutions stages,
                FailureDetector failureDetector,
                Executor executor,
                DynamicFilterService dynamicFilterService,
                AtomicReference<DistributedStagesScheduler> distributedStagesScheduler,
                TaskManager coordinatorTaskManager)
        {
            Map<PlanFragmentId, OutputBufferManager> outputBuffersForStagesConsumedByCoordinator = createOutputBuffersForStagesConsumedByCoordinator(stages);
            Map<PlanFragmentId, Optional<int[]>> bucketToPartitionForStagesConsumedByCoordinator = createBucketToPartitionForStagesConsumedByCoordinator(stages);

            TaskLifecycleListener taskLifecycleListener = new QueryOutputTaskLifecycleListener(queryStateMachine);
            // create executions
            ImmutableList.Builder<PipelinedStageExecution> executions = ImmutableList.builder();
            for (SqlStageExecution stageExecution : stages.getCoordinatorStagesInTopologicalOrder()) {
                PipelinedStageExecution execution = createPipelinedStageExecution(
                        stageExecution,
                        outputBuffersForStagesConsumedByCoordinator,
                        taskLifecycleListener,
                        failureDetector,
                        executor,
                        bucketToPartitionForStagesConsumedByCoordinator.get(stageExecution.getFragment().getId()),
                        0);
                executions.add(execution);
                taskLifecycleListener = execution.getTaskLifecycleListener();
            }

            CoordinatorStagesScheduler coordinatorStagesScheduler = new CoordinatorStagesScheduler(
                    queryStateMachine,
                    nodeScheduler,
                    outputBuffersForStagesConsumedByCoordinator,
                    bucketToPartitionForStagesConsumedByCoordinator,
                    taskLifecycleListener,
                    stages,
                    executions.build(),
                    dynamicFilterService,
                    distributedStagesScheduler,
                    coordinatorTaskManager);
            coordinatorStagesScheduler.initialize();

            return coordinatorStagesScheduler;
        }

        private static Map<PlanFragmentId, OutputBufferManager> createOutputBuffersForStagesConsumedByCoordinator(StageExecutions stages)
        {
            ImmutableMap.Builder<PlanFragmentId, OutputBufferManager> result = ImmutableMap.builder();

            // create output buffer for output stage
            SqlStageExecution outputStage = stages.getOutputStage();
            result.put(outputStage.getFragment().getId(), createSingleStreamOutputBuffer(outputStage));

            // create output buffers for stages consumed by coordinator
            for (SqlStageExecution coordinatorStage : stages.getCoordinatorStagesInTopologicalOrder()) {
                for (SqlStageExecution child : stages.getChildren(coordinatorStage.getStageId())) {
                    result.put(child.getFragment().getId(), createSingleStreamOutputBuffer(child));
                }
            }

            return result.build();
        }

        private static OutputBufferManager createSingleStreamOutputBuffer(SqlStageExecution stage)
        {
            PartitioningHandle partitioningHandle = stage.getFragment().getPartitioningScheme().getPartitioning().getHandle();
            checkArgument(partitioningHandle.isSingleNode(), "partitioning is expected to be single node: " + partitioningHandle);
            return new PartitionedOutputBufferManager(partitioningHandle, 1);
        }

        private static Map<PlanFragmentId, Optional<int[]>> createBucketToPartitionForStagesConsumedByCoordinator(StageExecutions stages)
        {
            ImmutableMap.Builder<PlanFragmentId, Optional<int[]>> result = ImmutableMap.builder();

            SqlStageExecution outputStage = stages.getOutputStage();
            result.put(outputStage.getFragment().getId(), Optional.of(SINGLE_PARTITION));

            for (SqlStageExecution coordinatorStage : stages.getCoordinatorStagesInTopologicalOrder()) {
                for (SqlStageExecution child : stages.getChildren(coordinatorStage.getStageId())) {
                    result.put(child.getFragment().getId(), Optional.of(SINGLE_PARTITION));
                }
            }

            return result.build();
        }

        private CoordinatorStagesScheduler(
                QueryStateMachine queryStateMachine,
                NodeScheduler nodeScheduler,
                Map<PlanFragmentId, OutputBufferManager> outputBuffersForStagesConsumedByCoordinator,
                Map<PlanFragmentId, Optional<int[]>> bucketToPartitionForStagesConsumedByCoordinator,
                TaskLifecycleListener taskLifecycleListener,
                StageExecutions stages,
                List<PipelinedStageExecution> executions,
                DynamicFilterService dynamicFilterService,
                AtomicReference<DistributedStagesScheduler> distributedStagesScheduler,
                TaskManager coordinatorTaskManager)
        {
            this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
            this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
            this.outputBuffersForStagesConsumedByCoordinator = ImmutableMap.copyOf(requireNonNull(outputBuffersForStagesConsumedByCoordinator, "outputBuffersForStagesConsumedByCoordinator is null"));
            this.bucketToPartitionForStagesConsumedByCoordinator = ImmutableMap.copyOf(requireNonNull(bucketToPartitionForStagesConsumedByCoordinator, "bucketToPartitionForStagesConsumedByCoordinator is null"));
            this.taskLifecycleListener = requireNonNull(taskLifecycleListener, "taskLifecycleListener is null");
            this.stages = requireNonNull(stages, "stages is null");
            this.executions = ImmutableList.copyOf(requireNonNull(executions, "executions is null"));
            this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
            this.distributedStagesScheduler = requireNonNull(distributedStagesScheduler, "distributedStagesScheduler is null");
            this.coordinatorTaskManager = requireNonNull(coordinatorTaskManager, "coordinatorTaskManager is null");
        }

        private void initialize()
        {
            for (PipelinedStageExecution execution : executions) {
                execution.addStateChangeListener(state -> {
                    if (queryStateMachine.isDone()) {
                        return;
                    }
                    if (!state.canScheduleMoreTasks()) {
                        dynamicFilterService.stageCannotScheduleMoreTasks(execution.getStageId(), execution.getAllTasks().size());
                    }
                    // if any coordinator stage failed transition directly to failure
                    if (state == FAILED) {
                        RuntimeException failureCause = execution.getFailureCause()
                                .map(ExecutionFailureInfo::toException)
                                .orElseGet(() -> new VerifyException(format("stage execution for stage %s is failed by failure cause is not present", execution.getStageId())));
                        stages.get(execution.getStageId()).fail(failureCause);
                        queryStateMachine.transitionToFailed(failureCause);
                    }
                    else if (state == ABORTED) {
                        // this should never happen, since abort can only be triggered in query clean up after the query is finished
                        stages.get(execution.getStageId()).abort();
                        queryStateMachine.transitionToFailed(new TrinoException(GENERIC_INTERNAL_ERROR, "Query stage was aborted"));
                    }
                    else if (state.isDone()) {
                        stages.get(execution.getStageId()).finish();
                    }
                });
            }

            for (int currentIndex = 0, nextIndex = 1; nextIndex < executions.size(); currentIndex++, nextIndex++) {
                PipelinedStageExecution current = executions.get(currentIndex);
                PipelinedStageExecution next = executions.get(nextIndex);
                verify(getOnlyElement(stages.getChildren(current.getStageId())).getStageId().equals(next.getStageId()), "coordinator stages are expected to consume only a singe other coordinator stage");
                current.addStateChangeListener(newState -> {
                    if (newState == FLUSHING || newState.isDone()) {
                        next.cancel();
                    }
                });
            }

            Optional<PipelinedStageExecution> root = executions.isEmpty() ? Optional.empty() : Optional.of(executions.get(0));
            root.ifPresent(execution -> execution.addStateChangeListener(state -> {
                if (state == FINISHED) {
                    queryStateMachine.transitionToFinishing();
                }
                else if (state == CANCELED) {
                    // output stage was canceled
                    queryStateMachine.transitionToCanceled();
                }
            }));

            Optional<PipelinedStageExecution> last = executions.isEmpty() ? Optional.empty() : Optional.of(executions.get(executions.size() - 1));
            last.ifPresent(execution -> execution.addStateChangeListener(newState -> {
                if (newState == FLUSHING || newState.isDone()) {
                    DistributedStagesScheduler distributedStagesScheduler = this.distributedStagesScheduler.get();
                    if (distributedStagesScheduler != null) {
                        distributedStagesScheduler.cancel();
                    }
                }
            }));
        }

        public void schedule()
        {
            if (!scheduled.compareAndSet(false, true)) {
                return;
            }

            TaskFailureReporter failureReporter = new TaskFailureReporter(distributedStagesScheduler);
            queryStateMachine.addOutputTaskFailureListener(failureReporter);

            InternalNode coordinator = nodeScheduler.createNodeSelector(queryStateMachine.getSession(), Optional.empty()).selectCurrentNode();
            for (PipelinedStageExecution execution : executions) {
                Optional<RemoteTask> remoteTask = execution.scheduleTask(
                        coordinator,
                        0,
                        ImmutableMultimap.of(),
                        ImmutableMultimap.of());
                execution.schedulingComplete();
                remoteTask.ifPresent(task -> coordinatorTaskManager.addSourceTaskFailureListener(task.getTaskId(), failureReporter));
            }
        }

        public Map<PlanFragmentId, OutputBufferManager> getOutputBuffersForStagesConsumedByCoordinator()
        {
            return outputBuffersForStagesConsumedByCoordinator;
        }

        public Map<PlanFragmentId, Optional<int[]>> getBucketToPartitionForStagesConsumedByCoordinator()
        {
            return bucketToPartitionForStagesConsumedByCoordinator;
        }

        public TaskLifecycleListener getTaskLifecycleListener()
        {
            return taskLifecycleListener;
        }

        public void cancelStage(StageId stageId)
        {
            for (PipelinedStageExecution execution : executions) {
                if (execution.getStageId().equals(stageId)) {
                    execution.cancel();
                }
            }
        }

        public void cancel()
        {
            executions.forEach(PipelinedStageExecution::cancel);
        }

        public void abort()
        {
            executions.forEach(PipelinedStageExecution::abort);
        }
    }

    private static class TaskFailureReporter
            implements TaskFailureListener
    {
        private final AtomicReference<DistributedStagesScheduler> distributedStagesScheduler;

        private TaskFailureReporter(AtomicReference<DistributedStagesScheduler> distributedStagesScheduler)
        {
            this.distributedStagesScheduler = distributedStagesScheduler;
        }

        @Override
        public void onTaskFailed(TaskId taskId, Throwable failure)
        {
            if (failure instanceof TrinoException && ((TrinoException) failure).getErrorCode() == REMOTE_TASK_FAILED.toErrorCode()) {
                // This error indicates that a downstream task was trying to fetch results from an upstream task that is marked as failed
                // Instead of failing a downstream task let the coordinator handle and report the failure of an upstream task to ensure correct error reporting
                log.info("Task failure discovered while fetching task results: %s", taskId);
                return;
            }
            log.warn(failure, "Reported task failure: %s", taskId);
            DistributedStagesScheduler scheduler = this.distributedStagesScheduler.get();
            if (scheduler != null) {
                scheduler.reportTaskFailure(taskId, failure);
            }
        }
    }

    private interface DistributedStagesScheduler
    {
        void schedule();

        void cancelStage(StageId stageId);

        void cancel();

        void abort();

        void reportTaskFailure(TaskId taskId, Throwable failureCause);

        void addStateChangeListener(StateChangeListener<DistributedStagesSchedulerState> stateChangeListener);

        Optional<StageFailureInfo> getFailureCause();
    }

    private static class PipelinedDistributedStagesScheduler
            implements DistributedStagesScheduler
    {
        private final DistributedStagesSchedulerStateMachine stateMachine;
        private final QueryStateMachine queryStateMachine;
        private final SplitSchedulerStats schedulerStats;
        private final StageExecutions stages;
        private final ExecutionSchedule executionSchedule;
        private final Map<StageId, StageScheduler> stageSchedulers;
        private final Map<StageId, PipelinedStageExecution> stageExecutions;
        private final DynamicFilterService dynamicFilterService;

        private final AtomicBoolean started = new AtomicBoolean();

        public static PipelinedDistributedStagesScheduler create(
                QueryStateMachine queryStateMachine,
                SplitSchedulerStats schedulerStats,
                NodeScheduler nodeScheduler,
                NodePartitioningManager nodePartitioningManager,
                StageExecutions stages,
                CoordinatorStagesScheduler coordinatorStagesScheduler,
                ExecutionPolicy executionPolicy,
                FailureDetector failureDetector,
                ScheduledExecutorService executor,
                SplitSourceFactory splitSourceFactory,
                int splitBatchSize,
                DynamicFilterService dynamicFilterService,
                TableExecuteContextManager tableExecuteContextManager,
                RetryPolicy retryPolicy,
                int attempt)
        {
            DistributedStagesSchedulerStateMachine stateMachine = new DistributedStagesSchedulerStateMachine(queryStateMachine.getQueryId(), executor);

            Map<PartitioningHandle, NodePartitionMap> partitioningCacheMap = new HashMap<>();
            Function<PartitioningHandle, NodePartitionMap> partitioningCache = partitioningHandle ->
                    partitioningCacheMap.computeIfAbsent(partitioningHandle, handle -> nodePartitioningManager.getNodePartitioningMap(queryStateMachine.getSession(), handle));

            Map<PlanFragmentId, Optional<int[]>> bucketToPartitionMap = createBucketToPartitionMap(
                    coordinatorStagesScheduler.getBucketToPartitionForStagesConsumedByCoordinator(),
                    stages,
                    partitioningCache);
            Map<PlanFragmentId, OutputBufferManager> outputBufferManagers = createOutputBufferManagers(
                    coordinatorStagesScheduler.getOutputBuffersForStagesConsumedByCoordinator(),
                    stages,
                    bucketToPartitionMap);

            TaskLifecycleListener coordinatorTaskLifecycleListener = coordinatorStagesScheduler.getTaskLifecycleListener();
            if (retryPolicy != RetryPolicy.NONE) {
                // when retries are enabled only close exchange clients on coordinator when the query is finished
                TaskLifecycleListenerBridge taskLifecycleListenerBridge = new TaskLifecycleListenerBridge(coordinatorTaskLifecycleListener);
                coordinatorTaskLifecycleListener = taskLifecycleListenerBridge;
                stateMachine.addStateChangeListener(state -> {
                    if (state == DistributedStagesSchedulerState.FINISHED) {
                        taskLifecycleListenerBridge.notifyNoMoreSourceTasks();
                    }
                });
            }

            Map<StageId, PipelinedStageExecution> stageExecutions = new HashMap<>();
            for (SqlStageExecution stage : stages.getDistributedStagesInTopologicalOrder()) {
                Optional<SqlStageExecution> parent = stages.getParent(stage.getStageId());
                TaskLifecycleListener taskLifecycleListener;
                if (parent.isEmpty() || parent.get().getFragment().getPartitioning().isCoordinatorOnly()) {
                    // output will be consumed by coordinator
                    taskLifecycleListener = coordinatorTaskLifecycleListener;
                }
                else {
                    StageId parentStageId = parent.get().getStageId();
                    PipelinedStageExecution execution = requireNonNull(stageExecutions.get(parentStageId), () -> "execution is null for stage: " + parentStageId);
                    taskLifecycleListener = execution.getTaskLifecycleListener();
                }

                PlanFragment fragment = stage.getFragment();
                PipelinedStageExecution execution = createPipelinedStageExecution(
                        stages.get(fragment.getId()),
                        outputBufferManagers,
                        taskLifecycleListener,
                        failureDetector,
                        executor,
                        bucketToPartitionMap.get(fragment.getId()),
                        attempt);
                stageExecutions.put(stage.getStageId(), execution);
            }

            ImmutableMap.Builder<StageId, StageScheduler> stageSchedulers = ImmutableMap.builder();
            for (PipelinedStageExecution stageExecution : stageExecutions.values()) {
                List<PipelinedStageExecution> children = stages.getChildren(stageExecution.getStageId()).stream()
                        .map(stage -> requireNonNull(stageExecutions.get(stage.getStageId()), () -> "stage execution not found for stage: " + stage))
                        .collect(toImmutableList());
                StageScheduler scheduler = createStageScheduler(
                        queryStateMachine,
                        stageExecution,
                        splitSourceFactory,
                        children,
                        partitioningCache,
                        nodeScheduler,
                        nodePartitioningManager,
                        splitBatchSize,
                        dynamicFilterService,
                        executor,
                        tableExecuteContextManager);
                stageSchedulers.put(stageExecution.getStageId(), scheduler);
            }

            PipelinedDistributedStagesScheduler distributedStagesScheduler = new PipelinedDistributedStagesScheduler(
                    stateMachine,
                    queryStateMachine,
                    schedulerStats,
                    stages,
                    executionPolicy.createExecutionSchedule(stageExecutions.values()),
                    stageSchedulers.build(),
                    ImmutableMap.copyOf(stageExecutions),
                    dynamicFilterService);
            distributedStagesScheduler.initialize();
            return distributedStagesScheduler;
        }

        private static Map<PlanFragmentId, Optional<int[]>> createBucketToPartitionMap(
                Map<PlanFragmentId, Optional<int[]>> bucketToPartitionForStagesConsumedByCoordinator,
                StageExecutions stages,
                Function<PartitioningHandle, NodePartitionMap> partitioningCache)
        {
            ImmutableMap.Builder<PlanFragmentId, Optional<int[]>> result = ImmutableMap.builder();
            result.putAll(bucketToPartitionForStagesConsumedByCoordinator);
            for (SqlStageExecution stage : stages.getDistributedStagesInTopologicalOrder()) {
                PlanFragment fragment = stage.getFragment();
                Optional<int[]> bucketToPartition = getBucketToPartition(fragment.getPartitioning(), partitioningCache, fragment.getRoot(), fragment.getRemoteSourceNodes());
                for (SqlStageExecution child : stages.getChildren(stage.getStageId())) {
                    result.put(child.getFragment().getId(), bucketToPartition);
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

        private static Map<PlanFragmentId, OutputBufferManager> createOutputBufferManagers(
                Map<PlanFragmentId, OutputBufferManager> outputBuffersForStagesConsumedByCoordinator,
                StageExecutions executions,
                Map<PlanFragmentId, Optional<int[]>> bucketToPartitionMap)
        {
            ImmutableMap.Builder<PlanFragmentId, OutputBufferManager> result = ImmutableMap.builder();
            result.putAll(outputBuffersForStagesConsumedByCoordinator);
            for (SqlStageExecution parent : executions.getDistributedStagesInTopologicalOrder()) {
                for (SqlStageExecution execution : executions.getChildren(parent.getStageId())) {
                    PlanFragmentId fragmentId = execution.getFragment().getId();
                    PartitioningHandle partitioningHandle = execution.getFragment().getPartitioningScheme().getPartitioning().getHandle();

                    OutputBufferManager outputBufferManager;
                    if (partitioningHandle.equals(FIXED_BROADCAST_DISTRIBUTION)) {
                        outputBufferManager = new BroadcastOutputBufferManager();
                    }
                    else if (partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
                        outputBufferManager = new ScaledOutputBufferManager();
                    }
                    else {
                        Optional<int[]> bucketToPartition = bucketToPartitionMap.get(fragmentId);
                        checkArgument(bucketToPartition.isPresent(), "bucketToPartition is expected to be present for fragment: %s", fragmentId);
                        int partitionCount = Ints.max(bucketToPartition.get()) + 1;
                        outputBufferManager = new PartitionedOutputBufferManager(partitioningHandle, partitionCount);
                    }
                    result.put(fragmentId, outputBufferManager);
                }
            }
            return result.build();
        }

        private static StageScheduler createStageScheduler(
                QueryStateMachine queryStateMachine,
                PipelinedStageExecution stageExecution,
                SplitSourceFactory splitSourceFactory,
                List<PipelinedStageExecution> childStages,
                Function<PartitioningHandle, NodePartitionMap> partitioningCache,
                NodeScheduler nodeScheduler,
                NodePartitioningManager nodePartitioningManager,
                int splitBatchSize,
                DynamicFilterService dynamicFilterService,
                ScheduledExecutorService executor,
                TableExecuteContextManager tableExecuteContextManager)
        {
            Session session = queryStateMachine.getSession();
            PlanFragment fragment = stageExecution.getFragment();
            PartitioningHandle partitioningHandle = fragment.getPartitioning();
            Map<PlanNodeId, SplitSource> splitSources = splitSourceFactory.createSplitSources(session, fragment);
            if (!splitSources.isEmpty()) {
                queryStateMachine.addStateChangeListener(new StateChangeListener<>()
                {
                    private final AtomicReference<Collection<SplitSource>> splitSourcesReference = new AtomicReference<>(splitSources.values());

                    @Override
                    public void stateChanged(QueryState newState)
                    {
                        if (newState.isDone()) {
                            // ensure split sources are closed and release memory
                            Collection<SplitSource> sources = splitSourcesReference.getAndSet(null);
                            if (sources != null) {
                                closeSplitSources(sources);
                            }
                        }
                    }
                });
            }
            if (partitioningHandle.equals(SOURCE_DISTRIBUTION)) {
                // nodes are selected dynamically based on the constraints of the splits and the system load
                Entry<PlanNodeId, SplitSource> entry = getOnlyElement(splitSources.entrySet());
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
                        tableExecuteContextManager,
                        () -> childStages.stream().anyMatch(PipelinedStageExecution::isAnyTaskBlocked));
            }
            else if (partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
                Supplier<Collection<TaskStatus>> sourceTasksProvider = () -> childStages.stream()
                        .map(PipelinedStageExecution::getTaskStatuses)
                        .flatMap(List::stream)
                        .collect(toImmutableList());
                Supplier<Collection<TaskStatus>> writerTasksProvider = stageExecution::getTaskStatuses;

                ScaledWriterScheduler scheduler = new ScaledWriterScheduler(
                        stageExecution,
                        sourceTasksProvider,
                        writerTasksProvider,
                        nodeScheduler.createNodeSelector(session, Optional.empty()),
                        executor,
                        getWriterMinSize(session));

                whenAllStages(childStages, PipelinedStageExecution.State::isDone)
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
                            dynamicFilterService,
                            tableExecuteContextManager);
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

        private static ListenableFuture<Void> whenAllStages(Collection<PipelinedStageExecution> stages, Predicate<PipelinedStageExecution.State> predicate)
        {
            checkArgument(!stages.isEmpty(), "stages is empty");
            Set<StageId> stageIds = stages.stream()
                    .map(PipelinedStageExecution::getStageId)
                    .collect(toCollection(Sets::newConcurrentHashSet));
            SettableFuture<Void> future = SettableFuture.create();

            for (PipelinedStageExecution stage : stages) {
                stage.addStateChangeListener(state -> {
                    if (predicate.test(state) && stageIds.remove(stage.getStageId()) && stageIds.isEmpty()) {
                        future.set(null);
                    }
                });
            }

            return future;
        }

        private PipelinedDistributedStagesScheduler(
                DistributedStagesSchedulerStateMachine stateMachine,
                QueryStateMachine queryStateMachine,
                SplitSchedulerStats schedulerStats,
                StageExecutions stages,
                ExecutionSchedule executionSchedule,
                Map<StageId, StageScheduler> stageSchedulers,
                Map<StageId, PipelinedStageExecution> stageExecutions,
                DynamicFilterService dynamicFilterService)
        {
            this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
            this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
            this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
            this.stages = requireNonNull(stages, "stages is null");
            this.executionSchedule = requireNonNull(executionSchedule, "executionSchedule is null");
            this.stageSchedulers = ImmutableMap.copyOf(requireNonNull(stageSchedulers, "stageSchedulers is null"));
            this.stageExecutions = ImmutableMap.copyOf(requireNonNull(stageExecutions, "stageExecutions is null"));
            this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
        }

        private void initialize()
        {
            for (PipelinedStageExecution execution : stageExecutions.values()) {
                List<PipelinedStageExecution> children = stages.getChildren(execution.getStageId()).stream()
                        .map(stage -> requireNonNull(stageExecutions.get(stage.getStageId()), () -> "stage execution not found for stage: " + stage))
                        .collect(toImmutableList());
                if (!children.isEmpty()) {
                    execution.addStateChangeListener(newState -> {
                        if (newState == FLUSHING || newState.isDone()) {
                            children.forEach(PipelinedStageExecution::cancel);
                        }
                    });
                }
            }

            Set<StageId> finishedStages = newConcurrentHashSet();
            for (PipelinedStageExecution execution : stageExecutions.values()) {
                execution.addStateChangeListener(state -> {
                    if (stateMachine.getState().isDone()) {
                        return;
                    }
                    // TODO: support dynamic filter for failure retries
                    if (!state.canScheduleMoreTasks()) {
                        dynamicFilterService.stageCannotScheduleMoreTasks(execution.getStageId(), execution.getAllTasks().size());
                    }
                    if (!execution.getAllTasks().isEmpty()) {
                        stateMachine.transitionToRunning();
                    }
                    if (state == FAILED) {
                        RuntimeException failureCause = execution.getFailureCause()
                                .map(ExecutionFailureInfo::toException)
                                .orElseGet(() -> new VerifyException(format("stage execution for stage %s is failed by failure cause is not present", execution.getStageId())));
                        fail(failureCause, Optional.of(execution.getStageId()));
                    }
                    else if (state.isDone()) {
                        finishedStages.add(execution.getStageId());
                        if (finishedStages.containsAll(stageExecutions.keySet())) {
                            stateMachine.transitionToFinished();
                        }
                    }
                });
            }
        }

        @Override
        public void schedule()
        {
            checkState(started.compareAndSet(false, true), "already started");

            try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
                while (!executionSchedule.isFinished()) {
                    List<ListenableFuture<Void>> blockedStages = new ArrayList<>();
                    for (PipelinedStageExecution stage : executionSchedule.getStagesToSchedule()) {
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

                for (PipelinedStageExecution stage : stageExecutions.values()) {
                    PipelinedStageExecution.State state = stage.getState();
                    if (state != SCHEDULED && state != RUNNING && state != FLUSHING && !state.isDone()) {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Scheduling is complete, but stage %s is in state %s", stage.getStageId(), state));
                    }
                }
            }
            catch (Throwable t) {
                fail(t, Optional.empty());
            }
            finally {
                RuntimeException closeError = new RuntimeException();
                for (StageScheduler scheduler : stageSchedulers.values()) {
                    try {
                        scheduler.close();
                    }
                    catch (Throwable t) {
                        fail(t, Optional.empty());
                        // Self-suppression not permitted
                        if (closeError != t) {
                            closeError.addSuppressed(t);
                        }
                    }
                }
            }
        }

        @Override
        public void cancelStage(StageId stageId)
        {
            PipelinedStageExecution execution = stageExecutions.get(stageId);
            if (execution != null) {
                execution.cancel();
            }
        }

        @Override
        public void cancel()
        {
            stateMachine.transitionToCanceled();
            stageExecutions.values().forEach(PipelinedStageExecution::cancel);
        }

        @Override
        public void abort()
        {
            stateMachine.transitionToAborted();
            stageExecutions.values().forEach(PipelinedStageExecution::abort);
        }

        public void fail(Throwable failureCause, Optional<StageId> failedStageId)
        {
            stateMachine.transitionToFailed(failureCause, failedStageId);
            stageExecutions.values().forEach(PipelinedStageExecution::abort);
        }

        @Override
        public void reportTaskFailure(TaskId taskId, Throwable failureCause)
        {
            PipelinedStageExecution execution = stageExecutions.get(taskId.getStageId());
            if (execution == null) {
                return;
            }

            List<RemoteTask> tasks = execution.getAllTasks();
            if (tasks.stream().noneMatch(task -> task.getTaskId().equals(taskId))) {
                return;
            }

            execution.failTask(taskId, failureCause);
            stateMachine.transitionToFailed(failureCause, Optional.of(taskId.getStageId()));
            stageExecutions.values().forEach(PipelinedStageExecution::abort);
        }

        @Override
        public void addStateChangeListener(StateChangeListener<DistributedStagesSchedulerState> stateChangeListener)
        {
            stateMachine.addStateChangeListener(stateChangeListener);
        }

        @Override
        public Optional<StageFailureInfo> getFailureCause()
        {
            return stateMachine.getFailureCause();
        }
    }

    private static class FaultTolerantDistributedStagesScheduler
            implements DistributedStagesScheduler
    {
        private final DistributedStagesSchedulerStateMachine stateMachine;
        private final QueryStateMachine queryStateMachine;
        private final List<FaultTolerantStageScheduler> schedulers;
        private final SplitSchedulerStats schedulerStats;
        private final NodeAllocator nodeAllocator;
        private final ScheduledFuture<?> nodeUpdateTask;

        private final AtomicBoolean started = new AtomicBoolean();

        public static FaultTolerantDistributedStagesScheduler create(
                QueryStateMachine queryStateMachine,
                StageExecutions stages,
                FailureDetector failureDetector,
                TaskSourceFactory taskSourceFactory,
                ExchangeManager exchangeManager,
                NodePartitioningManager nodePartitioningManager,
                TaskLifecycleListener coordinatorTaskLifecycleListener,
                int retryAttempts,
                ScheduledExecutorService scheduledExecutorService,
                SplitSchedulerStats schedulerStats,
                NodeScheduler nodeScheduler)
        {
            DistributedStagesSchedulerStateMachine stateMachine = new DistributedStagesSchedulerStateMachine(queryStateMachine.getQueryId(), scheduledExecutorService);

            Session session = queryStateMachine.getSession();
            int hashPartitionCount = getHashPartitionCount(session);
            Map<PartitioningHandle, BucketToPartition> bucketToPartitionCacheMap = new HashMap<>();
            Function<PartitioningHandle, BucketToPartition> bucketToPartitionMapCache = partitioningHandle ->
                    bucketToPartitionCacheMap.computeIfAbsent(partitioningHandle, handle -> createBucketToPartitionMap(session, hashPartitionCount, handle, nodePartitioningManager));

            ImmutableList.Builder<FaultTolerantStageScheduler> schedulers = ImmutableList.builder();
            Map<PlanFragmentId, Exchange> exchanges = new HashMap<>();

            FixedCountNodeAllocator nodeAllocator = new FixedCountNodeAllocator(nodeScheduler, session, 1);
            ScheduledFuture<?> nodeUpdateTask = scheduledExecutorService.scheduleAtFixedRate(nodeAllocator::updateNodes, 5, 5, SECONDS);

            try {
                // top to bottom order
                List<SqlStageExecution> distributedStagesInTopologicalOrder = stages.getDistributedStagesInTopologicalOrder();
                // bottom to top order
                List<SqlStageExecution> distributedStagesInReverseTopologicalOrder = reverse(distributedStagesInTopologicalOrder);

                ImmutableSet.Builder<PlanFragmentId> coordinatorConsumedFragmentsBuilder = ImmutableSet.builder();

                for (SqlStageExecution stage : distributedStagesInReverseTopologicalOrder) {
                    PlanFragment fragment = stage.getFragment();
                    Optional<SqlStageExecution> parent = stages.getParent(stage.getStageId());
                    TaskLifecycleListener taskLifecycleListener;
                    Optional<Exchange> exchange;
                    if (parent.isEmpty() || parent.get().getFragment().getPartitioning().isCoordinatorOnly()) {
                        // output will be consumed by coordinator
                        exchange = Optional.empty();
                        taskLifecycleListener = coordinatorTaskLifecycleListener;
                        coordinatorConsumedFragmentsBuilder.add(fragment.getId());
                    }
                    else {
                        // create external exchange
                        exchange = Optional.of(exchangeManager.create(new ExchangeContext(session.getQueryId(), stage.getStageId().getId()), hashPartitionCount));
                        exchanges.put(fragment.getId(), exchange.get());
                        taskLifecycleListener = TaskLifecycleListener.NO_OP;
                    }

                    ImmutableMap.Builder<PlanFragmentId, Exchange> inputExchanges = ImmutableMap.builder();
                    for (SqlStageExecution child : stages.getChildren(fragment.getId())) {
                        PlanFragmentId childFragmentId = child.getFragment().getId();
                        Exchange sourceExchange = exchanges.get(childFragmentId);
                        verify(sourceExchange != null, "exchange not found for fragment: %s", childFragmentId);
                        inputExchanges.put(childFragmentId, sourceExchange);
                    }

                    BucketToPartition inputBucketToPartition = bucketToPartitionMapCache.apply(fragment.getPartitioning());
                    FaultTolerantStageScheduler scheduler = new FaultTolerantStageScheduler(
                            session,
                            stage,
                            failureDetector,
                            taskSourceFactory,
                            nodeAllocator,
                            taskLifecycleListener,
                            exchange,
                            bucketToPartitionMapCache.apply(fragment.getPartitioningScheme().getPartitioning().getHandle()).getBucketToPartitionMap(),
                            inputExchanges.build(),
                            inputBucketToPartition.getBucketToPartitionMap(),
                            inputBucketToPartition.getBucketNodeMap(),
                            retryAttempts);

                    schedulers.add(scheduler);
                }

                Set<PlanFragmentId> coordinatorConsumedFragments = coordinatorConsumedFragmentsBuilder.build();
                stateMachine.addStateChangeListener(state -> {
                    if (state == DistributedStagesSchedulerState.FINISHED) {
                        coordinatorConsumedFragments.forEach(coordinatorTaskLifecycleListener::noMoreTasks);
                    }
                });

                return new FaultTolerantDistributedStagesScheduler(
                        stateMachine,
                        queryStateMachine,
                        schedulers.build(),
                        schedulerStats,
                        nodeAllocator,
                        nodeUpdateTask);
            }
            catch (Throwable t) {
                schedulers.build().forEach(FaultTolerantStageScheduler::abort);

                nodeUpdateTask.cancel(true);
                try {
                    nodeAllocator.close();
                }
                catch (Throwable closeFailure) {
                    if (t != closeFailure) {
                        t.addSuppressed(closeFailure);
                    }
                }

                for (Exchange exchange : exchanges.values()) {
                    try {
                        exchange.close();
                    }
                    catch (Throwable closeFailure) {
                        if (t != closeFailure) {
                            t.addSuppressed(closeFailure);
                        }
                    }
                }
                throw t;
            }
        }

        private static BucketToPartition createBucketToPartitionMap(
                Session session,
                int hashPartitionCount,
                PartitioningHandle partitioningHandle,
                NodePartitioningManager nodePartitioningManager)
        {
            if (partitioningHandle.equals(FIXED_HASH_DISTRIBUTION)) {
                return new BucketToPartition(Optional.of(IntStream.range(0, hashPartitionCount).toArray()), Optional.empty());
            }
            else if (partitioningHandle.getConnectorId().isPresent()) {
                int partitionCount = hashPartitionCount;
                BucketNodeMap bucketNodeMap = nodePartitioningManager.getBucketNodeMap(session, partitioningHandle, true);
                if (!bucketNodeMap.isDynamic()) {
                    partitionCount = bucketNodeMap.getNodeCount();
                }
                int bucketCount = bucketNodeMap.getBucketCount();
                int[] bucketToPartition = new int[bucketCount];
                int nextPartitionId = 0;
                for (int bucket = 0; bucket < bucketCount; bucket++) {
                    bucketToPartition[bucket] = nextPartitionId++ % partitionCount;
                }
                return new BucketToPartition(Optional.of(bucketToPartition), Optional.of(bucketNodeMap));
            }
            else {
                return new BucketToPartition(Optional.empty(), Optional.empty());
            }
        }

        private static class BucketToPartition
        {
            private final Optional<int[]> bucketToPartitionMap;
            private final Optional<BucketNodeMap> bucketNodeMap;

            private BucketToPartition(Optional<int[]> bucketToPartitionMap, Optional<BucketNodeMap> bucketNodeMap)
            {
                this.bucketToPartitionMap = requireNonNull(bucketToPartitionMap, "bucketToPartitionMap is null");
                this.bucketNodeMap = requireNonNull(bucketNodeMap, "bucketNodeMap is null");
            }

            public Optional<int[]> getBucketToPartitionMap()
            {
                return bucketToPartitionMap;
            }

            public Optional<BucketNodeMap> getBucketNodeMap()
            {
                return bucketNodeMap;
            }
        }

        private FaultTolerantDistributedStagesScheduler(
                DistributedStagesSchedulerStateMachine stateMachine,
                QueryStateMachine queryStateMachine,
                List<FaultTolerantStageScheduler> schedulers,
                SplitSchedulerStats schedulerStats,
                NodeAllocator nodeAllocator,
                ScheduledFuture<?> nodeUpdateTask)
        {
            this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
            this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
            this.schedulers = requireNonNull(schedulers, "schedulers is null");
            this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
            this.nodeAllocator = requireNonNull(nodeAllocator, "nodeAllocator is null");
            this.nodeUpdateTask = requireNonNull(nodeUpdateTask, "nodeUpdateTask is null");
        }

        @Override
        public void schedule()
        {
            checkState(started.compareAndSet(false, true), "already started");

            try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
                List<ListenableFuture<Void>> blockedStages = new ArrayList<>();
                while (!isFinishingOrDone(queryStateMachine) && !stateMachine.getState().isDone()) {
                    blockedStages.clear();
                    boolean atLeastOneStageIsNotBlocked = false;
                    boolean allFinished = true;
                    for (FaultTolerantStageScheduler scheduler : schedulers) {
                        if (scheduler.isFinished()) {
                            continue;
                        }
                        allFinished = false;
                        ListenableFuture<Void> blocked = scheduler.isBlocked();
                        if (!blocked.isDone()) {
                            blockedStages.add(blocked);
                            continue;
                        }
                        try {
                            scheduler.schedule();
                        }
                        catch (Throwable t) {
                            stateMachine.transitionToFailed(t, Optional.of(scheduler.getStageId()));
                            return;
                        }
                        blocked = scheduler.isBlocked();
                        if (!blocked.isDone()) {
                            blockedStages.add(blocked);
                        }
                        else {
                            atLeastOneStageIsNotBlocked = true;
                        }
                    }
                    if (allFinished) {
                        stateMachine.transitionToFinished();
                        return;
                    }
                    // wait for a state change and then schedule again
                    if (!atLeastOneStageIsNotBlocked && !blockedStages.isEmpty()) {
                        try (TimeStat.BlockTimer timer = schedulerStats.getSleepTime().time()) {
                            try {
                                tryGetFutureValue(whenAnyComplete(blockedStages), 1, SECONDS);
                            }
                            catch (CancellationException e) {
                                log.debug("Future cancelled");
                            }
                        }
                    }
                }
            }
            catch (Throwable t) {
                stateMachine.transitionToFailed(t, Optional.empty());
                schedulers.forEach(FaultTolerantStageScheduler::abort);
                closeNodeAllocator();
            }
        }

        private static boolean isFinishingOrDone(QueryStateMachine queryStateMachine)
        {
            QueryState queryState = queryStateMachine.getQueryState();
            return queryState == FINISHING || queryState.isDone();
        }

        @Override
        public void cancelStage(StageId stageId)
        {
            // single stage cancellation is not supported in fault tolerant mode
        }

        @Override
        public void cancel()
        {
            stateMachine.transitionToCanceled();
            schedulers.forEach(FaultTolerantStageScheduler::cancel);
            closeNodeAllocator();
        }

        @Override
        public void abort()
        {
            stateMachine.transitionToAborted();
            schedulers.forEach(FaultTolerantStageScheduler::abort);
            closeNodeAllocator();
        }

        private void closeNodeAllocator()
        {
            nodeUpdateTask.cancel(true);
            try {
                nodeAllocator.close();
            }
            catch (Throwable t) {
                log.warn(t, "Error closing node allocator for query: %s", queryStateMachine.getQueryId());
            }
        }

        @Override
        public void reportTaskFailure(TaskId taskId, Throwable failureCause)
        {
            for (FaultTolerantStageScheduler scheduler : schedulers) {
                if (scheduler.getStageId().equals(taskId.getStageId())) {
                    scheduler.reportTaskFailure(taskId, failureCause);
                }
            }
        }

        @Override
        public void addStateChangeListener(StateChangeListener<DistributedStagesSchedulerState> stateChangeListener)
        {
            stateMachine.addStateChangeListener(stateChangeListener);
        }

        @Override
        public Optional<StageFailureInfo> getFailureCause()
        {
            return stateMachine.getFailureCause();
        }
    }

    private enum DistributedStagesSchedulerState
    {
        PLANNED(false, false),
        RUNNING(false, false),
        FINISHED(true, false),
        CANCELED(true, false),
        ABORTED(true, true),
        FAILED(true, true);

        public static final Set<DistributedStagesSchedulerState> TERMINAL_STATES = Stream.of(DistributedStagesSchedulerState.values()).filter(DistributedStagesSchedulerState::isDone).collect(toImmutableSet());

        private final boolean doneState;
        private final boolean failureState;

        DistributedStagesSchedulerState(boolean doneState, boolean failureState)
        {
            checkArgument(!failureState || doneState, "%s is a non-done failure state", name());
            this.doneState = doneState;
            this.failureState = failureState;
        }

        /**
         * Is this a terminal state.
         */
        public boolean isDone()
        {
            return doneState;
        }

        /**
         * Is this a non-success terminal state.
         */
        public boolean isFailure()
        {
            return failureState;
        }
    }

    private static class DistributedStagesSchedulerStateMachine
    {
        private final QueryId queryId;
        private final StateMachine<DistributedStagesSchedulerState> state;
        private final AtomicReference<StageFailureInfo> failureCause = new AtomicReference<>();

        public DistributedStagesSchedulerStateMachine(QueryId queryId, Executor executor)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            requireNonNull(executor, "executor is null");
            state = new StateMachine<>("Distributed stages scheduler", executor, DistributedStagesSchedulerState.PLANNED, DistributedStagesSchedulerState.TERMINAL_STATES);
        }

        public DistributedStagesSchedulerState getState()
        {
            return state.get();
        }

        public boolean transitionToRunning()
        {
            return state.setIf(DistributedStagesSchedulerState.RUNNING, currentState -> !currentState.isDone());
        }

        public boolean transitionToFinished()
        {
            return state.setIf(DistributedStagesSchedulerState.FINISHED, currentState -> !currentState.isDone());
        }

        public boolean transitionToCanceled()
        {
            return state.setIf(DistributedStagesSchedulerState.CANCELED, currentState -> !currentState.isDone());
        }

        public boolean transitionToAborted()
        {
            return state.setIf(DistributedStagesSchedulerState.ABORTED, currentState -> !currentState.isDone());
        }

        public boolean transitionToFailed(Throwable throwable, Optional<StageId> failedStageId)
        {
            requireNonNull(throwable, "throwable is null");

            failureCause.compareAndSet(null, new StageFailureInfo(toFailure(throwable), failedStageId));
            boolean failed = state.setIf(DistributedStagesSchedulerState.FAILED, currentState -> !currentState.isDone());
            if (failed) {
                log.error(throwable, "Failure in distributed stage for query %s", queryId);
            }
            else {
                log.debug(throwable, "Failure in distributed stage for query %s after finished", queryId);
            }
            return failed;
        }

        public Optional<StageFailureInfo> getFailureCause()
        {
            return Optional.ofNullable(failureCause.get());
        }

        /**
         * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
         * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
         * possible notifications are observed out of order due to the asynchronous execution.
         */
        public void addStateChangeListener(StateChangeListener<DistributedStagesSchedulerState> stateChangeListener)
        {
            state.addStateChangeListener(stateChangeListener);
        }
    }

    private static class TaskLifecycleListenerBridge
            implements TaskLifecycleListener
    {
        private final TaskLifecycleListener listener;

        @GuardedBy("this")
        private final Set<PlanFragmentId> noMoreSourceTasks = new HashSet<>();
        @GuardedBy("this")
        private boolean done;

        private TaskLifecycleListenerBridge(TaskLifecycleListener listener)
        {
            this.listener = requireNonNull(listener, "listener is null");
        }

        @Override
        public synchronized void taskCreated(PlanFragmentId fragmentId, RemoteTask task)
        {
            checkState(!done, "unexpected state");
            listener.taskCreated(fragmentId, task);
        }

        @Override
        public synchronized void noMoreTasks(PlanFragmentId fragmentId)
        {
            checkState(!done, "unexpected state");
            noMoreSourceTasks.add(fragmentId);
        }

        public synchronized void notifyNoMoreSourceTasks()
        {
            checkState(!done, "unexpected state");
            done = true;
            noMoreSourceTasks.forEach(listener::noMoreTasks);
        }
    }

    private static class StageFailureInfo
    {
        private final ExecutionFailureInfo failureInfo;
        private final Optional<StageId> failedStageId;

        private StageFailureInfo(ExecutionFailureInfo failureInfo, Optional<StageId> failedStageId)
        {
            this.failureInfo = requireNonNull(failureInfo, "failureInfo is null");
            this.failedStageId = requireNonNull(failedStageId, "failedStageId is null");
        }

        public ExecutionFailureInfo getFailureInfo()
        {
            return failureInfo;
        }

        public Optional<StageId> getFailedStageId()
        {
            return failedStageId;
        }
    }
}
