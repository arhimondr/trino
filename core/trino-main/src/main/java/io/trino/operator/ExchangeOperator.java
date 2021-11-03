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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.slice.Slice;
import io.trino.connector.CatalogName;
import io.trino.execution.TaskFailureListener;
import io.trino.execution.buffer.PagesSerde;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.execution.buffer.SerializedPage;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.Split;
import io.trino.exchange.ShuffleServiceManager;
import io.trino.spi.Page;
import io.trino.spi.connector.UpdatablePageSource;
import io.trino.spi.exchange.ShuffleInput;
import io.trino.spi.exchange.ShuffleService;
import io.trino.split.RemoteSplit;
import io.trino.split.RemoteSplit.RemoteTaskInput;
import io.trino.split.RemoteSplit.ShuffleServiceInput;
import io.trino.sql.planner.plan.PlanNodeId;

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.execution.buffer.PagesSerdeUtil.readSerializedPage;
import static java.util.Objects.requireNonNull;

public class ExchangeOperator
        implements SourceOperator
{
    public static final CatalogName REMOTE_CONNECTOR_ID = new CatalogName("$remote");

    public static class ExchangeOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId sourceId;
        private final ExchangeClientSupplier exchangeClientSupplier;
        private final PagesSerdeFactory serdeFactory;
        private final RetryPolicy retryPolicy;
        private final ShuffleServiceManager shuffleServiceManager;
        private PageInput pageInput;
        private boolean closed;

        public ExchangeOperatorFactory(
                int operatorId,
                PlanNodeId sourceId,
                ExchangeClientSupplier exchangeClientSupplier,
                PagesSerdeFactory serdeFactory,
                RetryPolicy retryPolicy,
                ShuffleServiceManager shuffleServiceManager)
        {
            this.operatorId = operatorId;
            this.sourceId = sourceId;
            this.exchangeClientSupplier = exchangeClientSupplier;
            this.serdeFactory = serdeFactory;
            this.retryPolicy = requireNonNull(retryPolicy, "retryPolicy is null");
            this.shuffleServiceManager = requireNonNull(shuffleServiceManager, "shuffleServiceManager is null");
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            TaskContext taskContext = driverContext.getPipelineContext().getTaskContext();
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, sourceId, ExchangeOperator.class.getSimpleName());
            LocalMemoryContext memoryContext = driverContext.getPipelineContext().localSystemMemoryContext();
            if (pageInput == null) {
                pageInput = new LazyPageInput(exchangeClientSupplier, memoryContext, taskContext::sourceTaskFailed, retryPolicy, shuffleServiceManager);
            }
            return new ExchangeOperator(
                    operatorContext,
                    sourceId,
                    pageInput,
                    serdeFactory.createPagesSerde());
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final PlanNodeId sourceId;
    private final PageInput pageInput;
    private final PagesSerde serde;
    private ListenableFuture<Void> isBlocked = NOT_BLOCKED;

    public ExchangeOperator(
            OperatorContext operatorContext,
            PlanNodeId sourceId,
            PageInput pageInput,
            PagesSerde serde)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.pageInput = requireNonNull(pageInput, "pageInput is null");
        this.serde = requireNonNull(serde, "serde is null");

        operatorContext.setInfoSupplier(pageInput::getInfo);
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split)
    {
        requireNonNull(split, "split is null");
        checkArgument(split.getCatalogName().equals(REMOTE_CONNECTOR_ID), "split is not a remote split");

        pageInput.addSplit((RemoteSplit) split.getConnectorSplit());

        return Optional::empty;
    }

    @Override
    public void noMoreSplits()
    {
        pageInput.noMoreSplits();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        close();
    }

    @Override
    public boolean isFinished()
    {
        return pageInput.isFinished();
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        // Avoid registering a new callback in the ExchangeClient when one is already pending
        if (isBlocked.isDone()) {
            isBlocked = pageInput.isBlocked();
            if (isBlocked.isDone()) {
                isBlocked = NOT_BLOCKED;
            }
        }
        return isBlocked;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException(getClass().getName() + " cannot take input");
    }

    @Override
    public Page getOutput()
    {
        SerializedPage page = pageInput.pollPage();
        if (page == null) {
            return null;
        }

        operatorContext.recordNetworkInput(page.getSizeInBytes(), page.getPositionCount());

        Page deserializedPage = serde.deserialize(page);
        operatorContext.recordProcessedInput(deserializedPage.getSizeInBytes(), page.getPositionCount());

        return deserializedPage;
    }

    @Override
    public void close()
    {
        pageInput.close();
    }

    private interface ExchangeDataSource
            extends Closeable
    {
        SerializedPage pollPage();

        boolean isFinished();

        ListenableFuture<Void> isBlocked();

        void addSplit(RemoteSplit remoteSplit);

        void noMoreSplits();

        OperatorInfo getInfo();

        @Override
        void close();
    }

    private static class LazyExchangeDataSource
            implements PageInput
    {
        private final ExchangeClientSupplier exchangeClientSupplier;
        private final LocalMemoryContext systemMemoryContext;
        private final TaskFailureListener taskFailureListener;
        private final RetryPolicy retryPolicy;
        private final ShuffleServiceManager shuffleServiceManager;

        private final SettableFuture<Void> initializationFuture = SettableFuture.create();
        private final AtomicReference<PageInput> delegate = new AtomicReference<>();
        private final AtomicBoolean closed = new AtomicBoolean();

        private LazyPageInput(
                ExchangeClientSupplier exchangeClientSupplier,
                LocalMemoryContext systemMemoryContext,
                TaskFailureListener taskFailureListener,
                RetryPolicy retryPolicy,
                ShuffleServiceManager shuffleServiceManager)
        {
            this.exchangeClientSupplier = requireNonNull(exchangeClientSupplier, "exchangeClientSupplier is null");
            this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
            this.taskFailureListener = requireNonNull(taskFailureListener, "taskFailureListener is null");
            this.retryPolicy = requireNonNull(retryPolicy, "retryPolicy is null");
            this.shuffleServiceManager = requireNonNull(shuffleServiceManager, "shuffleServiceManager is null");
        }

        @Override
        public SerializedPage pollPage()
        {
            if (closed.get()) {
                return null;
            }

            PageInput pageInput = delegate.get();
            if (pageInput == null) {
                return null;
            }
            return pageInput.pollPage();
        }

        @Override
        public boolean isFinished()
        {
            if (closed.get()) {
                return true;
            }
            PageInput pageInput = delegate.get();
            if (pageInput == null) {
                return false;
            }
            return pageInput.isFinished();
        }

        @Override
        public ListenableFuture<Void> isBlocked()
        {
            if (closed.get()) {
                return immediateVoidFuture();
            }
            if (!initializationFuture.isDone()) {
                return initializationFuture;
            }
            PageInput pageInput = delegate.get();
            if (pageInput == null) {
                return immediateVoidFuture();
            }
            return pageInput.isBlocked();
        }

        @Override
        public void addSplit(RemoteSplit remoteSplit)
        {
            SettableFuture<Void> future = null;
            synchronized (this) {
                if (closed.get()) {
                    return;
                }
                PageInput pageInput = delegate.get();
                if (pageInput == null) {
                    if (remoteSplit.getRemoteSplitInput() instanceof RemoteTaskInput) {
                        ExchangeClient exchangeClient = exchangeClientSupplier.get(systemMemoryContext, taskFailureListener, retryPolicy);
                        pageInput = new ExchangeClientPageInput(exchangeClient);
                        pageInput.addSplit(remoteSplit);
                    }
                    else if (remoteSplit.getRemoteSplitInput() instanceof ShuffleServiceInput) {
                        ShuffleServiceInput shuffleServiceInput = (ShuffleServiceInput) remoteSplit.getRemoteSplitInput();
                        ShuffleService shuffleService = shuffleServiceManager.getShuffleService();
                        ShuffleInput shuffleInput = shuffleService.createInput(shuffleServiceInput.getShufflePartitionHandles());
                        pageInput = new ShuffleServicePageInput(shuffleInput, systemMemoryContext);
                    }
                    else {
                        throw new IllegalArgumentException("Unexpected split: " + remoteSplit);
                    }
                    delegate.set(pageInput);
                    future = initializationFuture;
                }
                else {
                    pageInput.addSplit(remoteSplit);
                }
            }

            if (future != null) {
                future.set(null);
            }
        }

        @Override
        public synchronized void noMoreSplits()
        {
            if (closed.get()) {
                return;
            }
            PageInput pageInput = delegate.get();
            if (pageInput != null) {
                pageInput.noMoreSplits();
            }
            else {
                close();
            }
        }

        @Override
        public OperatorInfo getInfo()
        {
            PageInput pageInput = delegate.get();
            if (pageInput == null) {
                return null;
            }
            return pageInput.getInfo();
        }

        @Override
        public void close()
        {
            SettableFuture<Void> future;
            synchronized (this) {
                if (!closed.compareAndSet(false, true)) {
                    return;
                }
                PageInput pageInput = delegate.get();
                if (pageInput != null) {
                    pageInput.close();
                }
                future = initializationFuture;
            }
            future.set(null);
        }
    }

    private static class ExchangeClientPageInput
            implements PageInput
    {
        private final ExchangeClient exchangeClient;

        private ExchangeClientPageInput(ExchangeClient exchangeClient)
        {
            this.exchangeClient = requireNonNull(exchangeClient, "exchangeClient is null");
        }

        @Override
        public SerializedPage pollPage()
        {
            return exchangeClient.pollPage();
        }

        @Override
        public boolean isFinished()
        {
            return exchangeClient.isFinished();
        }

        @Override
        public ListenableFuture<Void> isBlocked()
        {
            return exchangeClient.isBlocked();
        }

        @Override
        public void addSplit(RemoteSplit remoteSplit)
        {
            RemoteTaskInput taskInput = (RemoteTaskInput) remoteSplit.getRemoteSplitInput();
            exchangeClient.addLocation(taskInput.getTaskId(), taskInput.getLocation());
        }

        @Override
        public void noMoreSplits()
        {
            exchangeClient.noMoreLocations();
        }

        @Override
        public OperatorInfo getInfo()
        {
            return exchangeClient.getStatus();
        }

        @Override
        public void close()
        {
            exchangeClient.close();
        }
    }

    private static class ExternalExchangeDataSource
            implements PageInput
    {
        private final ShuffleInput shuffleInput;
        private final LocalMemoryContext systemMemoryContext;

        private ShuffleServicePageInput(ShuffleInput shuffleInput, LocalMemoryContext systemMemoryContext)
        {
            this.shuffleInput = requireNonNull(shuffleInput, "shuffleInput is null");
            this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
        }

        @Override
        public SerializedPage pollPage()
        {
            Slice data = shuffleInput.pool();
            systemMemoryContext.setBytes(shuffleInput.getSystemMemoryUsage());
            if (data == null) {
                return null;
            }
            // TODO: Avoid extra memory copy
            return readSerializedPage(data.getInput());
        }

        @Override
        public boolean isFinished()
        {
            return shuffleInput.isFinished();
        }

        @Override
        public ListenableFuture<Void> isBlocked()
        {
            return asVoid(toListenableFuture(shuffleInput.isBlocked()));
        }

        @Override
        public void addSplit(RemoteSplit remoteSplit)
        {
            // ignore
        }

        @Override
        public void noMoreSplits()
        {
            // ignore
        }

        @Override
        public OperatorInfo getInfo()
        {
            return null;
        }

        @Override
        public void close()
        {
            shuffleInput.close();
        }
    }
}
