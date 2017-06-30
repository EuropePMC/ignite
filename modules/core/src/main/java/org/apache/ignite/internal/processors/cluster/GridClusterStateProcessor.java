/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cluster;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.ExchangeActions;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridChangeGlobalStateMessageResponse;
import org.apache.ignite.internal.processors.cache.StateChangeRequest;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.STATE_PROC;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;

/**
 *
 */
public class GridClusterStateProcessor extends GridProcessorAdapter {
    /** */
    private volatile DiscoveryDataClusterState globalState;

    /** Local action future. */
    private final AtomicReference<GridChangeGlobalStateFuture> stateChangeFut = new AtomicReference<>();

    /** */
    private TransitionOnJoinWaitFuture joinFut;

    /** Process. */
    @GridToStringExclude
    private GridCacheProcessor cacheProc;

    /** Shared context. */
    @GridToStringExclude
    private GridCacheSharedContext<?, ?> sharedCtx;

    /** Listener. */
    private final GridLocalEventListener lsr = new GridLocalEventListener() {
        @Override public void onEvent(Event evt) {
            assert evt != null;

            final DiscoveryEvent e = (DiscoveryEvent)evt;

            assert e.type() == EVT_NODE_LEFT || e.type() == EVT_NODE_FAILED : this;

            final GridChangeGlobalStateFuture f = stateChangeFut.get();

            if (f != null)
                f.initFut.listen(new CI1<IgniteInternalFuture<?>>() {
                    @Override public void apply(IgniteInternalFuture<?> fut) {
                        f.onDiscoveryEvent(e);
                    }
                });
        }
    };

    /**
     * @param ctx Kernal context.
     */
    public GridClusterStateProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @return Cluster state to be used on public API.
     */
    public boolean publicApiActiveState() {
        DiscoveryDataClusterState globalState = this.globalState;

        assert globalState != null;

        if (globalState.transition()) {
            Boolean transitionRes = globalState.transitionResult();

            if (transitionRes != null)
                return transitionRes;
            else
                return !globalState.active();
        }
        else
            return globalState.active();
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        // Start first node as inactive if persistence is enabled.
        boolean activeOnStart = !ctx.config().isPersistentStoreEnabled() && ctx.config().isActiveOnStart();

        globalState = DiscoveryDataClusterState.createState(activeOnStart);

        ctx.event().addLocalEventListener(lsr, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /**
     * @return If transition is in progress returns future which is completed when transition finishes.
     */
    @Nullable public IgniteInternalFuture<Boolean> onLocalJoin(DiscoCache discoCache) {
        if (globalState.transition()) {
            joinFut = new TransitionOnJoinWaitFuture(globalState, discoCache);

            return joinFut;
        }

        return null;
    }

    /**
     * @param node Failed node.
     */
    public void onNodeLeft(ClusterNode node) {
        // TODO GG-12389.
    }

    /**
     * @param msg Message.
     */
    public void onStateFinishMessage(ChangeGlobalStateFinishMessage msg) {
        if (joinFut != null)
            joinFut.onDone(msg.clusterActive());

        if (msg.requestId().equals(globalState.transitionRequestId()))
            globalState = DiscoveryDataClusterState.createState(msg.clusterActive());
    }

    /**
     * @param topVer Current topology version.
     * @param msg Message.
     * @param discoCache Current nodes.
     * @return {@code True} if need start state change process.
     */
    public boolean onStateChangeMessage(AffinityTopologyVersion topVer,
        ChangeGlobalStateMessage msg,
        DiscoCache discoCache) {
        if (globalState.transition()) {
            if (globalState.active() != msg.activate()) {
                GridChangeGlobalStateFuture fut = changeStateFuture(msg);

                if (fut != null)
                    fut.onDone(concurrentStateChangeError(msg.activate()));
            }
            else {
                final GridChangeGlobalStateFuture stateFut = changeStateFuture(msg);

                if (stateFut != null) {
                    // TODO GG-12389, check for result.
                    IgniteInternalFuture<?> exchFut = ctx.cache().context().exchange().affinityReadyFuture(
                        globalState.transitionTopologyVersion());

                    if (exchFut == null)
                        exchFut = new GridFinishedFuture<>();

                    exchFut.listen(new CI1<IgniteInternalFuture<?>>() {
                        @Override public void apply(IgniteInternalFuture<?> exchFut) {
                            stateFut.onDone();
                        }
                    });
                }
            }
        }
        else {
            if (globalState.active() != msg.activate()) {
//                if (!ctx.localNodeId().equals(msg.initiatorNodeId()))
//                    stateChangeFut.compareAndSet(null, new GridChangeGlobalStateFuture(msg.requestId(), msg.activate(), ctx));
// TODO GG-12389
                Set<UUID> nodeIds = U.newHashSet(discoCache.allNodes().size());

                for (ClusterNode node : discoCache.allNodes())
                    nodeIds.add(node.id());

                GridChangeGlobalStateFuture fut = changeStateFuture(msg);

                if (fut != null)
                    fut.setRemaining(nodeIds, topVer.nextMinorVersion());

                globalState = DiscoveryDataClusterState.createTransitionState(msg.activate(),
                    msg.requestId(),
                    topVer,
                    nodeIds);

                ExchangeActions exchangeActions = new ExchangeActions();

                StateChangeRequest req = new StateChangeRequest(msg, topVer.nextMinorVersion());

                exchangeActions.stateChangeRequest(req);

                ctx.cache().onStateChangeRequest(exchangeActions);

                msg.exchangeActions(exchangeActions);

                return true;
            }
            else {
                GridChangeGlobalStateFuture stateFut = changeStateFuture(msg);

                if (stateFut != null)
                    stateFut.onDone();
            }
        }

        return false;
    }

    /**
     * @return Current cluster state, should be called only from discovery thread.
     */
    public DiscoveryDataClusterState clusterState() {
        return globalState;
    }

    /**
     * @param msg State change message.
     * @return Local future for state change process.
     */
    @Nullable private GridChangeGlobalStateFuture changeStateFuture(ChangeGlobalStateMessage msg) {
        if (msg.initiatorNodeId().equals(ctx.localNodeId())) {
            GridChangeGlobalStateFuture fut = stateChangeFut.get();

            if (fut != null && fut.requestId.equals(msg.requestId()))
                return fut;
        }

        return null;
    }

    /**
     * @param activate New state.
     * @return State change error.
     */
    private IgniteCheckedException concurrentStateChangeError(boolean activate) {
        return new IgniteCheckedException("Failed to " + prettyStr(activate) +
            ", because another state change operation is currently in progress: " + prettyStr(!activate));
    }

    /**
     *
     */
    public void cacheProcessorStarted() {
        cacheProc = ctx.cache();
        sharedCtx = cacheProc.context();

        sharedCtx.io().addCacheHandler(
            0, GridChangeGlobalStateMessageResponse.class,
            new CI2<UUID, GridChangeGlobalStateMessageResponse>() {
                @Override public void apply(UUID nodeId, GridChangeGlobalStateMessageResponse msg) {
                    processChangeGlobalStateResponse(nodeId, msg);
                }
            });
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        super.stop(cancel);

        sharedCtx.io().removeHandler(false, 0, GridChangeGlobalStateMessageResponse.class);

        ctx.event().removeLocalEventListener(lsr, EVT_NODE_LEFT, EVT_NODE_FAILED);

        IgniteCheckedException stopErr = new IgniteCheckedException(
            "Node is stopping: " + ctx.igniteInstanceName());

        GridChangeGlobalStateFuture f = stateChangeFut.get();

        if (f != null)
            f.onDone(stopErr);
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryDataExchangeType discoveryDataType() {
        return DiscoveryDataExchangeType.STATE_PROC;
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        if (!dataBag.commonDataCollectedFor(STATE_PROC.ordinal()))
            dataBag.addGridCommonData(STATE_PROC.ordinal(), globalState);
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        DiscoveryDataClusterState state = (DiscoveryDataClusterState)data.commonData();

        assert state != null : data;

        globalState = state;
    }

    /**
     *
     */
    public IgniteInternalFuture<?> changeGlobalState(final boolean activate) {
        if (cacheProc.transactions().tx() != null || sharedCtx.lockedTopologyVersion(null) != null) {
            return new GridFinishedFuture<>(new IgniteCheckedException("Failed to " + prettyStr(activate) +
                " cluster (must invoke the method outside of an active transaction)."));
        }

        DiscoveryDataClusterState curState = globalState;

        if (!curState.transition() && curState.active() == activate)
            return new GridFinishedFuture<>();

        GridChangeGlobalStateFuture startedFut = null;

        GridChangeGlobalStateFuture fut = stateChangeFut.get();

        while (fut == null) {
            fut = new GridChangeGlobalStateFuture(UUID.randomUUID(), activate, ctx);

            if (stateChangeFut.compareAndSet(null, fut)) {
                startedFut = fut;

                break;
            }
            else
                fut = stateChangeFut.get();
        }

        if (startedFut == null) {
            if (fut.activate != activate) {
                return new GridFinishedFuture<>(new IgniteCheckedException("Failed to " + prettyStr(activate) +
                    ", because another state change operation is currently in progress: " + prettyStr(fut.activate)));
            }
            else
                return fut;
        }

        ChangeGlobalStateMessage msg = new ChangeGlobalStateMessage(startedFut.requestId, ctx.localNodeId(), activate);

        try {
            ctx.discovery().sendCustomEvent(msg);

            if (ctx.isStopping())
                startedFut.onDone(new IgniteCheckedException("Failed to execute " + prettyStr(activate) + " request, " +
                    "node is stopping."));
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send global state change request: " + activate, e);

            startedFut.onDone(e);
        }

        return startedFut;
    }

    /**
     * Invoke from exchange future.
     */
    public Exception changeGlobalState(StateChangeRequest req) {
        return req.activate() ? onActivate(req.topologyVersion()) : onDeActivate(req.topologyVersion());
    }

    /**
     * @param exs Exs.
     */
    public void onStateChangeError(Map<UUID, Exception> exs, StateChangeRequest req) {
        assert !F.isEmpty(exs);

        // Revert change if activation request fail.
        if (req.activate()) {
            try {
                cacheProc.onKernalStopCaches(true);

                cacheProc.stopCaches(true);

                sharedCtx.affinity().removeAllCacheInfo();

                ctx.discovery().cleanCachesAndGroups();

                if (!ctx.clientNode()) {
                    sharedCtx.database().onDeActivate(ctx);

                    if (sharedCtx.pageStore() != null)
                        sharedCtx.pageStore().onDeActivate(ctx);

                    if (sharedCtx.wal() != null)
                        sharedCtx.wal().onDeActivate(ctx);
                }
            }
            catch (Exception e) {
                for (Map.Entry<UUID, Exception> entry : exs.entrySet())
                    e.addSuppressed(entry.getValue());

                U.error(log, "Failed to revert activation request changes", e);
            }
        }
        else {
            //todo https://issues.apache.org/jira/browse/IGNITE-5480
        }

        // TODO GG-12389.
        GridChangeGlobalStateFuture af = stateChangeFut.get();

        if (af != null && af.requestId.equals(req.requestId())) {
            IgniteCheckedException e = new IgniteCheckedException(
                "Fail " + prettyStr(req.activate()),
                null,
                false
            );

            for (Map.Entry<UUID, Exception> entry : exs.entrySet())
                e.addSuppressed(entry.getValue());

            af.onDone(e);
        }
    }

    /**
     *
     */
    private Exception onActivate(AffinityTopologyVersion topVer) {
        if (log.isInfoEnabled()) {
            log.info("Start activation process [nodeId=" + ctx.localNodeId() +
                ", client=" + ctx.clientNode() +
                ", topVer=" + topVer + "]");
        }

        try {
            sharedCtx.activate();

            if (log.isInfoEnabled()) {
                log.info("Successfully activated persistence managers [nodeId=" + ctx.localNodeId() +
                    ", client=" + ctx.clientNode() +
                    ", topVer=" + topVer + "]");
            }

            return null;
        }
        catch (Exception e) {
            U.error(log, "Failed to activate persistence managers [nodeId=" + ctx.localNodeId() +
                ", client=" + ctx.clientNode() +
                ", topVer=" + topVer + "]", e);

            return e;
        }
    }

    /**
     *
     */
    private Exception onDeActivate(AffinityTopologyVersion topVer) {
        final boolean client = ctx.clientNode();

        if (log.isInfoEnabled())
            log.info("Starting deactivation [id=" + ctx.localNodeId() + ", client=" +
                client + ", topVer=" + topVer + "]");

        try {
            ctx.dataStructures().onDeActivate(ctx);

            ctx.service().onDeActivate(ctx);

            if (log.isInfoEnabled())
                log.info("Successfully deactivated persistence processors [id=" + ctx.localNodeId() + ", client=" +
                    client + ", topVer=" + topVer + "]");

            return null;
        }
        catch (Exception e) {
            U.error(log, "Failed to execute deactivation callback [nodeId=" + ctx.localNodeId() + ", client=" + client +
                ", topVer=" + topVer + "]", e);

            return e;
        }
    }

    /**
     *
     */
    private void onFinalActivate(final StateChangeRequest req) {
        ctx.closure().runLocalSafe(new Runnable() {
            @Override public void run() {
                boolean client = ctx.clientNode();

                Exception e = null;

                try {
                    ctx.service().onUtilityCacheStarted();

                    ctx.service().onActivate(ctx);

                    ctx.dataStructures().onActivate(ctx);

                    if (log.isInfoEnabled())
                        log.info("Successfully performed final activation steps [nodeId="
                            + ctx.localNodeId() + ", client=" + client + ", topVer=" + req.topologyVersion() + "]");
                }
                catch (Exception ex) {
                    e = ex;

                    U.error(log, "Failed to perform final activation steps [nodeId=" + ctx.localNodeId() +
                        ", client=" + client + ", topVer=" + req.topologyVersion() + "]", ex);
                }
                finally {
                    globalState.setTransitionResult(req.requestId(), true);

                    sendChangeGlobalStateResponse(req.requestId(), req.initiatorNodeId(), e);
                }
            }
        });
    }

    /**
     *
     */
    private void onFinalDeActivate(final StateChangeRequest req) {
        final boolean client = ctx.clientNode();

        if (log.isInfoEnabled())
            log.info("Successfully performed final deactivation steps [nodeId="
                + ctx.localNodeId() + ", client=" + client + ", topVer=" + req.topologyVersion() + "]");

        Exception ex = null;

        try {
            sharedCtx.deactivate();

            sharedCtx.affinity().removeAllCacheInfo();
        }
        catch (Exception e) {
            ex = e;
        }

        globalState.setTransitionResult(req.requestId(), false);

        sendChangeGlobalStateResponse(req.requestId(), req.initiatorNodeId(), ex);
    }

    /**
     *
     */
    public void onExchangeDone(boolean fail, StateChangeRequest req) {
        // TODO GG-12389 pass correct fail flag.
        if (!fail) {
            if (req.activate())
                onFinalActivate(req);
            else
                onFinalDeActivate(req);
        }
    }

    /**
     * @param reqId Request ID.
     * @param initNodeId Initialize node id.
     * @param ex Exception.
     */
    private void sendChangeGlobalStateResponse(UUID reqId, UUID initNodeId, Exception ex) {
        assert reqId != null;
        assert initNodeId != null;

        GridChangeGlobalStateMessageResponse res = new GridChangeGlobalStateMessageResponse(reqId, ex);

        try {
            if (log.isDebugEnabled())
                log.debug("Sending global state change response [nodeId=" + ctx.localNodeId() +
                    ", topVer=" + ctx.discovery().topologyVersionEx() + ", res=" + res + "]");

            if (ctx.localNodeId().equals(initNodeId))
                processChangeGlobalStateResponse(ctx.localNodeId(), res);
            else
                sharedCtx.io().send(initNodeId, res, SYSTEM_POOL);
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled()) {
                log.debug("Failed to send change global state response, node left [node=" + initNodeId +
                    ", res=" + res + ']');
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send change global state response [node=" + initNodeId + ", res=" + res + ']', e);
        }
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     */
    private void processChangeGlobalStateResponse(final UUID nodeId, final GridChangeGlobalStateMessageResponse msg) {
        assert nodeId != null;
        assert msg != null;

        if (log.isDebugEnabled()) {
            log.debug("Received activation response [requestId=" + msg.getRequestId() +
                ", nodeId=" + nodeId + "]");
        }

        UUID requestId = msg.getRequestId();

        final GridChangeGlobalStateFuture fut = stateChangeFut.get();

        if (fut != null && requestId.equals(fut.requestId)) {
            if (fut.initFut.isDone())
                fut.onResponse(nodeId, msg);
            else {
                fut.initFut.listen(new CI1<IgniteInternalFuture<?>>() {
                    @Override public void apply(IgniteInternalFuture<?> f) {
                        // initFut is completed from discovery thread, process response from other thread.
                        ctx.getSystemExecutorService().execute(new Runnable() {
                            @Override public void run() {
                                fut.onResponse(nodeId, msg);
                            }
                        });
                    }
                });
            }
        }
    }

    /**
     * @param activate Activate.
     */
    private static String prettyStr(boolean activate) {
        return activate ? "activate" : "deactivate";
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClusterStateProcessor.class, this);
    }

    /**
     *
     */
    private static class GridChangeGlobalStateFuture extends GridFutureAdapter<Void> {
        /** Request id. */
        @GridToStringInclude
        private final UUID requestId;

        /** Activate. */
        private final boolean activate;

        /** Nodes. */
        @GridToStringInclude
        private final Set<UUID> remaining = new HashSet<>();

        /** Responses. */
        @GridToStringInclude
        private final Map<UUID, GridChangeGlobalStateMessageResponse> responses = new HashMap<>();

        /** Context. */
        @GridToStringExclude
        private final GridKernalContext ctx;

        /** */
        @GridToStringExclude
        private final Object mux = new Object();

        /** */
        @GridToStringInclude
        private final GridFutureAdapter<?> initFut = new GridFutureAdapter<>();

        /** Grid logger. */
        @GridToStringExclude
        private final IgniteLogger log;

        /**
         *
         */
        GridChangeGlobalStateFuture(UUID requestId, boolean activate, GridKernalContext ctx) {
            this.requestId = requestId;
            this.activate = activate;
            this.ctx = ctx;

            log = ctx.log(getClass());
        }

        /**
         * @param event Event.
         */
        public void onDiscoveryEvent(DiscoveryEvent event) {
            assert event != null;

            if (isDone())
                return;

            boolean allReceived = false;

            synchronized (mux) {
                if (remaining.remove(event.eventNode().id()))
                    allReceived = remaining.isEmpty();
            }

            if (allReceived)
                onAllReceived();
        }

        /**
         *
         */
        void setRemaining(Set<UUID> nodesIds, AffinityTopologyVersion topVer) {
            if (log.isDebugEnabled()) {
                log.debug("Setup remaining node [id=" + ctx.localNodeId() +
                    ", client=" + ctx.clientNode() +
                    ", topVer=" + topVer +
                    ", nodes=" + nodesIds + "]");
            }

            synchronized (mux) {
                remaining.addAll(nodesIds);
            }

            initFut.onDone();
        }

        /**
         * @param msg Activation message response.
         */
        public void onResponse(UUID nodeId, GridChangeGlobalStateMessageResponse msg) {
            assert msg != null;

            if (isDone())
                return;

            boolean allReceived = false;

            synchronized (mux) {
                if (remaining.remove(nodeId))
                    allReceived = remaining.isEmpty();

                responses.put(nodeId, msg);
            }

            if (allReceived)
                onAllReceived();
        }

        /**
         *
         */
        private void onAllReceived() {
            Throwable e = new Throwable();

            boolean fail = false;

            for (Map.Entry<UUID, GridChangeGlobalStateMessageResponse> entry : responses.entrySet()) {
                GridChangeGlobalStateMessageResponse r = entry.getValue();

                if (r.getError() != null) {
                    fail = true;

                    e.addSuppressed(r.getError());
                }
            }

            if (fail)
                onDone(e);
            else
                onDone();
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Void res, @Nullable Throwable err) {
            if (super.onDone(res, err)) {
                ctx.state().stateChangeFut.compareAndSet(this, null);

                return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(GridChangeGlobalStateFuture.class, this);
        }
    }

    /**
     *
     */
    private static class ClientChangeGlobalStateComputeRequest implements IgniteRunnable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Activation. */
        private final boolean activation;

        /** Ignite. */
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         *
         */
        private ClientChangeGlobalStateComputeRequest(boolean activation) {
            this.activation = activation;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            ignite.active(activation);
        }
    }

    /**
     *
     */
    class TransitionOnJoinWaitFuture extends GridFutureAdapter<Boolean> {
        /** */
        private DiscoveryDataClusterState transitionState;

        /** */
        private final Set<UUID> transitionNodes;

        /**
         * @param state Current state.
         */
        TransitionOnJoinWaitFuture(DiscoveryDataClusterState state, DiscoCache discoCache) {
            assert state.transition() : state;

            transitionNodes = U.newHashSet(state.transitionNodes().size());

            for (UUID nodeId : state.transitionNodes()) {
                if (discoCache.node(nodeId) != null)
                    transitionNodes.add(nodeId);
            }
        }
    }
}
