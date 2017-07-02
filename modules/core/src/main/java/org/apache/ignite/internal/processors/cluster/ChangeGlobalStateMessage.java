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

import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.cache.ExchangeActions;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Message represent request for change cluster global state.
 */
public class ChangeGlobalStateMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Custom message ID. */
    private IgniteUuid id = IgniteUuid.randomUuid();

    /** Request ID */
    private UUID requestId;

    /** Initiator node ID. */
    private UUID initiatingNodeId;

    /** If true activate else deactivate. */
    private boolean activate;

    /** */
    private List<StoredCacheData> storedCfgs;

    /** */
    private transient ExchangeActions exchangeActions;

    /**
     *
     */
    public ChangeGlobalStateMessage(
        UUID requestId,
        UUID initiatingNodeId,
        List<StoredCacheData> storedCfgs,
        boolean activate
    ) {
        this.requestId = requestId;
        this.initiatingNodeId = initiatingNodeId;
        this.storedCfgs = storedCfgs;
        this.activate = activate;
    }

    /**
     * @return Stored cache configurations.
     */
    @Nullable public List<StoredCacheData> storedCacheConfigurations() {
        return storedCfgs;
    }

    /**
     * @return Cache updates to be executed on exchange.
     */
    public ExchangeActions exchangeActions() {
        return exchangeActions;
    }

    /**
     * @param exchangeActions Cache updates to be executed on exchange.
     */
    public void exchangeActions(ExchangeActions exchangeActions) {
        assert exchangeActions != null && !exchangeActions.empty() : exchangeActions;

        this.exchangeActions = exchangeActions;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return false;
    }

   /**
     *
     */
    public UUID initiatorNodeId() {
        return initiatingNodeId;
    }

    /**
     *
     */
    public boolean activate() {
        return activate;
    }

    /**
     *
     */
    public UUID requestId() {
        return requestId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ChangeGlobalStateMessage.class, this);
    }
}
