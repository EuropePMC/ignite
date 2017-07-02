/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.testframework.GridTestUtils;

/**
 *
 */
public class IgniteClusterActivateDeactivateTestWithPersistence extends IgniteClusterActivateDeactivateTest {
    /** {@inheritDoc} */
    @Override protected boolean persistenceEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        GridTestUtils.deleteDbFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        GridTestUtils.deleteDbFiles();
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivateCachesRestore_SingleNode() throws Exception {
        activateCachesRestore(1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivateCachesRestore_5_Servers() throws Exception {
        activateCachesRestore(5);
    }

    /**
     * @param srvs Number of server nodes.
     * @throws Exception If failed.
     */
    private void activateCachesRestore(int srvs) throws Exception {
        Ignite srv = startGrids(srvs);

        srv.active(true);

        srv.createCaches(Arrays.asList(cacheConfigurations1()));

        stopAllGrids();

        srv = startGrids(srvs);

        checkNoCaches(srvs);

        srv.active(true);

        for (int i = 0; i < srvs; i++) {
            for (int c = 0; c < 2; c++)
                checkCache(ignite(i), CACHE_NAME_PREFIX + c, true);
        }

        checkCaches(srvs, 2);
    }
}
