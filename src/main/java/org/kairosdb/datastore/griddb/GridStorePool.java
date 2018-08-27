/*
   Copyright (c) 2017 TOSHIBA Digital Solutions Corporation

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package org.kairosdb.datastore.griddb;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Implement apache commons pool.
 *
 * @param <GridStore>
 */
public class GridStorePool<GridStore> extends GenericObjectPool<GridStore> {

    /**
     * Initialize gridstore pool with configuration.
     * @param factory define pool factory 
     * @param config define input configuration
     */
    public GridStorePool(final PooledObjectFactory<GridStore> factory,
            final GenericObjectPoolConfig config) {
        super(factory, config);

    }

    /**
     * Initialize gridstore pool without configuration.
     * @param factory define pool factory 
     */
    public GridStorePool(final PooledObjectFactory<GridStore> factory) {
        super(factory);

    }

}
