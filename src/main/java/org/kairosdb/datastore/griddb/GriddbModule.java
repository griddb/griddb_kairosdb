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

import org.kairosdb.core.datastore.Datastore;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;

/**
 * Implement DI framework.
 *
 */
public class GriddbModule extends AbstractModule {
    /**
     * Constructor.
     */
    public GriddbModule() {
        // This constructor is intentionally empty. Nothing special is needed
        // here.
    }

    /**
     * Bind object follow DI framework.
     */
    @Override
    protected final void configure() {
        requestStaticInjection(GridStorePoolFactory.class);
        bind(GriddbDatastore.class).in(Scopes.SINGLETON);
        bind(GriddbConfiguration.class).in(Scopes.SINGLETON);
        bind(Datastore.class).to(GriddbDatastore.class).in(Scopes.SINGLETON);
    }
}
