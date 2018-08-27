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

import java.util.Properties;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import com.google.inject.Inject;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;

/**
 *
 * Implement object pool pattern using apache commons pool.
 *
 */
public class GridStorePoolFactory implements PooledObjectFactory<GridStore> {
    /**
     * Injected configuration object.
     */
    @Inject
    private static GriddbConfiguration m_configuration;
    /**
     * Object contains all properties defined by configuration object.
     */
    private final Properties m_props;

    public GridStorePoolFactory() {
        m_props = new Properties();
        m_props.setProperty("clusterName", m_configuration.getClusterName());
        if (m_configuration.getNotificationAddress().length() > 0) {
            m_props.setProperty("notificationAddress",
                    m_configuration.getNotificationAddress());
        }
        if (m_configuration.getNotificationPort().length() > 0) {
            m_props.setProperty("notificationPort",
                    m_configuration.getNotificationPort());
        }
        if (m_configuration.getNotificationMember().length() > 0) {
            m_props.setProperty("notificationMember",
                    m_configuration.getNotificationMember());
        }
        if (m_configuration.getNotificationProviderUrl().length() > 0) {
            m_props.setProperty("notificationProvider",
                    m_configuration.getNotificationProviderUrl());
        }
        m_props.setProperty("user", m_configuration.getUser());
        m_props.setProperty("password", m_configuration.getPassword());

        // Below values are optional, use default if empty
        if (m_configuration.getConsistency().length() > 0) {
            m_props.setProperty("consistency",
                    m_configuration.getConsistency());
        }
        if (m_configuration.getTransactionTimeout().length() > 0) {
            m_props.setProperty("transactionTimeout",
                    m_configuration.getTransactionTimeout());
        }
        if (m_configuration.getFailOverTimeout().length() > 0) {
            m_props.setProperty("failoverTimeout",
                    m_configuration.getFailOverTimeout());
        }
        if (m_configuration.getContainerCacheSize().length() > 0) {
            m_props.setProperty("containerCacheSize",
                    m_configuration.getContainerCacheSize());
        }
        if (m_configuration.getDataAffinityPattern().length() > 0) {
            m_props.setProperty("dataAffinityPattern",
                    m_configuration.getDataAffinityPattern());
        }
    }

    @Override
    public void activateObject(final PooledObject<GridStore> p)
            throws Exception {
        // Not implemented
    }

    @Override
    public void destroyObject(final PooledObject<GridStore> p)
            throws Exception {
        if (p.getObject() != null) {
            try {
                p.getObject().close();
            } catch (final GSException e) {
                // The connection is already released. Do nothing.
                return;
            }
        }
    }

    @Override
    public PooledObject<GridStore> makeObject() throws Exception {
        final GridStore gridstore =
                GridStoreFactory.getInstance().getGridStore(m_props);
        return new DefaultPooledObject<GridStore>(gridstore);
    }

    @Override
    public void passivateObject(final PooledObject<GridStore> p)
            throws Exception {
        // Not implemented
    }

    @Override
    public boolean validateObject(final PooledObject<GridStore> p) {
        return (p.getObject() != null);
    }

}
