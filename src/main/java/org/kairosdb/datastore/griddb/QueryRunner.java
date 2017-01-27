/*
   Copyright (c) 2016 TOSHIBA CORPORATION.
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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.datapoints.DataPointFactory;
import org.kairosdb.core.datastore.QueryCallback;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.datastore.griddb.WriteBuffer.DataPoint;
import org.kairosdb.util.KDataInput;

import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.RowSet;

/**
 * This class is a runner for a list of queries.
 *
 */
public class QueryRunner {
    /**
     * Input query list.
     */
    private final List<Query<DataPoint>> m_queryList;
    /**
     * Query result to send back to KairosDB.
     */
    private final QueryCallback m_queryCallback;
    /**
     * Connection object.
     */
    private final GridStore m_gridstore;
    /**
     * Type of data result, used to specify datapoint factory.
     */
    private final String m_dataType;
    /**
     * Input tags for queries, used to specify datapoint factory.
     */
    private final Map<String, String> m_tags;
    /**
     * General factory.
     */
    private final KairosDataPointFactory m_kairosDataPointFactory;

    /**
     * Set variable values.
     *
     * @param queryList
     *            list of input query
     * @param queryCallback
     *            query result
     * @param dataType
     *            type of result data
     * @param tags
     *            tags for query
     * @param kairosDataPointFactory
     *            provides functions to create data points
     * @param gridstore
     *            connection object
     */

    public QueryRunner(final List<Query<DataPoint>> queryList,
            final QueryCallback queryCallback, final String dataType,
            final Map<String, String> tags,
            final KairosDataPointFactory kairosDataPointFactory,
            final GridStore gridstore) {
        m_queryList = queryList;
        m_queryCallback = queryCallback;
        m_dataType = dataType;
        m_tags = tags;
        m_kairosDataPointFactory = kairosDataPointFactory;
        m_gridstore = gridstore;
    }

    /**
     * Execute query.
     *
     * Read query from list. Query GridDB, get response and store result to
     * queryCallback
     *
     * @throws IOException
     *
     * @throws DatastoreException
     *
     */

    public final void runQuery() throws IOException, GSException {
        final DataPointFactory dataPointFactory =
                m_kairosDataPointFactory.getFactoryForDataStoreType(m_dataType);

        m_gridstore.fetchAll(m_queryList);

        for (final Query<DataPoint> query : m_queryList) {
            final RowSet<DataPoint> rowset = query.getRowSet();
            if (rowset.size() != 0) {
                m_queryCallback.startDataPointSet(m_dataType, m_tags);

                while (rowset.hasNext()) {
                    final DataPoint row = rowset.next();
                    final long timestamp = row.timestamp.getTime();
                    final byte[] value = row.val;

                    // Add row values to result storage
                    m_queryCallback.addDataPoint(dataPointFactory.getDataPoint(
                            timestamp, KDataInput.createInput(value)));
                }
            }
        }
    }
}
