/*
 * Copyright 2013 Proofpoint Inc.
 * Copyright (c) 2016 TOSHIBA CORPORATION.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
/*
 * This file is modified by TOSHIBA CORPORATION.
 *
 * This file is based on the file CassandraDatastore.java
 * in https://github.com/kairosdb/kairosdb/archive/v1.1.1.zip
 * (KairosDB 1.1.1).
 */
package org.kairosdb.datastore.griddb;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.DatastoreMetricQuery;
import org.kairosdb.core.datastore.Order;
import org.kairosdb.core.datastore.QueryCallback;
import org.kairosdb.core.datastore.TagSet;
import org.kairosdb.core.datastore.TagSetImpl;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.datastore.griddb.WriteBuffer.DataPoint;
import org.kairosdb.datastore.griddb.WriteBuffer.RowKeyIndex;
import org.kairosdb.datastore.griddb.WriteBuffer.StringIndex;
import org.kairosdb.util.KDataOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.SetMultimap;
import com.google.inject.Inject;
import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.FetchOption;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GSTimeoutException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.QueryOrder;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.TimeSeries;
import com.toshiba.mwcloud.gs.TimeSeriesProperties;
import com.toshiba.mwcloud.gs.common.RowMapper;

/**
 * Implement interface between KairosDB and GridDB. In the KairosDB(GridDB)
 * version, we use 3 type of containers to store data. metricName_valueType_tags
 * : to store data point. metricName : to store container row key.
 * metric_names, tag_names, tag_values: store information about metric
 * name, tag name, tag value.
 */
public class GriddbDatastore implements Datastore {
    /**
     * Griddb Configuration.
     */
    private final GriddbConfiguration m_griddbConfiguration;
    /**
     * GridStore pool implement Object pool pattern.
     */
    private GridStorePool<GridStore> m_gridstorePool;
    /**
     * Data point write buffer.
     */
    private List<WriteBuffer> m_dataPointWriteBufferList;

    private GridStore m_gridstoreForPId;
    /**
     * Row key write buffer.
     */
    private WriteBuffer m_rowKeyWriteBuffer;
    /**
     * String index write buffer.
     */
    private WriteBuffer m_stringindexWriteBuffer;
    /**
     * Row key index cache.
     */
    private DataCache<DataPointContainerKey> m_rowKeyIndexCache;
    /**
     * Metric name cache.
     */
    private DataCache<String> m_metricNameCache;
    /**
     * Tag name cache.
     */
    private DataCache<String> m_tagNameCache;
    /**
     * Tag value cache.
     */
    private DataCache<String> m_tagValueCache;
    /**
     * Logger.
     */
    public static final Logger LOGGER =
            LoggerFactory.getLogger(GriddbDatastore.class);
    /**
     * Delimiter.
     */
    public static final String DELIMITER = "_";
    /**
     * Empty string.
     */
    public static final String EMPTY_STRING = "";
    /**
     * Index of first tag name in String retrieved from metric name container.
     */
    private static final int TAG_START_INDEX = 1;
    /**
     * Step between each tag in String retrieved from metric name container.
     */
    private static final int TAG_STEP = 2;
    /**
     * Display error for data point.
     */
    private static final String INVALID_DATAPOINT = "Invalid datapoint";
    /**
     * Display error for ttl.
     */
    private static final String INVALID_TTL = "Invalid ttl value";
    /**
     * Display error for invalid metric name.
     */
    private static final String INVALID_METRIC_NAME = "Invalid metric name";
    /**
     * Ignore error on kairosdb auto insert metric.
     */
    private static final String AUTO_INSERT_KAIROS_METRIC_PREFIX = "kairosdb.";
    /**
     * Display error for invalid tag name.
     */
    private static final String INVALID_TAG_NAME = "Invalid tag name";
    /**
     * Display error for invalid tag value.
     */
    private static final String INVALID_TAG_VALUE = "Invalid tag value";
    /**
     * Display error for connection.
     */
    public static final String CONNECTION_ERR = "Connection error with GridDB";
    /**
     * Display error for wrong input condition.
     */
    private static final String INVALID_QUERY_CONDITION =
            "Invalid query condition";
    /**
     * Display error for transaction.
     */
    private static final String TRANSACTION_ERR = "Transaction error";
    /**
     * Display error for failure in releasing buffer.
     */
    private static final String RELEASING_RES_ERR = "Releasing resource error";
    /**
     * Size of container name Approximately 16KB if block size = 64KB,
     * Approximately 128KB if block size = 1MB.
     */
    private static final Integer GRIDB_NAME_MAX_BYTES = 16384;

    /**
     * Display error when container name is too long.
     */
    private static final String CONTAINER_NAME_ERROR =
            "Container name is too long.";
    /**
     * KairosDB data point factory.
     */
    private final KairosDataPointFactory m_kairosDataPointFactory;
    /**
     * GridDB min timestamp (1970-01-01)
     */
    private static final Long GRIDDB_MIN_TIMESTAMP = 0l;
    /**
     * GridDB max timestamp (9999-12-31)
     */
    private static final Long GRIDDB_MAX_TIMESTAMP = 253402300799999L;
    /**
     * Error when parse invalid timestamp.
     */
    private static final String DATAPOINT_TIMESTAMP_ERR = "Invalid timestamp";
    /**
     * Config number of write buffer for data point only.
     */
    private final Integer m_numberWriteBufferDataPoint;
    /**
     * Config number of thread for multiput in write buffer.
     */
    private final Integer NUMBER_THREAD_MULTIPUT_DATAPOINT = 1;
    /**
     * Store hash map for partition index
     */
    private final HashMap<String, Integer> m_hashPartitionId;
    /**
     * Cache information of RowMapper for all write buffer.
     */
    private final HashMap<String, RowMapper> m_resolveMapperArr;

    /**
     * Constructor.
     *
     * @param griddBConfiguration
     *            configuration
     * @param kairosDataPointFactory
     *            Kairos data point factory
     * @throws DatastoreException
     */
    @Inject
    public GriddbDatastore(final GriddbConfiguration griddBConfiguration,
            final KairosDataPointFactory kairosDataPointFactory)
            throws DatastoreException {
        this.m_griddbConfiguration = griddBConfiguration;
        m_kairosDataPointFactory = kairosDataPointFactory;

        // Create pool for gridstore objects
        final GridStorePoolFactory poolFactory = new GridStorePoolFactory();
        final GridStorePool<GridStore> gridstorePool =
                new GridStorePool<>(poolFactory);

        m_numberWriteBufferDataPoint =
                m_griddbConfiguration.getNumberDataPointWriteBuffer();
        m_resolveMapperArr = new HashMap<>();
        m_hashPartitionId = new HashMap<>();
        // Initialize connection pool, buffer, cache
        initialize(gridstorePool);
    }

    /**
     * Initialize GridStore object pool, write buffer, data cache.
     *
     * @param gridstorePool
     *            inject dependency, support testing.
     * @throws DatastoreException
     *
     *
     */
    public final void initialize(final GridStorePool<GridStore> gridstorePool)
            throws DatastoreException {
        final ReentrantLock mutatorLock = new ReentrantLock();

        // Set size for pool
        final GenericObjectPoolConfig poolConfig =
                new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(m_griddbConfiguration.getNumberOfConcurrency());
        gridstorePool.setConfig(poolConfig);
        m_gridstorePool = gridstorePool;

        // Create Buffer objects
        final int writeDelay = m_griddbConfiguration.getWriteDelay();
        final int bufferSize = m_griddbConfiguration.getMaxWriteBufferSize();
        // TODO : Use configurable variable
        final int threadCount = NUMBER_THREAD_MULTIPUT_DATAPOINT;

        m_dataPointWriteBufferList = new ArrayList<>();
        for (int i = 0; i < m_numberWriteBufferDataPoint; i++) {
            m_dataPointWriteBufferList
                    .add(new WriteBuffer(writeDelay, bufferSize, mutatorLock,
                            m_gridstorePool, WriteBuffer.BUFFER_TYPE_DATAPOINT,
                            threadCount, m_resolveMapperArr));
        }
        m_rowKeyWriteBuffer =
                new WriteBuffer(writeDelay, bufferSize, mutatorLock,
                        m_gridstorePool, WriteBuffer.BUFFER_TYPE_ROW_KEY_INDEX,
                        threadCount, m_resolveMapperArr);
        m_stringindexWriteBuffer =
                new WriteBuffer(writeDelay, bufferSize, mutatorLock,
                        m_gridstorePool, WriteBuffer.BUFFER_TYPE_STRING_INDEX,
                        threadCount, m_resolveMapperArr);

        // Create data cache objects
        final int cacheSize = m_griddbConfiguration.getMaxDataCacheSize();
        m_rowKeyIndexCache = new DataCache<>(cacheSize);
        m_metricNameCache = new DataCache<>(cacheSize);
        m_tagNameCache = new DataCache<>(cacheSize);
        m_tagValueCache = new DataCache<>(cacheSize);
        try {
            m_gridstoreForPId = gridstorePool.borrowObject();
        } catch (final Exception e) {
            // Announce connection error to user
            throw new DatastoreException(CONNECTION_ERR, e);
        }

        // Create 3 containers to store metric name, tag name, tag value
        final String containerMetrics = "metric_names";
        final String containerTags = "tag_names";
        final String containerTagValue = "tag_values";
        try {
            createContainer(WriteBuffer.BUFFER_TYPE_STRING_INDEX,
                    containerMetrics, m_gridstoreForPId);
            createContainer(WriteBuffer.BUFFER_TYPE_STRING_INDEX, containerTags,
                    m_gridstoreForPId);
            createContainer(WriteBuffer.BUFFER_TYPE_STRING_INDEX,
                    containerTagValue, m_gridstoreForPId);
        } catch (final GSException e) {
            // When error with GridDB.
            throw new DatastoreException(CONNECTION_ERR, e);
        }
        // End create 3 containers.
    }

    /**
     * The method is closed when KairosDB is shutdown.
     *
     * @throws DatastoreException
     *             throws closing exception
     */
    @Override
    public final void close() throws DatastoreException {
        // Clean buffers
        try {
            for (final WriteBuffer wbuf : m_dataPointWriteBufferList) {
                wbuf.close();
            }
            m_rowKeyWriteBuffer.close();
            m_stringindexWriteBuffer.close();
        } catch (final InterruptedException e) {
            LOGGER.error(RELEASING_RES_ERR, e);
            throw new DatastoreException(RELEASING_RES_ERR);
        }

        // Close gridstore object pool
        m_gridstorePool.clear();
        m_gridstorePool.close();
    }

    /**
     * Put data point to GridDB.
     */
    @Override
    public final void putDataPoint(final String inputMetricName,
            final ImmutableSortedMap<String, String> inputTags,
            final org.kairosdb.core.DataPoint dataPoint, final int timeToLive)
            throws DatastoreException {
        SortedMap<String, String> tags = new TreeMap<>();
        String metricName;
        int ttl = timeToLive;
        final boolean kairosdbAutoInsertData =
                inputMetricName.startsWith(AUTO_INSERT_KAIROS_METRIC_PREFIX);

        if (kairosdbAutoInsertData) {
            metricName = removeSpecialChars(inputMetricName);
            for (final String tagName : inputTags.keySet()) {
                final String tagValue = inputTags.get(tagName);
                tags.put(removeSpecialChars(tagName),
                        removeSpecialChars(tagValue));
            }
        } else {
            metricName = inputMetricName;
            tags = inputTags;
        }

        if (dataPoint == null) {
            throw new DatastoreException(INVALID_DATAPOINT);
        }
        if (ttl < 0) {
            throw new DatastoreException(INVALID_TTL);
        }

        // time the data is written
        if (0 == ttl) {
            ttl = m_griddbConfiguration.getDatapointTtl();
        }

        final DataPointContainerKey containerKey = new DataPointContainerKey(
                metricName, dataPoint.getDataStoreDataType(), tags);
        if (containerKey.toString().getBytes().length > GRIDB_NAME_MAX_BYTES) {
            throw new DatastoreException(CONTAINER_NAME_ERROR);
        }

        if (dataPoint.getTimestamp() < GRIDDB_MIN_TIMESTAMP
                || dataPoint.getTimestamp() > GRIDDB_MAX_TIMESTAMP) {
            throw new DatastoreException(DATAPOINT_TIMESTAMP_ERR);
        }

        final DataPointContainerKey cachedKey =
                m_rowKeyIndexCache.cacheItem(containerKey);

        // Create new row in row key index with container name is metric
        // name
        if (cachedKey == null) {
            try {
                if (!kairosdbAutoInsertData) {
                    verifyMetricName(metricName);
                    verifyTags(tags);
                }

                final GridStore gridstore = m_gridstorePool.borrowObject();
                // create container for metric name -row key index
                createContainer(WriteBuffer.BUFFER_TYPE_ROW_KEY_INDEX,
                        metricName, gridstore);
                // create container for datapoint
                createContainer(WriteBuffer.BUFFER_TYPE_DATAPOINT,
                        containerKey.toString(), gridstore, ttl);
                m_gridstorePool.returnObject(gridstore);
            } catch (final GSTimeoutException eTimeOut) {
                throw new DatastoreException(CONNECTION_ERR, eTimeOut);
            } catch (final Exception e) {
                throw new DatastoreException(e.getMessage(), e);
            }
            addMetricNameToWriteBuffer(dataPoint, tags, metricName,
                    containerKey);
        }

        // Add data to String index Write Buffer
        try {
            addMetricTagNameTagValueToWriteBuffer(tags, metricName);
        } catch (final Exception e) {
            throw new DatastoreException(e.getMessage(), e);
        }

        // Create containers and rows for data_points
        addDataPointToWriteBuffer(dataPoint, tags, metricName, ttl,
                containerKey);
    }

    /**
     * Delete data points.
     */
    @Override
    public final void deleteDataPoints(final DatastoreMetricQuery deleteQuery)
            throws DatastoreException {
        GridStore gridstore = null;
        checkNotNull(deleteQuery);
        final String valid = validateStartEndTime(deleteQuery.getStartTime(),
                deleteQuery.getEndTime());
        switch (valid) {
        case "OK":
            // Do nothing
            break;
        case "MAX":
            // Do nothing
            break;
        default:
            // error case.
            throw new DatastoreException(valid);
        }

        try {
            gridstore = m_gridstorePool.borrowObject();
        } catch (final Exception e) {
            // Cannot create object due to connection failure
            throw new DatastoreException(CONNECTION_ERR, e);
        }

        // Get input keys
        Iterator<DataPointContainerKey> rowKeyIterator;
        try {
            rowKeyIterator = getContainerKeyFromQuery(deleteQuery, gridstore);
        } catch (final DatastoreException e1) {
            // Return exception for conditions checking
            throw e1;
        } catch (final GSException e2) {
            // Return error due to connection failure
            throw new DatastoreException(CONNECTION_ERR, e2);
        } finally {
            m_gridstorePool.returnObject(gridstore);
        }

        while (rowKeyIterator.hasNext()) {
            final DataPointContainerKey rowKey = rowKeyIterator.next();
            int pId;
            try {
                pId = this.getParitionId(rowKey.toString());
            } catch (final GSException e3) {
                // Catch error from GridDB, ex : timeout exception,...
                LOGGER.error(CONNECTION_ERR, e3);
                throw new DatastoreException(CONNECTION_ERR, e3);
            }
            m_dataPointWriteBufferList.get(pId % m_numberWriteBufferDataPoint)
                    .deleteData(rowKey, deleteQuery.getStartTime(),
                            deleteQuery.getEndTime(), DELIMITER);

            m_metricNameCache.clear();
            m_tagNameCache.clear();
            m_tagValueCache.clear();
            m_rowKeyIndexCache.clear();
        }

    }

    /**
     * Get all metric names from GridDB.
     */
    @Override
    public final Iterable<String> getMetricNames() throws DatastoreException {
        final String containerMetrics = "metric_names";
        return listAllItemStringIndex(containerMetrics);
    }

    /**
     * Get all tag names from GridDB.
     */
    @Override
    public final Iterable<String> getTagNames() throws DatastoreException {
        final String containerTagNames = "tag_names";
        return listAllItemStringIndex(containerTagNames);
    }

    /**
     * Get all tag values from GridDB.
     */
    @Override
    public final Iterable<String> getTagValues() throws DatastoreException {
        final String containerTagValues = "tag_values";
        return listAllItemStringIndex(containerTagValues);
    }

    /**
     * Return datapoints which satisfy specific conditions.
     *
     * Get input conditions from KairosDB query, create query to request
     * specific data from GridDB and store result in QueryCallBack
     *
     * @param query
     *            input query from KairosDB
     * @param queryCallback
     *            query result
     * @throws DatastoreException
     *             exception message to kairosDB
     */
    @Override
    public final void queryDatabase(final DatastoreMetricQuery query,
            final QueryCallback queryCallback) throws DatastoreException {
        final GridStore gridstore;
        final Iterator<DataPointContainerKey> containerKeys;
        Date startTime = null; 
        Date endTime = null; 
        int limit;
        String valid;
        QueryOrder order = QueryOrder.ASCENDING;
        List<QueryRunner> runners;

        try {
            gridstore = m_gridstorePool.borrowObject();
        } catch (final Exception e) {
            // Cannot create object due to connection failure
            throw new DatastoreException(CONNECTION_ERR, e);
        }

        valid = validateStartEndTime(query.getStartTime(), query.getEndTime());
        switch (valid) {
        case "OK":
            startTime = new Date(query.getStartTime());
            endTime = new Date(query.getEndTime());
            break;
        case "MAX":
            startTime = new Date(query.getStartTime());
            break;
        default:
            throw new DatastoreException(valid);
        }

        // Get input keys
        try {
            containerKeys = getContainerKeyFromQuery(query, gridstore);
        } catch (final DatastoreException e1) {
            // Return exception for conditions checking
            m_gridstorePool.returnObject(gridstore);
            throw e1;
        } catch (final GSException e2) {
            // Return error due to connection failure
            m_gridstorePool.returnObject(gridstore);
            throw new DatastoreException(CONNECTION_ERR, e2);
        }
        limit = query.getLimit();

        // Get query conditions
        if (query.getOrder().equals(Order.DESC)) {
            order = QueryOrder.DESCENDING;
        }

        // Create query runners
        try {
            runners = createRunners(containerKeys, startTime, endTime,
                    queryCallback, order, limit, gridstore);
        } catch (final GSTimeoutException e3) {
            // Cannot create query due to connection failure
            m_gridstorePool.returnObject(gridstore);
            throw new DatastoreException(CONNECTION_ERR, e3);
        } catch (final GSException e4) {
            // Wrong input format
            m_gridstorePool.returnObject(gridstore);
            throw new DatastoreException(INVALID_QUERY_CONDITION, e4);
        }

        // Execute runners
        try {
            for (final QueryRunner runner : runners) {
                runner.runQuery();
            }
        } catch (final GSTimeoutException e5) {
            // Connection failure
            throw new DatastoreException(CONNECTION_ERR, e5);
        } catch (final GSException e6) {
            // GridDB query failure
            throw new DatastoreException(e6);
        } catch (final IOException e7) {
            // Cannot return result to user
            throw new DatastoreException(TRANSACTION_ERR, e7);
        } finally {
            m_gridstorePool.returnObject(gridstore);
        }

        // Return result to KairosDB
        try {
            queryCallback.endDataPoints();
        } catch (final IOException e) {
            throw new DatastoreException(TRANSACTION_ERR, e);
        }
    }

    /**
     * Return tags for a specific metric name with conditions.
     *
     * Get conditions from input query, get corresponding container keys, filter
     * keys according to input conditions then return result
     *
     * @param query
     *            input query from KairosDB
     * @return set of result tags
     */
    @Override
    public final TagSet queryMetricTags(final DatastoreMetricQuery query)
            throws DatastoreException {
        final TagSetImpl tagSet = new TagSetImpl();
        Iterator<DataPointContainerKey> containerKeys;
        Date startTime = new Date(0); // min date is 01/01/1970
        Date endTime = null;
        GridStore gridstore = null;
        String valid;

        // Validate input time
        valid = validateStartEndTime(query.getStartTime(), query.getEndTime());
        switch (valid) {
        case "OK":
            startTime = new Date(query.getStartTime());
            endTime = new Date(query.getEndTime());
            break;
        case "MAX":
            startTime = new Date(query.getStartTime());
            break;
        default:
            throw new DatastoreException(valid);
        }

        try {
            gridstore = m_gridstorePool.borrowObject();
        } catch (final Exception e) {
            // Cannot create object due to connection failure
            throw new DatastoreException(CONNECTION_ERR, e);
        }

        try {
            containerKeys = getContainerKeyFromQuery(query, gridstore);
            // Loop for every Container Keys
            while (containerKeys.hasNext()) {

                final DataPointContainerKey key = containerKeys.next();
                // Check time conditions
                if (!isInTimeRange(startTime, endTime, key, gridstore)) {
                    continue;
                }

                // Add valid tags to set
                final Set<Map.Entry<String, String>> tags =
                        key.getTags().entrySet();
                for (final Map.Entry<String, String> tag : tags) {
                    tagSet.addTag(tag.getKey(), tag.getValue());
                }
            }
        } catch (final DatastoreException e1) {
            // Return exception for conditions checking
            throw e1;
        } catch (final GSException e2) {
            // Fail to query GridDB due to connection failure
            throw new DatastoreException(CONNECTION_ERR, e2);
        } finally {
            m_gridstorePool.returnObject(gridstore);
        }

        return tagSet;
    }

    /**
     * Get all container keys for a specific condition from input query.
     *
     * Query containers with name achieved from input query. Extract tags,
     * dataType, metric name from result then compare with input conditions
     * (tags, dataType) and filter the result.
     *
     * @param query
     *            input query from KairosDB
     * @return list of Container Keys corresponding to that query
     * @throws GSException
     *             Gridstore exception
     * @throws DatastoreException
     *             KairosDB Exception
     */
    private Iterator<DataPointContainerKey> getContainerKeyFromQuery(
            final DatastoreMetricQuery query, final GridStore gridstore)
            throws GSException, DatastoreException {
        String metricName;
        SetMultimap<String, String> tags;
        Query<RowKeyIndex> gridDBQuery;
        RowSet<RowKeyIndex> queryResult;
        Collection<String, RowKeyIndex> rowKeyContainer = null;
        final List<DataPointContainerKey> keyObjectList = new ArrayList<>();

        // Get conditions from input query
        metricName = query.getName();
        tags = query.getTags();

        // Validate metric name, tags
        if (!metricName.startsWith(AUTO_INSERT_KAIROS_METRIC_PREFIX)) {
            verifyMetricName(metricName);
        } else {
            metricName = removeSpecialChars(metricName);
        }
        verifyTags(tags);

        // Query database to get Container keys
        try {
            rowKeyContainer =
                    gridstore.getCollection(metricName, RowKeyIndex.class);
        } catch (final GSTimeoutException e1) {
            // Case time out exception when waiting for GridDB for too long.
            throw new DatastoreException(CONNECTION_ERR, e1);
        } catch (final GSException e) {
            rowKeyContainer = null;
        }
        // If container does not exist, return empty value
        if (rowKeyContainer == null) {
            return keyObjectList.iterator();
        }

        // Query database to get rowKeyIndex container values
        gridDBQuery = rowKeyContainer.query("select *");
        queryResult = gridDBQuery.fetch();
        while (queryResult.hasNext()) {
            final RowKeyIndex rowKey = queryResult.next();
            String dataType;
            String keyTags;
            DataPointContainerKey dataPointKey;

            // Store data type, metric name and container key (String format)
            dataType = rowKey.dataType;
            dataPointKey = new DataPointContainerKey(metricName, dataType);

            // Remove dataType from rowKey value
            keyTags = rowKey.tagValue.replaceFirst(dataType, EMPTY_STRING);

            // Extract tags
            final String[] extractedTags = keyTags.split(DELIMITER);

            // Add tags to data point container key object
            for (int i = TAG_START_INDEX; i < extractedTags.length; i =
                    i + TAG_STEP) {
                final String tagName = extractedTags[i];
                final String tagValue = extractedTags[i + 1];
                dataPointKey.addTag(tagName, tagValue);
            }
            keyObjectList.add(dataPointKey);
        }

        // Filter keys by tags
        return filterContainerKeybyTags(keyObjectList, tags).iterator();
    }

    /**
     * Return container keys which contain specific tags from input container
     * keys list.
     *
     * @param inputKeyList
     *            input container keys list
     * @param inputTags
     *            condition to filter keys from list
     * @return filtered container keys list
     */
    private List<DataPointContainerKey> filterContainerKeybyTags(
            final List<DataPointContainerKey> inputKeyList,
            final SetMultimap<String, String> inputTags) {
        final List<DataPointContainerKey> resultKeyList = new ArrayList<>();
        boolean isValidKey = true;

        for (final DataPointContainerKey key : inputKeyList) {
            final Map<String, String> keyTags = key.getTags();

            // Loop through all tags defined in condition
            for (final String tag : inputTags.keySet()) {
                final String value = keyTags.get(tag);

                // Skip if key tags does not match with input tags
                if (value == null || !inputTags.get(tag).contains(value)) {
                    isValidKey = false;
                    break;
                }
            }
            if (isValidKey) {
                resultKeyList.add(key);
            }

            // Reset value for next key
            isValidKey = true;
        }
        return resultKeyList;
    }

    /**
     * Support list all item in String index container ("metric_names",
     * "tag_names", "tag_values").
     *
     * @param containerName
     *            container name
     * @return Iterable<String>
     * @throws DatastoreException
     *             gridstore exception
     */
    private Iterable<String> listAllItemStringIndex(final String containerName)
            throws DatastoreException {
        final List<String> ret = new ArrayList<>();
        GridStore gridstore = null;
        final Collection<String, StringIndex> col;

        try {
            gridstore = m_gridstorePool.borrowObject();
        } catch (final Exception e) {
            // Cannot object due to connection failure
            throw new DatastoreException(CONNECTION_ERR, e);
        }

        try {
            col = gridstore.getCollection(containerName, StringIndex.class);
        } catch (final GSException e1) {
            m_gridstorePool.returnObject(gridstore);
            throw new DatastoreException(CONNECTION_ERR, e1);
        }
        // List all items in collection
        if (col != null) {
            try {
                final Query<StringIndex> query = col.query("select *");
                final RowSet<StringIndex> rs = query.fetch();
                while (rs.hasNext()) {
                    final StringIndex rowMetric = rs.next();
                    ret.add(rowMetric.key);
                }
            } catch (final GSException e2) {
                throw new DatastoreException(e2);
            } finally {
                m_gridstorePool.returnObject(gridstore);
            }
        } else {
            m_gridstorePool.returnObject(gridstore);
        }
        return ret;
    }

    /**
     * Remove all special characters except letters and numbers.
     *
     * @param input
     *            Input String
     * @return String after removing special characters
     */
    private String removeSpecialChars(final String input) {
        return input.replaceAll("[^a-zA-Z0-9]+", "");
    }

    /**
     * Add metric name to Row key write buffer.
     *
     * @param dataPoint
     *            data point data
     * @param tags
     *            tag name, tag value
     * @param metricName
     *            metric name
     */
    private void addMetricNameToWriteBuffer(
            final org.kairosdb.core.DataPoint dataPoint,
            final SortedMap<String, String> tags, final String metricName,
            final DataPointContainerKey containerKey) {
        // Create new row in row key index with container name is metric
        final String containerRowKeyIndex =
                containerKey.getContainerRowKeyIndex();
        final RowKeyIndex row = new RowKeyIndex();
        row.tagValue = containerRowKeyIndex;
        row.dataType = dataPoint.getDataStoreDataType();
        m_rowKeyWriteBuffer.addData(metricName, row, 0);
    }

    /**
     * Add metric name, tag name, tag value to write buffer.
     *
     * @param tags
     *            tag name, tag value
     * @param metricName
     *            metric name
     * @throws GSException
     */
    private void addMetricTagNameTagValueToWriteBuffer(
            final SortedMap<String, String> tags, final String metricName)
            throws Exception {
        // Create 1 container for metric_names
        final String containerMetrics = "metric_names";
        final String cachedName = m_metricNameCache.cacheItem(metricName);
        if (cachedName == null) {
            final GridStore gridstore = m_gridstorePool.borrowObject();
            createContainer(WriteBuffer.BUFFER_TYPE_ROW_KEY_INDEX, metricName,
                    gridstore);
            m_gridstorePool.returnObject(gridstore);
            final StringIndex rowMetricName = new StringIndex();
            rowMetricName.key = metricName;
            m_stringindexWriteBuffer.addData(containerMetrics, rowMetricName,
                    0);
        }

        // Create 1 container for tag names and 1 for tag values
        final String containerTags = "tag_names";
        final String containerTagValue = "tag_values";
        for (final Map.Entry<String, String> entry : tags.entrySet()) {
            final String tagName = entry.getKey();
            final String tagValue = entry.getValue();
            final String cachedTagName = m_tagNameCache.cacheItem(tagName);
            if (cachedTagName == null) {
                final StringIndex rowTagName = new StringIndex();
                rowTagName.key = tagName;
                m_stringindexWriteBuffer.addData(containerTags, rowTagName, 0);
            }
            final String cachedValue = m_tagValueCache.cacheItem(tagValue);
            if (cachedValue == null) {
                final StringIndex rowTagInfo = new StringIndex();
                rowTagInfo.key = tagValue;
                m_stringindexWriteBuffer.addData(containerTagValue, rowTagInfo,
                        0);
            }
        }
    }

    /**
     * Add to data point write buffer.
     *
     * @param dataPoint
     *            data point data
     * @param tags
     *            tag name, tag value
     * @param metricName
     *            metric name
     * @param ttl
     *            time to live
     * @throws GSException
     *             gridstore connection
     */
    private void addDataPointToWriteBuffer(
            final org.kairosdb.core.DataPoint dataPoint,
            final SortedMap<String, String> tags, final String metricName,
            final int ttl, final DataPointContainerKey containerKey)
            throws DatastoreException {
        // Use byte[] to store data point value
        final KDataOutput kDataOutput = new KDataOutput();
        byte[] datapointValue = new byte[0];
        try {
            dataPoint.writeValueToBuffer(kDataOutput);
            datapointValue = kDataOutput.getBytes();
        } catch (final IOException e) {
            e.printStackTrace();
        }
        final String dpContainerName = containerKey.toString();
        final Date dpDate = new Date(dataPoint.getTimestamp());
        final DataPoint rowDataPoint = new DataPoint();
        rowDataPoint.timestamp = dpDate;
        rowDataPoint.val = datapointValue;

        final int pId;
        try {
            pId = this.getParitionId(dpContainerName);
        } catch (final GSException e) {
            // Catch error from GridDB, ex : timeout exception,...
            LOGGER.error(CONNECTION_ERR, e);
            throw new DatastoreException(CONNECTION_ERR, e);
        }
        m_dataPointWriteBufferList.get(pId % m_numberWriteBufferDataPoint)
                .addData(dpContainerName, rowDataPoint,
                        dataPoint.getTimestamp(), ttl);
    }

    /**
     * Create query runners with input parameters.
     *
     * @param containerKeys
     *            list of container keys
     * @param startTime
     *            start postion of query
     * @param endTime
     *            end position of query
     * @param queryCallback
     *            result storage
     * @param order
     *            sorting order of result
     * @param limit
     *            number of result to be returned
     * @param gridstore
     *            Gridstore object
     * @return list of query runners
     * @throws GSException
     *             exception when sending requests to GridDB
     */
    private List<QueryRunner> createRunners(
            final Iterator<DataPointContainerKey> containerKeys,
            final Date startTime, final Date endTime,
            final QueryCallback queryCallback, final QueryOrder order,
            final int limit, final GridStore gridstore) throws GSException {
        final List<QueryRunner> runners = new ArrayList<>();
        List<Query<DataPoint>> queryList = new ArrayList<>();
        String currentDataType = null;
        Map<String, String> currentTags = null;

        // Loop through all keys
        while (containerKeys.hasNext()) {
            final DataPointContainerKey key = containerKeys.next();
            if (currentDataType == null) {
                currentDataType = key.getDataType();
            }

            if (currentTags == null) {
                currentTags = key.getTags();
            }

            if (currentDataType.equals(key.getDataType())
                    && currentTags.equals(key.getTags())) {

                // Add new query to List for keys with same data type
                final TimeSeries<DataPoint> timeSeries = gridstore
                        .getTimeSeries(key.toString(), DataPoint.class);
                if (timeSeries != null) {
                    final Query<DataPoint> gridQuery =
                            timeSeries.query(startTime, endTime, order);
                    if (limit != 0) {
                        gridQuery.setFetchOption(FetchOption.LIMIT, limit);
                    }
                    queryList.add(gridQuery);
                }
            } else {

                // For new data type, run current query list and create new
                // list for new data type
                runners.add(new QueryRunner(queryList, queryCallback,
                        currentDataType, currentTags, m_kairosDataPointFactory,
                        gridstore));

                queryList = new ArrayList<>();
                final TimeSeries<DataPoint> timeSeries = gridstore
                        .getTimeSeries(key.toString(), DataPoint.class);
                if (timeSeries != null) {
                    final Query<DataPoint> gridQuery =
                            timeSeries.query(startTime, endTime, order);
                    queryList.add(gridQuery);
                    currentDataType = key.getDataType();
                    currentTags = key.getTags();
                }
            }
        }

        // Create runner for remaining queries
        if (!queryList.isEmpty()) {
            runners.add(
                    new QueryRunner(queryList, queryCallback, currentDataType,
                            currentTags, m_kairosDataPointFactory, gridstore));
        }

        return runners;
    }

    /**
     * Check if container key has datapoints in defined range.
     *
     * @param startTime
     *            start point
     * @param endTime
     *            end point
     * @param key
     *            container key
     * @param gridstore
     *            connection object
     * @return true if there are datapoints in defined range and vice versa
     * @throws GSException
     *             connection exception
     */
    private boolean isInTimeRange(final Date startTime, final Date endTime,
            final DataPointContainerKey key, final GridStore gridstore)
            throws GSException {

        // Get time series
        final TimeSeries<DataPoint> timeSeries =
                gridstore.getTimeSeries(key.toString(), DataPoint.class);
        if (timeSeries != null) {
            final Query<DataPoint> gridQuery =
                    timeSeries.query(startTime, endTime);
            final RowSet<DataPoint> rs = gridQuery.fetch();

            // Return true if container contains datapoints in range
            if (rs.size() > 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if input time of Long type is in valid range.
     *
     * @param time
     *            input time to for checking
     * @return OK if time is between valid range
     *         MAX if end time is max value of long type
     *         Exception if time is out of range
     */
    private String validateStartEndTime(final long startTime,
            final long endTime) {

        final long MINTIME = GRIDDB_MIN_TIMESTAMP; // Day 1/1/1970 00:00:00
        final long MAXTIME = GRIDDB_MAX_TIMESTAMP; // Day 31/12/9999 23:59:59

        String response = "OK";

        if (startTime < MINTIME || startTime > MAXTIME) {
            return INVALID_QUERY_CONDITION + " start_time";
        }

        if (endTime < MINTIME
                || (endTime > MAXTIME && endTime != Long.MAX_VALUE)) {
            return INVALID_QUERY_CONDITION + " end_time";
        }

        if (endTime == Long.MAX_VALUE) {
            response = "MAX";
        }
        return response;
    }

    /**
     * Check metric name with required conditions of gridDB.
     *
     * @param metricName
     *            metric name to be verified
     * @throws DatastoreException
     *             output if input value is not satisfied required conditions
     */
    private void verifyMetricName(final String metricName)
            throws DatastoreException {
        // Validate metric names
        if (!metricName.matches("[a-zA-Z0-9]+")
                || metricName.substring(0, 1).matches("[0-9]")) {
            throw new DatastoreException(INVALID_METRIC_NAME);
        }
    }

    /**
     * Check tags with required conditions of gridDB for put request.
     *
     * @param tags
     *            set of tag name, tag value to be verified
     * @throws DatastoreException
     *             output if input value is not satisfied required conditions
     */
    private void verifyTags(final SortedMap<String, String> tags)
            throws DatastoreException {
        for (final String tagName : tags.keySet()) {
            final String tagValue = tags.get(tagName);
            if (!tagName.matches("[a-zA-Z0-9]+")) {
                throw new DatastoreException(INVALID_TAG_NAME);
            }
            if (!tagValue.matches("[a-zA-Z0-9]+")) {
                throw new DatastoreException(INVALID_TAG_VALUE);
            }
        }
    }

    /**
     * Check tags with required conditions of gridDB for query, delete request.
     *
     * @param tags
     *            set of tag name, tag value to be verified
     * @throws DatastoreException
     *             output if input value is not satisfied required conditions
     */
    private void verifyTags(final SetMultimap<String, String> tags)
            throws DatastoreException {
        // Validate tags
        for (final String tagName : tags.keySet()) {
            final Set<String> valueSet = tags.get(tagName);

            // Validate tag names
            if (!tagName.matches("[a-zA-Z0-9]+")) {
                throw new DatastoreException(INVALID_TAG_NAME);
            }

            // Validate tag value
            for (final String tagValue : valueSet) {
                if (!tagValue.matches("[a-zA-Z0-9]+")) {
                    throw new DatastoreException(INVALID_TAG_VALUE);
                }
            }
        }
    }

    /**
     * Create container before put row to container in WriteBuffer.
     *
     * @param bufferType
     *            Buffer type
     * @param containerName
     *            container name
     * @param gridstore
     *            GridStore object
     * @throws GSException
     *             gridstore connection
     */
    private void createContainer(final String bufferType,
            final String containerName, final GridStore gridstore)
            throws GSException {
        this.createContainer(bufferType, containerName, gridstore, 0);
    }

    /**
     * Create container before put row to container in WriteBuffer.
     *
     * @param bufferType
     *            Buffer type
     * @param containerName
     *            container name
     * @param gridstore
     *            GridStore object
     * @throws GSException
     *             gridstore connection
     */
    private void createContainer(final String bufferType,
            final String containerName, final GridStore gridstore, Integer ttl)
            throws GSException {
        if (bufferType.equals(WriteBuffer.BUFFER_TYPE_DATAPOINT)) {
            // Prevent error when change TTL : will not update already created
            // TimeSeries
            if (gridstore.getTimeSeries(containerName,
                    DataPoint.class) == null) {
                if (ttl == 0) {
                    ttl = m_griddbConfiguration.getDatapointTtl();
                }
                if (ttl > 0) {
                    final TimeSeriesProperties timseriesProp =
                            new TimeSeriesProperties();
                    // Update TTL to data points.
                    timseriesProp.setRowExpiration(
                            m_griddbConfiguration.getDatapointTtl(),
                            com.toshiba.mwcloud.gs.TimeUnit.SECOND);
                    gridstore.putTimeSeries(containerName, DataPoint.class,
                            timseriesProp, false);
                } else {
                    gridstore.putTimeSeries(containerName, DataPoint.class);
                }
            }
        } else if (bufferType.equals(WriteBuffer.BUFFER_TYPE_ROW_KEY_INDEX)) {
            gridstore.putCollection(containerName, RowKeyIndex.class);
        } else {
            // String index
            gridstore.putCollection(containerName, StringIndex.class);
        }
    }

    /**
     * create cache for method getPartitionIndexOfContainer
     *
     * @param containerName
     * @return
     * @throws GSException
     */
    private Integer getParitionId(final String containerName)
            throws GSException {
        Integer id = m_hashPartitionId.get(containerName);
        if (id == null) {
            id = m_gridstoreForPId.getPartitionController()
                    .getPartitionIndexOfContainer(containerName);
            m_hashPartitionId.put(containerName, id);
        }
        return id;

    }
}
