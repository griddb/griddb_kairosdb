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
 * This file is based on the file WriteBuffer.java
 * in https://github.com/kairosdb/kairosdb/archive/v1.1.1.zip
 * (KairosDB 1.1.1).
 */
package org.kairosdb.datastore.griddb;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.util.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.toshiba.mwcloud.gs.Aggregation;
import com.toshiba.mwcloud.gs.AggregationResult;
import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.ColumnInfo;
import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.ContainerType;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GSTimeoutException;
import com.toshiba.mwcloud.gs.GSType;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowKey;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.TimeSeries;
import com.toshiba.mwcloud.gs.common.RowMapper;
import com.toshiba.mwcloud.gs.common.RowMapper.Tool;

/**
 * Buffer support add, delete data points.
 *
 */
public class WriteBuffer implements Runnable {
    /**
     * Logger.
     */
    public static final Logger LOGGER =
            LoggerFactory.getLogger(WriteBuffer.class);

    /**
     * Buffer type data point.
     */
    public static final String BUFFER_TYPE_DATAPOINT = "DATAPOINT";

    /**
     * Buffer type row key index constant.
     */
    public static final String BUFFER_TYPE_ROW_KEY_INDEX = "ROW_KEY_INDEX";

    /**
     * Buffer type string index constant.
     */
    public static final String BUFFER_TYPE_STRING_INDEX = "STRING_INDEX";

    /**
     * Connection error with GridDB.
     */
    public static final String CONNECTION_ERR = "Connection error with GridDB.";

    /**
     * Support add and delete datapoint.
     *
     */
    @SuppressWarnings("ignoreAnnotationCanonicalNames")
    static public class DataPoint {
        /**
         * Time stamp of data point, row key.
         */
        @RowKey
        public Date timestamp;
        /**
         * Value is store in byte[].
         */
        public byte[] val;

    }

    /**
     * Support store row key index container of GridDB.
     *
     */
    @SuppressWarnings("ignoreAnnotationCanonicalNames")
    static public class RowKeyIndex {
        /**
         * Tag value.
         */
        @RowKey
        public String tagValue;

        /**
         * KairosDB data's type.
         */
        public String dataType;
    }

    /**
     *
     * Support store string index container of GridDB.
     *
     */
    @SuppressWarnings("ignoreAnnotationCanonicalNames")
    static public class StringIndex {
        /**
         * Value.
         */
        @RowKey
        public String key;
    }

    /**
     * Mutator lock.
     */
    private final ReentrantLock m_mutatorLock;
    
    /**
     * Buffer.
     */
    private List<Triple<String, Object, String>> m_buffer;
    
    /**
     * Buffer count.
     */
    private volatile int m_bufferCount = 0;
    
    /**
     * Max buffer size.
     */
    private final int m_maxBufferSize;
    
    /**
     * Support run thread.
     */
    private final ExecutorService m_executorService;
    
    /**
     * After write delay, data is written to GridDB.
     */
    private final int m_writeDelay;
    
    /**
     * Support close buffer when kairosdb shutdown.
     */
    private boolean m_exit = false;
    
    /**
     * Support write data.
     */
    private final Thread m_writeThread;
    
    /**
     * Support object pool pattern.
     */
    private final GridStorePool<GridStore> m_gridstorePool;

    /**
     * Maximum query date.
     */
    private static final String MAXDATE = "9999-12-31";

    /**
     * Minimum query date.
     */
    private static final String MINDATE = "1970-01-02";

    /**
     * Buffer type.
     */
    private final String m_bufferType;

    /**
     * Create container object, support push data to GridDB.
     */
    private ContainerInfo m_containerInfo;
    
    /**
     * Gridstore object.
     */
    private GridStore m_gridstore = null;
    
    /**
     * Cache RowMapper object for Write Buffer.
     */
    private final HashMap<String, RowMapper> m_resolveMapperArr;

    /**
     *
     * Constructor.
     *
     * @param writeDelay
     *            : after writeDelay time (mili-second), data will be written to
     *            GridDB
     * @param maxWriteSize
     *            : maximum buffer size, when buffer is full, data will be
     *            written to GridDB
     * @param mutatorLock
     *            : use for concurrency process
     * @param gridstorePool
     *            : use for store data to GridDB
     * @param type
     *            : buffer type
     * @param threadCount
     *            : number of thread use to call multiPut
     */
    public WriteBuffer(final int writeDelay, final int maxWriteSize,
            final ReentrantLock mutatorLock,
            final GridStorePool<GridStore> gridstorePool, final String type,
            final int threadCount,
            final HashMap<String, RowMapper> resolveMapperArr) {
        m_mutatorLock = new ReentrantLock();
        m_buffer = new ArrayList<>();
        m_maxBufferSize = maxWriteSize;
        m_executorService = Executors.newFixedThreadPool(threadCount);
        m_writeDelay = writeDelay;
        m_gridstorePool = gridstorePool;
        m_bufferType = type;
        this.m_resolveMapperArr = resolveMapperArr;

        try {
            // Create container with dummy name, the name will be change by
            // method ContainerInfo::setName in each schema.
            m_containerInfo = this.createContainerInfo("ContainerDummyName");
        } catch (final GSException e) {
            LOGGER.error(CONNECTION_ERR, e);
            e.printStackTrace();
        }

        try {
            m_gridstore = m_gridstorePool.borrowObject();
        } catch (final Exception e) {
            LOGGER.error(CONNECTION_ERR, e);
            e.printStackTrace();
        }

        m_writeThread = new Thread(this);
        m_writeThread.start();
    }

    /**
     * Thread run.
     */
    @Override
    public final void run() {
        while (!m_exit) {
            try {
                Thread.sleep(m_writeDelay);
            } catch (final InterruptedException ignored) {
            }
            if (m_bufferCount != 0) {
                m_mutatorLock.lock();
                try {
                    submitJob();
                } finally {
                    m_mutatorLock.unlock();
                }
            }
        }
    }

    /**
     * Add data to write buffer with 3 parameters.
     *
     * @param containerName
     *            container name
     * @param data
     *            row data
     * @param timestamp
     *            timestamp of data point
     */
    public final void addData(final String containerName, final Object data,
            final long timestamp) {
        addData(containerName, data, timestamp, 0);
    }

    /**
     * Add data to write buffer with 4 parameters.
     *
     * @param containerName
     *            container name
     * @param data
     *            row data
     * @param timestamp
     *            time stamp of data point
     * @param ttl
     *            time to live
     */
    public final void addData(final String containerName, final Object data,
            final long timestamp, final int ttl) {
        m_mutatorLock.lock();
        try {
            waitOnBufferFull();
            m_bufferCount++;

            m_buffer.add(new Triple<>(containerName, data, "", timestamp, ttl));
        } finally {
            m_mutatorLock.unlock();
        }
    }

    /**
     * Delete data from write buffer.
     *
     * @param rowKey
     *            container row key
     * @param startTime
     *            start time
     * @param endTime
     *            end time
     * @param delimiter
     *            delimiter (space character)
     * @return Boolean
     * @throws DatastoreException
     *             throw exception when error
     */
    @SuppressWarnings("finally")
    public final Boolean deleteData(final DataPointContainerKey rowKey,
            final long startTime, final long endTime, final String delimiter)
            throws DatastoreException {
        m_mutatorLock.lock();
        GridStore gridstore = null;
        boolean result = false;

        try {
            // Use different gridstore object for deleteData because gridstore
            // object is not thread-safe and m_gridstore is used frequently by
            // WriteDataJob which runs on different thread
            gridstore = m_gridstorePool.borrowObject();
        } catch (final Exception e) {
            // Case can't check out gridstore object, return error .
            throw new DatastoreException(CONNECTION_ERR, e);
        }

        try {
            waitOnBufferFull();
            m_bufferCount++;
            result = deleteDataFromGridDB(rowKey, startTime, endTime, delimiter,
                    gridstore);
        } finally {
            m_mutatorLock.unlock();
            m_gridstorePool.returnObject(gridstore);
        }
        return result;
    }

    /**
     * Close write buffer.
     *
     * @throws InterruptedException
     *             throw interruption exception
     */
    public final void close() throws InterruptedException {
        m_exit = true;
        m_writeThread.interrupt();
        m_writeThread.join();
    }

    /**
     * Wait until buffer full or time to write data to push data to GridDB.
     */
    private void waitOnBufferFull() {
        if (m_bufferCount > m_maxBufferSize
                && m_mutatorLock.getHoldCount() == 1) {
            submitJob();
        }
    }

    /**
     * Submit data to GridDB.
     */
    private void submitJob() {
        final List<Triple<String, Object, String>> buffer;
        buffer = m_buffer;
        m_buffer = new ArrayList<>();
        m_bufferCount = 0;
        final WriteDataJob writeDataJob = new WriteDataJob(buffer, m_bufferType,
                m_containerInfo, m_gridstore, m_resolveMapperArr);
        // submit job
        m_executorService.submit(writeDataJob);
        writeDataJob.waitTillStarted();
    }

    /**
     * Delete data from GridDB.
     *
     * @param rowKey
     *            container row key
     * @param startTime
     *            start time
     * @param endTime
     *            end time
     * @param delimiter
     *            delimiter (space character)
     * @return boolean
     * @throws DatastoreException
     *             throw exception when can't delete.
     */
    private boolean deleteDataFromGridDB(final DataPointContainerKey rowKey,
            final long startTime, final long endTime, final String delimiter,
            final GridStore gridstore) throws DatastoreException {
        boolean result = true;
        final String containerName;
        final String rowKeyIndexStr;
        containerName = rowKey.toString();
        rowKeyIndexStr = rowKey.getContainerRowKeyIndex();
        TimeSeries<DataPoint> tsContainer = null;

        final SimpleDateFormat fmt =
                new SimpleDateFormat("yyyy-MM-dd", Locale.JAPAN);
        Date maxDate = null;
        Date minDate = null;
        Date endDate = null;
        Date startDate = null;
        try {
            maxDate = fmt.parse(MAXDATE);
            minDate = fmt.parse(MINDATE);
            endDate = new Date(endTime);
            if (endDate.compareTo(maxDate) > 0) {
                endDate = fmt.parse(MAXDATE);
            }
            startDate = new Date(startTime);
            if (startDate.compareTo(minDate) <= 0) {
                startDate = fmt.parse(MINDATE);
            }
        } catch (final ParseException e) {
            result = false;
            e.printStackTrace();
        }

        try {
            tsContainer = gridstore.getTimeSeries(containerName, DataPoint.class);
        } catch (final GSException e) {
            result = false;
            throw new DatastoreException(CONNECTION_ERR);
        }
        if (tsContainer != null) {
            try {
                final RowSet<DataPoint> rs =
                        tsContainer.query(startDate, endDate).fetch();
                while (rs.hasNext()) {
                    final DataPoint rowDataPoint = rs.next();
                    tsContainer.remove(rowDataPoint.timestamp);
                }
            } catch (final GSException e) {
                result = false;
                throw new DatastoreException(CONNECTION_ERR);
            }
        }

        // Case data point container is delete all item => delete metric
        // information in string index container

        // Count remain item in data point container
        final AggregationResult numberRowAgg;
        try {
            if (tsContainer != null) {
                numberRowAgg = tsContainer.aggregate(minDate, maxDate, "timestamp",
                        Aggregation.COUNT);
            } else {
                numberRowAgg = null;
            }
        } catch (final GSException e) {
            result = false;
            throw new DatastoreException(CONNECTION_ERR);
        }
        final long numRow;
        if (numberRowAgg != null) {
            numRow = numberRowAgg.getLong();
        } else {
            numRow = 0;
        }

        if (numRow == 0) {
            try {
                gridstore.dropTimeSeries(containerName);
            } catch (final GSException e) {
                throw new DatastoreException(CONNECTION_ERR);
            }
            // Delete in container cache and resolve mapper.
            m_resolveMapperArr.remove(containerName);
            try {
                final Collection<String, StringIndex> collection = gridstore
                        .getCollection("metric_names", StringIndex.class);
                if (collection != null) {
                    collection.remove(rowKey.getMetricName());
                }
            } catch (final GSException e) {
                result = false;
                throw new DatastoreException(CONNECTION_ERR);
            }

            // Remove in row key index container
            try {
                final Collection<String, RowKeyIndex> rowKeyIndexCol =
                        gridstore.getCollection(rowKey.getMetricName(),
                                RowKeyIndex.class);
                if (rowKeyIndexCol != null) {
                    rowKeyIndexCol.remove(rowKeyIndexStr);
                }
            } catch (final GSException e) {
                result = false;
                throw new DatastoreException(CONNECTION_ERR);
            }
        }
        return result;
    }

    /**
     * Create row for GridDB.
     *
     * @param rowListMap
     *            row list map
     * @param containerName
     *            container name
     * @param gridstore
     *            GridStore object
     * @param rowObj
     *            row data
     * @return row Return Row object
     * @throws GSException
     *             gridstore connection
     */
    private ContainerInfo createContainerInfo(final String containerName)
            throws GSException {
        ContainerInfo row;
        if (this.m_bufferType == WriteBuffer.BUFFER_TYPE_DATAPOINT) {
            // Case data point
            row = createDataPointConInfo(containerName);
        } else if (this.m_bufferType == WriteBuffer.BUFFER_TYPE_ROW_KEY_INDEX) {
            // Case row key index
            row = createRowKeyConInfo(containerName);
        } else {
            // String index
            row = createStringIndexConInfo(containerName);
        }
        return row;
    }

    /**
     * Create ContainerInfo object for data point container.
     *
     * @param name
     *            container name
     * @return Container Info object
     */

    private ContainerInfo createDataPointConInfo(final String name) {
        if (m_containerInfo != null) {
            if (m_containerInfo.getName() != name) {
                m_containerInfo.setName(name);
            }
            return m_containerInfo;
        }
        final List<ColumnInfo> ColumnInfoList = new ArrayList<>();
        ColumnInfoList.add(new ColumnInfo("timestamp", GSType.TIMESTAMP));
        ColumnInfoList.add(new ColumnInfo("val", GSType.BYTE_ARRAY));
        m_containerInfo = new ContainerInfo(name, ContainerType.TIME_SERIES,
                ColumnInfoList, true);
        return m_containerInfo;
    }

    /**
     * Create ContainerInfo object for row key index container.
     *
     * @param name
     *            container name
     * @return Container Info object
     */
    private ContainerInfo createRowKeyConInfo(final String name) {
        if (m_containerInfo != null) {
            if (m_containerInfo.getName() != name) {
                m_containerInfo.setName(name);
            }
            return m_containerInfo;
        }
        final List<ColumnInfo> ColumnInfoList = new ArrayList<>();
        ColumnInfoList.add(new ColumnInfo("tagValue", GSType.STRING));
        ColumnInfoList.add(new ColumnInfo("dataType", GSType.STRING));
        m_containerInfo = new ContainerInfo(name, ContainerType.COLLECTION,
                ColumnInfoList, true);
        return m_containerInfo;
    }

    /**
     * Create ContainerInfo object for string index container.
     *
     * @param name
     *            container name
     * @return Container Info object
     */
    private ContainerInfo createStringIndexConInfo(final String name) {
        if (m_containerInfo != null) {
            if (m_containerInfo.getName() != name) {
                m_containerInfo.setName(name);
            }
            return m_containerInfo;
        }
        final List<ColumnInfo> ColumnInfoList = new ArrayList<>();
        ColumnInfoList.add(new ColumnInfo("key", GSType.STRING));

        m_containerInfo = new ContainerInfo(name, ContainerType.COLLECTION,
                ColumnInfoList, true);
        return m_containerInfo;
    }

    /**
     * Write data to GridDB.
     *
     */
    private static class WriteDataJob implements Runnable {
        /**
         * Lock.
         */
        private final Object m_jobLock;
        /**
         * Check start yet.
         */
        private boolean m_started = false;

        /**
         * Buffer.
         */
        private final List<Triple<String, Object, String>> m_buffer;
        
        /**
         * Buffer type.
         */
        private final String m_bufferType;

        /**
         * Store containerInfo.
         */
        private final ContainerInfo m_containerInfo;

        /**
         * Use same gridstore object for write buffer.
         */
        private final GridStore m_gridstore;

        /**
         * Store Hash map RowMapper object.
         */
        private final HashMap<String, RowMapper> m_resolveMapperArr;

        /**
         * Constructor.
         *
         * @param buffer
         *            buffer to write data
         * @param gridstorePool
         *            grid store pull
         * @param bufType
         *            buffer type
         * @param containerInfoArr
         *            : cache for ContainerInfo object.
         * @param resolveMapperArr
         *            : cache for RowMapper object.
         */
        WriteDataJob(final List<Triple<String, Object, String>> buffer,
                final String bufType, final ContainerInfo containerInfo,
                final GridStore m_gridstore,
                final HashMap<String, RowMapper> resolveMapperArr) {
            m_jobLock = new Object();
            m_buffer = buffer;
            m_bufferType = bufType;
            this.m_containerInfo = containerInfo;
            this.m_gridstore = m_gridstore;
            this.m_resolveMapperArr = resolveMapperArr;
        }

        /**
         * Wait until started.
         */
        public void waitTillStarted() {
            synchronized (m_jobLock) {
                while (!m_started) {
                    try {
                        m_jobLock.wait();
                    } catch (final InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        /**
         * Thread run.
         */
        @Override
        public void run() {
            synchronized (m_jobLock) {
                m_started = true;
                m_jobLock.notifyAll();
            }
            String containerName;
            final Map<String, List<Row>> rowListMap;
            rowListMap = new HashMap<>();

            try {
                // When timeout, it will stop.
                for (final Triple<String, Object, String> data : m_buffer) {
                    containerName = data.getFirst();
                    Row row = null;
                    final Object rowObj = data.getSecond();
                    rowListMap.putIfAbsent(containerName, new ArrayList<Row>());
                    row = createRow(rowListMap, containerName, m_gridstore,
                            rowObj);
                    if (row != null) {
                        rowListMap.get(containerName).add(row);
                    }
                }
            } catch (final GSTimeoutException e2) {
                LOGGER.error(CONNECTION_ERR, e2);
                e2.printStackTrace();
                return;
            } catch (final GSException e1) {
                LOGGER.error(CONNECTION_ERR, e1);
                e1.printStackTrace();
                return;
            }
            if (rowListMap.isEmpty()) {
                return;
            }
            try {
                m_gridstore.multiPut(rowListMap);
            } catch (final GSException e) {
                LOGGER.error(CONNECTION_ERR, e);
                e.printStackTrace();
            }
        }

        /**
         * Create row data point.
         *
         * @param containerName
         *            container name
         * @param gridstore
         *            Gridstore
         * @param rowListMap
         *            row list map
         * @param rowObj
         *            row object
         * @return Row
         * @throws GSException
         *             gridstore connection
         */
        private Row createRowDataPoint(final String containerName,
                final GridStore gridstore,
                final Map<String, List<Row>> rowListMap, final Object rowObj)
                throws GSException {
            Row row;
            final ContainerInfo containerInfo =
                    createDataPointConInfo(containerName);
            
            row = createRowForContainer(containerName, gridstore,
                    containerInfo);
            final DataPoint dp = (DataPoint) rowObj;
            row.setTimestamp(0, dp.timestamp);
            row.setByteArray(1, dp.val);
            return row;
        }

        /**
         * Create row row key index.
         *
         * @param containerName
         *            container name
         * @param gridstore
         *            Gridstore
         * @param rowListMap
         *            row list map
         * @param rowObj
         *            row object
         * @return Row
         * @throws GSException
         *             gridstore connection
         */
        private Row createRowRowKeyIndex(final String containerName,
                final GridStore gridstore,
                final Map<String, List<Row>> rowListMap, final Object rowObj)
                throws GSException {
            Row row;
            final ContainerInfo containerInfo =
                    createRowKeyConInfo(containerName);
            
            row = createRowForContainer(containerName, gridstore,
                    containerInfo);
            final RowKeyIndex rowKeyIndex = (RowKeyIndex) rowObj;
            row.setString(0, rowKeyIndex.tagValue);
            row.setString(1, rowKeyIndex.dataType);
            return row;
        }

        /**
         * Create row string index.
         *
         * @param containerName
         *            container name
         * @param gridstore
         *            Gridstore
         * @param rowListMap
         *            row list map
         * @param rowObj
         *            row object
         * @return Row
         * @throws GSException
         *             gridstore connection
         */
        private Row createRowStringIndex(final String containerName,
                final GridStore gridstore,
                final Map<String, List<Row>> rowListMap, final Object rowObj)
                throws GSException {
            Row row;

            final ContainerInfo containerInfo =
                    createStringIndexConInfo(containerName);
            
            row = createRowForContainer(containerName, gridstore,
                    containerInfo);
            final StringIndex stringIndex = (StringIndex) rowObj;
            row.setString(0, stringIndex.key);
            return row;
        }

        /**
         * Create row for GridDB.
         *
         * @param rowListMap
         *            row list map
         * @param containerName
         *            container name
         * @param gridstore
         *            GridStore object
         * @param rowObj
         *            row data
         * @return row Return Row object
         * @throws GSException
         *             gridstore connection
         */
        private Row createRow(final Map<String, List<Row>> rowListMap,
                final String containerName, final GridStore gridstore,
                final Object rowObj) throws GSException {
            Row row;
            if (this.m_bufferType == WriteBuffer.BUFFER_TYPE_DATAPOINT) {
                // Case data point
                row = createRowDataPoint(containerName, gridstore, rowListMap,
                        rowObj);
            } else if (this.m_bufferType == WriteBuffer.BUFFER_TYPE_ROW_KEY_INDEX) {
                // Case row key index
                row = createRowRowKeyIndex(containerName, gridstore, rowListMap,
                        rowObj);
            } else {
                // String index
                row = createRowStringIndex(containerName, gridstore, rowListMap,
                        rowObj);
            }
            return row;
        }

        /**
         * Create ContainerInfo object for data point container.
         *
         * @param name
         *            container name
         * @return Container Info object
         */

        private ContainerInfo createDataPointConInfo(final String name) {
            if (m_containerInfo.getName() != name) {
                m_containerInfo.setName(name);
            }
            return m_containerInfo;
        }

        /**
         * Create ContainerInfo object for row key index container.
         *
         * @param name
         *            container name
         * @return Container Info object
         */
        private ContainerInfo createRowKeyConInfo(final String name) {
            if (m_containerInfo.getName() != name) {
                m_containerInfo.setName(name);
            }
            return m_containerInfo;
        }

        /**
         * Create ContainerInfo object for string index container.
         *
         * @param name
         *            container name
         * @return Container Info object
         */
        private ContainerInfo createStringIndexConInfo(final String name) {
            if (m_containerInfo.getName() != name) {
                m_containerInfo.setName(name);
            }
            return m_containerInfo;
        }

        /**
         * Method to reduce time to create new Row object
         *
         * @param containerName
         *            container name
         * @param gridstore
         *            GridStore object.
         * @param containerInfo
         *            reused ContainerInfo object
         * @return Row object.
         * @throws GSException
         *             gridstore connection
         */
        private Row createRowForContainer(final String containerName,
                final GridStore gridstore, final ContainerInfo containerInfo)
                throws GSException {

            Row row = null;
            RowMapper rowMapper = m_resolveMapperArr.get(containerName);
            if (rowMapper == null) {
                rowMapper = Tool.resolveMapper(containerInfo);
                if (rowMapper != null) {
                    m_resolveMapperArr.put(containerName, rowMapper);
                }
            }
            row = rowMapper.createGeneralRow();
            return row;
        }
    }
}
