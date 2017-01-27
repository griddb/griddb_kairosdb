/*
 * Copyright 2013 Proofpoint Inc.
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
 * This file is based on the file DataPointRowKey.java
 * in https://github.com/kairosdb/kairosdb/archive/v1.1.1.zip
 * (KairosDB 1.1.1).
 */
package org.kairosdb.datastore.griddb;

//modified: remove unused import ByteBuffer
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public class DataPointContainerKey
{
    /**
     * Modified: 
     * Remove unused attributes: m_timestamp, m_endSearchKey,
     * m_serializeBUffer.
     * 
     * Remove unused methods: getTimestamp(), isEndSearchKey(),
     * setEndSearchKey(), getSerializedBuffer(), setSerializedBuffer().
     * 
     * Add new attributes: HASH_NUMBER, m_toStringVal.
     */
	private final String m_metricName;
	private final String m_dataType;
	private final SortedMap<String, String> m_tags;

    /**
     * Hash number, support toHash() method.
     */
    private static final int HASH_NUMBER = 31;

    /**
     * Return of toString() function after first call.
     */
    private String m_toStringVal = null;

    /**
     * Modified:
     * Remove parameter timestamp
     */
	public DataPointContainerKey(final String metricName,
            final String dataType)
	{
        this(metricName, dataType, new TreeMap<String, String>());
	}

    /**
     * Modified:
     * Remove parameter timestamp, add null checking for tags
     */
	public DataPointContainerKey(final String metricName, final String datatype,
            final SortedMap<String, String> tags) 
	{
        m_metricName = checkNotNullOrEmpty(metricName);
        m_dataType = checkNotNull(datatype);
        m_tags = checkNotNull(tags);

	}

	/**
	 * Modified:
	 * Add null/empty checking for name, value 
	 */
	public void addTag(String name, String value)
	{
        checkNotNullOrEmpty(name);
        checkNotNullOrEmpty(value);
        m_tags.put(name, value);
	}

	public String getMetricName()
	{
		return m_metricName;
	}

	public SortedMap<String, String> getTags()
	{
		return m_tags;
	}

	/**
	 If this returns "" (empty string)` then it is the old row key format and the data type
	 is determined by the timestamp bit in the column.
	 @return
	 */
	public String getDataType()
	{
		return m_dataType;
	}

	/**
	 * Modified:
	 * Remove comparison for timestamp
	 * Rename DataPointsRowKey object to DataPointContainerKey
	 */
	@Override
	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		DataPointContainerKey that = (DataPointContainerKey) o;

		if (m_dataType != null ? !m_dataType.equals(that.m_dataType) : that.m_dataType != null)
			return false;
		if (!m_metricName.equals(that.m_metricName)) return false;
		if (!m_tags.equals(that.m_tags)) return false;

		return true;
	}

	/**
	 * Modified:
	 * Remove timestamp
	 * Remove magic number
	 */
	@Override
	public int hashCode()
	{
		int result = m_metricName.hashCode();
		result = HASH_NUMBER * result + (m_dataType != null ? m_dataType.hashCode() : 0);
		result = HASH_NUMBER * result + m_tags.hashCode();
		return result;
	}

    /**
     * Modified:
     * Implement specific structure for GridDB Container Key
     * 
     * Return container key in String format
     * "metricName_dataType_tagName_tagValue".
     *
     * @return container key converted to String
     */
    @Override
    public final String toString() {
        if (m_toStringVal == null) {
            final StringBuilder builder = new StringBuilder();

            // Append "metricName_dataType"
            builder.append(m_metricName);
            builder.append(GriddbDatastore.DELIMITER);
            builder.append(m_dataType);

            // Append "_tagName_tagValue"
            for (final Entry<String, String> tag : m_tags.entrySet()) {
                builder.append(GriddbDatastore.DELIMITER);
                builder.append(tag.getKey());
                builder.append(GriddbDatastore.DELIMITER);
                builder.append(tag.getValue());
            }
            m_toStringVal = builder.toString();
        }
        return m_toStringVal;
    }
    
    /**
     * Created new:
     * 
     * Get container row key index string:
     * datatype_tagname1_tagvalue1_tagnamen_tagvaluen
     *
     * @return
     */
    public final String getContainerRowKeyIndex() {
        final StringBuffer buf = new StringBuffer();
        buf.append(m_dataType);
        for (final Map.Entry<String, String> entry : m_tags.entrySet()) {
            final String tagName = entry.getKey();
            final String tagValue = entry.getValue();
            buf.append(GriddbDatastore.DELIMITER);
            buf.append(tagName);
            buf.append(GriddbDatastore.DELIMITER);
            buf.append(tagValue);
        }
        final String containerRowKeyIndex = buf.toString();
        return containerRowKeyIndex;
    }
}
