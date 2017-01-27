package org.kairosdb.datastore.griddb;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * This class is used to read configuration values from
 * properties file and provide functions to allow other classes
 * retrieving these values.
 *
 */
public class GriddbConfiguration {
    /**
     * Cluster name variable in properies file.
     */
    public static final String CLUSTER_NAME =
            "kairosdb.datastore.griddb.cluster_name";
    /**
     * Multi-cast IP address variable in properies file.
     */
    public static final String NOTIFICATION_ADDRESS =
            "kairosdb.datastore.griddb.notification_address";
    /**
     * Connection port variable in properies file.
     */
    public static final String NOTIFICATION_PORT =
            "kairosdb.datastore.griddb.notification_port";
    /**
     * List of member of cluster variable in properies file.
     */
    public static final String NOTIFICATION_MEMBER =
            "kairosdb.datastore.griddb.notification_member";
    /**
     * Provider url variable in properies file.
     */
    public static final String NOTIFICATION_PROVIDER_URL =
            "kairosdb.datastore.griddb.notification_provider_url";
    /**
     * Data point time to live variable in properies file.
     */
    public static final String DATAPOINT_TTL =
            "kairosdb.datastore.griddb.datapoint_ttl";
    /**
     * User name variable in properies file.
     */
    public static final String USER = "kairosdb.datastore.griddb.user";
    /**
     * User password variable in properies file.
     */
    public static final String PASSWORD = "kairosdb.datastore.griddb.password";
    /**
     * Time to commit client's update variable in properies file.
     */
    public static final String CONSISTENCY =
            "kairosdb.datastore.griddb.consistency";
    /**
     * Transaction timeout max value variable in properies file.
     */
    public static final String TRANSACTION_TIMEOUT =
            "kairosdb.datastore.griddb.transaction_timeout";
    /**
     * Failover timeout variable in properies file.
     */
    public static final String FAILOVER_TIMEOUT =
            "kairosdb.datastore.griddb.failover_timeout";
    /**
     * GridDB container cache size variable in properies file.
     */
    public static final String CONTAINER_CACHE_SIZE =
            "kairosdb.datastore.griddb.container_cache_size";
    /**
     * Data affinity pattern variable in properies file.
     */
    public static final String DATA_AFFINITY_PATTERN =
            "kairosdb.datastore.griddb.data_affinity_pattern";
    /**
     * Number of connection at a time variable in properies file.
     */
    public static final String NUMBER_REQUEST_CONCURRENCY =
            "kairosdb.datastore.griddb.number_request_concurrency";
    /**
     * Max buffer size used by this module variable in properies file.
     */
    public static final String MAX_WRITE_BUFFER_SIZE =
            "kairosdb.datastore.griddb.max_write_buffer_size";
    /**
     * Max cache size used by this module variable in properies file.
     */
    public static final String MAX_DATA_CACHE_SIZE =
            "kairosdb.datastore.griddb.max_data_cache_size";
    /**
     * Delay time before putting data to Database variable in properies file.
     */
    public static final String WRITE_DELAY =
            "kairosdb.datastore.griddb.write_delay";

    /**
     * Number of write buffer for put data point in properies file.
     */
    public static final String NUMBER_WRITE_BUFFER_DATAPOINT =
            "kairosdb.datastore.griddb.write_buffer_datapoint";
    /**
     * Cluster name value.
     */
    @Inject
    @Named(CLUSTER_NAME)
    private String m_clusterName;

    /**
     * IP address value.
     */
    @Inject
    @Named(NOTIFICATION_ADDRESS)
    private String m_notificationAddress;

    /**
     * Port value.
     */
    @Inject
    @Named(NOTIFICATION_PORT)
    private String m_notificationPort;

    /**
     * Member list value.
     */
    @Inject
    @Named(NOTIFICATION_MEMBER)
    private String m_notificationMember;

    /**
     * Provider url value.
     */
    @Inject
    @Named(NOTIFICATION_PROVIDER_URL)
    private String m_notificationProviderUrl;

    /**
     * Time to live value.
     */
    @Inject(optional = true)
    @Named(DATAPOINT_TTL)
    private int m_datapointTtl = 0;

    /**
     * Name value.
     */
    @Inject
    @Named(USER)
    private String m_user;

    /**
     * Password value.
     */
    @Inject
    @Named(PASSWORD)
    private String m_password;

    /**
     * Commitment time value.
     */
    @Inject
    @Named(CONSISTENCY)
    private String m_consistency;

    /**
     * Transaction timeout value.
     */
    @Inject
    @Named(TRANSACTION_TIMEOUT)
    private String m_transactionTimeout;

    /**
     * Failover timeout value.
     */
    @Inject
    @Named(FAILOVER_TIMEOUT)
    private String m_failoverTimeout;

    /**
     * Container cache size value.
     */
    @Inject
    @Named(CONTAINER_CACHE_SIZE)
    private String m_containerCacheSize;

    /**
     * Pattern String.
     */
    @Inject
    @Named(DATA_AFFINITY_PATTERN)
    private String m_dataAffinityPattern;

    /**
     * Number of connections.
     */
    @Inject
    @Named(NUMBER_REQUEST_CONCURRENCY)
    private int m_numberRequestConcurrency;

    /**
     * Size of Buffer.
     */
    @Inject
    @Named(MAX_WRITE_BUFFER_SIZE)
    private int m_maxWriteBufferSize;

    /**
     * Size of cache.
     */
    @Inject
    @Named(MAX_DATA_CACHE_SIZE)
    private int m_maxDataCacheSize;

    /**
     * Delay time.
     */
    @Inject
    @Named(WRITE_DELAY)
    private int m_writeDelay;

    /**
     * Delay time.
     */
    @Inject
    @Named(NUMBER_WRITE_BUFFER_DATAPOINT)
    private int m_numberWriteBufferDatapoint;

    /**
     * Get cluster name.
     *
     * Return cluster name defined in properties file to create connection with
     * GridDB
     *
     * @return cluster name
     */
    public final String getClusterName() {
        return m_clusterName;
    }

    /**
     * Empty constructor.
     */
    public GriddbConfiguration() {

    }

    /**
     * Constructor for essential components.
     *
     * @param clusterName
     *            name of cluster
     * @param notificationAddress
     *            address for connecting by multi-cast
     * @param notificationPort
     *            port for connecting by mukti-cast
     * @param user
     *            user name
     * @param password
     *            password
     * @param numberOfConcurrency
     *            number of connections at a time
     * @param bufferSize
     *            max size of buffer
     * @param cacheSize
     *            max size of cache
     * @param writeDelay
     *            time delay to write to DB
     */
    public GriddbConfiguration(final String clusterName,
            final String notificationAddress, final String notificationPort,
            final String user, final String password,
            final int numberOfConcurrency, final int bufferSize,
            final int cacheSize, final int writeDelay) {
        m_clusterName = clusterName;
        m_notificationAddress = notificationAddress;
        m_notificationPort = notificationPort;
        m_user = user;
        m_password = password;
        m_numberRequestConcurrency = numberOfConcurrency;
        m_maxWriteBufferSize = bufferSize;
        m_maxDataCacheSize = cacheSize;
        m_writeDelay = writeDelay;
    }

    /**
     * Get notification address.
     *
     * Return IP address to create connection with GridDB using multi-cast
     * method.
     *
     * @return notification address
     */
    public final String getNotificationAddress() {
        return m_notificationAddress;
    }

    /**
     * Get notification Port.
     *
     * Return Port to create connection with GridDB using multi-cast method.
     *
     * @return notification Port
     */
    public final String getNotificationPort() {
        return m_notificationPort;
    }

    /**
     * Get member list.
     *
     * Return list of cluster member IP address and port defined in properties
     * file. This list is used to create connection with GridDB using fixed list
     * method.
     *
     * @return list of members in cluster
     */
    public final String getNotificationMember() {
        return m_notificationMember;
    }

    /**
     * Get the URL of address provider.
     *
     * Return the URL of address list provider defined in properties file. This
     * list is used to create connection with GridDB using provider method.
     *
     * @return URL of address provider
     */
    public final String getNotificationProviderUrl() {
        return m_notificationProviderUrl;
    }

    /**
     * Get data point time to live defined in properties file.
     *
     * @return data point time to live
     */
    public final int getDatapointTtl() {
        return m_datapointTtl;
    }

    /**
     * Get user name defined in properties file for connection authentication.
     *
     * This value is mandatory
     *
     * @return user name
     */
    public final String getUser() {
        return m_user;
    }

    /**
     * Get user password defined in properties file for connection.
     * authentication
     *
     * This value is mandatory
     *
     * @return user password
     */
    public final String getPassword() {
        return m_password;
    }

    /**
     * Get time of client's updates commitment defined in properties file.
     *
     * This value is optional. If not specified, system default value is used.
     *
     * @return time to client's updates
     */
    public final String getConsistency() {
        return m_consistency;
    }

    /**
     * Get transaction timeout value defined in properties file.
     *
     * This value is optional. If not specified, system default value is used.
     *
     * @return minimum value for transaction timeout
     */
    public final String getTransactionTimeout() {
        return m_transactionTimeout;
    }

    /**
     * Get failover timeout value defined in properties file.
     *
     * This value is optional. If not specified, system default value is used.
     *
     * @return minimum waiting time for a failover
     */
    public final String getFailOverTimeout() {
        return m_failoverTimeout;
    }

    /**
     * Get Cache size for container.
     *
     * This value is set for GridDB in lower layer, not this module. This value
     * is optional. If not specified, system default value is used.
     *
     * @return maximum number of ContainerInfos on the Container Cache
     */
    public final String getContainerCacheSize() {
        return m_containerCacheSize;
    }

    /**
     * Get data affinity pattern defined in properties file.
     *
     * This value is optional. If not specified, system default value is used.
     *
     * @return data affinity pattern
     */
    public final String getDataAffinityPattern() {
        return m_dataAffinityPattern;
    }

    /**
     * Get the maximum number of connection.
     *
     * This value specifies the maximum number of connection between KairosDB
     * and GridDB created at a time.
     *
     * @return maximum number of connection between KairosDB and GridDB
     */
    public final int getNumberOfConcurrency() {
        return m_numberRequestConcurrency;
    }

    /**
     * Get buffer size defined in properties file.
     *
     * This buffer is used to support putting and deleting data request.
     *
     * @return maximum size for buffer
     */
    public final int getMaxWriteBufferSize() {
        return m_maxWriteBufferSize;
    }

    /**
     * Get Cache size value defined in properties file.
     *
     * This Cache is used by this module.
     *
     * @return maximum size of Data Cache used by this module
     */
    public final int getMaxDataCacheSize() {
        return m_maxDataCacheSize;
    }

    /**
     * Get delay time before putting to Database.
     *
     * @return delay time
     */
    public final int getWriteDelay() {
        return m_writeDelay;
    }

    /**
     * Number of write buffer use for put data points containers.
     *
     * @return
     */
    public final int getNumberDataPointWriteBuffer() {
        return m_numberWriteBufferDatapoint;
    }
}
