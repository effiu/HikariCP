package com.zaxxer.hikari.pool;

import static com.zaxxer.hikari.pool.ProxyConnection.DIRTY_BIT_AUTOCOMMIT;
import static com.zaxxer.hikari.pool.ProxyConnection.DIRTY_BIT_CATALOG;
import static com.zaxxer.hikari.pool.ProxyConnection.DIRTY_BIT_ISOLATION;
import static com.zaxxer.hikari.pool.ProxyConnection.DIRTY_BIT_NETTIMEOUT;
import static com.zaxxer.hikari.pool.ProxyConnection.DIRTY_BIT_READONLY;
import static com.zaxxer.hikari.util.UtilityElf.createInstance;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.metrics.MetricsTracker;
import com.zaxxer.hikari.util.ClockSource;
import com.zaxxer.hikari.util.DriverDataSource;
import com.zaxxer.hikari.util.PropertyElf;
import com.zaxxer.hikari.util.UtilityElf;
import com.zaxxer.hikari.util.UtilityElf.DefaultThreadFactory;

abstract class PoolBase {
   private final Logger LOGGER = LoggerFactory.getLogger(PoolBase.class);

   protected final HikariConfig config;
   protected final String poolName;
   protected long connectionTimeout;
   /**
    * 控制连接被用于测试是否可用的最大时间，必须小于连接的超时时间，最低可接受的时间是250ms，默认5000
    */
   protected long validationTimeout;

   private static final String[] RESET_STATES = {"readOnly", "autoCommit", "isolation", "catalog", "netTimeout"};
   private static final int UNINITIALIZED = -1;
   private static final int TRUE = 1;
   private static final int FALSE = 0;

   private int networkTimeout;
   /**
    * 内部值，记录Connection是否支持超时检查
    */
   private int isNetworkTimeoutSupported;
   /**
    * 内部值，记录Statement是否支持超时检查
    */
   private int isQueryTimeoutSupported;
   private int defaultTransactionIsolation;
   private int transactionIsolation;
   /**
    * 超时检查线程池
    */
   private Executor netTimeoutExecutor;
   private DataSource dataSource;

   private final String catalog;
   private final boolean isReadOnly;
   private final boolean isAutoCommit;

   /**
    * JDBC4标准的检查
    */
   private final boolean isUseJdbc4Validation;
   private final boolean isIsolateInternalQueries;
   /**
    * AtomicReference用于原子地更新对象的引用，提供单变量线程安全的原子更新操作
    * 内部保存的是newConnection抛出的异常
    */
   private final AtomicReference<Throwable> lastConnectionFailure;

   private volatile boolean isValidChecked;

   PoolBase(final HikariConfig config) {
      this.config = config;

      this.networkTimeout = UNINITIALIZED;
      this.catalog = config.getCatalog();
      this.isReadOnly = config.isReadOnly();
      this.isAutoCommit = config.isAutoCommit();
      this.transactionIsolation = UtilityElf.getTransactionIsolation(config.getTransactionIsolation());

      this.isQueryTimeoutSupported = UNINITIALIZED;
      this.isNetworkTimeoutSupported = UNINITIALIZED;
      this.isUseJdbc4Validation = config.getConnectionTestQuery() == null;
      this.isIsolateInternalQueries = config.isIsolateInternalQueries();

      this.poolName = config.getPoolName();
      this.connectionTimeout = config.getConnectionTimeout();
      this.validationTimeout = config.getValidationTimeout();
      this.lastConnectionFailure = new AtomicReference<>();

      initializeDataSource();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public String toString() {
      return poolName;
   }

   abstract void releaseConnection(final PoolEntry poolEntry);

   // ***********************************************************************
   //                           JDBC methods
   // ***********************************************************************

   /**
    * 译：悄悄或者偷偷的关闭连接，后台任务关闭连接
    * 调用场景:
    * 1. closeConnection时,后台任务销毁连接(连接超过最大空闲时间，被标记为evict等)
    * 2. 关闭线程池时，当指定销毁连接任务池失败后调用
    * 3. newConnection失败后，若连接不为null，则关闭连接
    *
    * @param connection
    * @param closureReason
    */
   void quietlyCloseConnection(final Connection connection, final String closureReason) {
      if (connection != null) {
         try {
            LOGGER.debug("{} - Closing connection {}: {}", poolName, connection, closureReason);
            try {
               setNetworkTimeout(connection, SECONDS.toMillis(15));
            } finally {
               connection.close(); // continue with the close even if setNetworkTimeout() throws
            }
         } catch (Throwable e) {
            LOGGER.debug("{} - Closing connection {} failed", poolName, connection, e);
         }
      }
   }

   /**
    * 连接检查，验证与数据库的连接是否正常
    * 调用场景: 当从连接池中获取连接时检查connection是否可用
    *
    * @param connection
    * @return
    */
   boolean isConnectionAlive(final Connection connection) {
      try {
         // JDBC4标准的连接检查
         if (isUseJdbc4Validation) {
            return connection.isValid((int) MILLISECONDS.toSeconds(Math.max(1000L, validationTimeout)));
         }
         // 设置本次连接检查的超时时间为validationTimeout,validationTimeout
         setNetworkTimeout(connection, validationTimeout);
         // 创建Statement执行连接检查
         try (Statement statement = connection.createStatement()) {
            if (isNetworkTimeoutSupported != TRUE) {
               // 当不支持超时检查时,设置Statement的超时检查
               setQueryTimeout(statement, (int) MILLISECONDS.toSeconds(Math.max(1000L, validationTimeout)));
            }
            // connectionTestQuery, 非JDBC4标准的检查连接的方式
            statement.execute(config.getConnectionTestQuery());
         }
         // 非只读事物，非自动提交
         if (isIsolateInternalQueries && !isReadOnly && !isAutoCommit) {
            connection.rollback();
         }
         // 设置networkTimeout
         setNetworkTimeout(connection, networkTimeout);

         return true;
      } catch (SQLException e) {
         lastConnectionFailure.set(e);
         LOGGER.warn("{} - Failed to validate connection {} ({})", poolName, connection, e.getMessage());
         return false;
      }
   }

   Throwable getLastConnectionFailure() {
      // 返回AtomicReference中的值，并设置为null
      return lastConnectionFailure.getAndSet(null);
   }

   public DataSource getUnwrappedDataSource() {
      return dataSource;
   }

   // ***********************************************************************
   //                         PoolEntry methods
   // ***********************************************************************

   /**
    * 新建一个 connection 包装类
    *
    * @return
    * @throws Exception
    */
   PoolEntry newPoolEntry() throws Exception {
      // connection、连接池、是否只读、是否自动提交
      return new PoolEntry(newConnection(), this, isReadOnly, isAutoCommit);
   }

   /**
    * TODO proxyConnection待阅读
    * 重置连接状态，dirtyBits不同位代表不同属性，readonly、autoCommit、isolation、catalog、netTimeout
    * @param connection      连接
    * @param proxyConnection 代理连接
    * @param dirtyBits       proxyConnection中的bit位
    * @throws SQLException
    */
   void resetConnectionState(final Connection connection, final ProxyConnection proxyConnection, final int dirtyBits) throws SQLException {
      int resetBits = 0;

      if ((dirtyBits & DIRTY_BIT_READONLY) != 0 && proxyConnection.getReadOnlyState() != isReadOnly) {
         connection.setReadOnly(isReadOnly);
         resetBits |= DIRTY_BIT_READONLY;
      }

      if ((dirtyBits & DIRTY_BIT_AUTOCOMMIT) != 0 && proxyConnection.getAutoCommitState() != isAutoCommit) {
         connection.setAutoCommit(isAutoCommit);
         resetBits |= DIRTY_BIT_AUTOCOMMIT;
      }

      if ((dirtyBits & DIRTY_BIT_ISOLATION) != 0 && proxyConnection.getTransactionIsolationState() != transactionIsolation) {
         connection.setTransactionIsolation(transactionIsolation);
         resetBits |= DIRTY_BIT_ISOLATION;
      }

      if ((dirtyBits & DIRTY_BIT_CATALOG) != 0 && catalog != null && !catalog.equals(proxyConnection.getCatalogState())) {
         connection.setCatalog(catalog);
         resetBits |= DIRTY_BIT_CATALOG;
      }

      if ((dirtyBits & DIRTY_BIT_NETTIMEOUT) != 0 && proxyConnection.getNetworkTimeoutState() != networkTimeout) {
         setNetworkTimeout(connection, networkTimeout);
         resetBits |= DIRTY_BIT_NETTIMEOUT;
      }

      if (resetBits != 0 && LOGGER.isDebugEnabled()) {
         LOGGER.debug("{} - Reset ({}) on connection {}", poolName, stringFromResetBits(resetBits), connection);
      }
   }

   /**
    * 关闭连接池时，关闭超时任务执行器
    */
   void shutdownNetworkTimeoutExecutor() {
      if (netTimeoutExecutor instanceof ThreadPoolExecutor) {
         // shutdown所有存活的任务，停止等待中的任务
         ((ThreadPoolExecutor) netTimeoutExecutor).shutdownNow();
      }
   }

   // ***********************************************************************
   //                       JMX methods
   // ***********************************************************************

   /**
    * 默认: false
    * Register MBeans for HikariConfig and HikariPool.
    *
    * @param hikariPool a HikariPool instance
    */
   void registerMBeans(final HikariPool hikariPool) {
      // 默认 registerMBeans 为 false
      if (!config.isRegisterMbeans()) {
         return;
      }

      try {
         // JMX(Java Management Extension, Java管理扩展)
         // JMX在Java编程语言中定义了应用程序以及网络管理和监控的体系结构、设计模式、应用程序接口以及服务。
         // 通常使用JMX来监控系统的运行状态或管理系统的某些方面，比如清空缓存、重新加载配置文件等
         final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

         final ObjectName beanConfigName = new ObjectName("com.zaxxer.hikari:type=PoolConfig (" + poolName + ")");
         final ObjectName beanPoolName = new ObjectName("com.zaxxer.hikari:type=Pool (" + poolName + ")");
         // 检查HikariConfig是否注册，将beanConfigName和beanPoolName注册到MBeanServer中
         if (!mBeanServer.isRegistered(beanConfigName)) {
            mBeanServer.registerMBean(config, beanConfigName);
            mBeanServer.registerMBean(hikariPool, beanPoolName);
         } else {
            LOGGER.error("{} - You cannot use the same pool name for separate pool instances.", poolName);
         }
      } catch (Exception e) {
         LOGGER.warn("{} - Failed to register management beans.", poolName, e);
      }
   }

   /**
    * Unregister MBeans for HikariConfig and HikariPool.
    */
   void unregisterMBeans() {
      if (!config.isRegisterMbeans()) {
         return;
      }

      try {
         final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

         final ObjectName beanConfigName = new ObjectName("com.zaxxer.hikari:type=PoolConfig (" + poolName + ")");
         final ObjectName beanPoolName = new ObjectName("com.zaxxer.hikari:type=Pool (" + poolName + ")");
         if (mBeanServer.isRegistered(beanConfigName)) {
            mBeanServer.unregisterMBean(beanConfigName);
            mBeanServer.unregisterMBean(beanPoolName);
         }
      } catch (Exception e) {
         LOGGER.warn("{} - Failed to unregister management beans.", poolName, e);
      }
   }

   // ***********************************************************************
   //                          Private methods
   // ***********************************************************************

   /**
    * 译:创建/初始化数底层据源
    * Create/initialize the underlying DataSource.
    *
    * @return a DataSource instance
    */
   private void initializeDataSource() {
      final String jdbcUrl = config.getJdbcUrl();
      final String username = config.getUsername();
      final String password = config.getPassword();
      final String dsClassName = config.getDataSourceClassName();
      final String driverClassName = config.getDriverClassName();
      final Properties dataSourceProperties = config.getDataSourceProperties();

      DataSource dataSource = config.getDataSource();

      // dataSourceClassName(dsClassName)以及jdbcUrl不需要同时配置,

      // 首先根据dataSourceClassName以及DataSource判断是否需要初始化dataSource
      if (dsClassName != null && dataSource == null) {
         dataSource = createInstance(dsClassName, DataSource.class);
         PropertyElf.setTargetFromProperties(dataSource, dataSourceProperties);
      }
      // 若dataSource为null且datasourceClassName为空时,使用jdbcUrl的方式初始化dataSource
      else if (jdbcUrl != null && dataSource == null) {
         dataSource = new DriverDataSource(jdbcUrl, driverClassName, dataSourceProperties, username, password);
      }

      if (dataSource != null) {
         setLoginTimeout(dataSource, connectionTimeout);
         createNetworkTimeoutExecutor(dataSource, dsClassName, jdbcUrl);
      }

      this.dataSource = dataSource;
   }

   /**
    * 创建数据库连接
    *
    * @return
    * @throws Exception
    */
   Connection newConnection() throws Exception {
      Connection connection = null;
      try {
         String username = config.getUsername();
         String password = config.getPassword();

         connection = (username == null) ? dataSource.getConnection() : dataSource.getConnection(username, password);
         // 设置一些初始值或者状态，例如超时时间、自动提交、事物隔离级别、连接检查等
         setupConnection(connection);
         lastConnectionFailure.set(null);
         return connection;
      } catch (Exception e) {
         lastConnectionFailure.set(e);
         quietlyCloseConnection(connection, "(Failed to create/set connection)");
         throw e;
      }
   }

   /**
    * 译:设置连接的初始状态,包括网络超时时间、驱动版本、连接的一些属性等等
    * Setup a connection initial state.
    *
    * @param connection a Connection
    * @throws SQLException thrown from driver
    */
   private void setupConnection(final Connection connection) throws SQLException {
      if (networkTimeout == UNINITIALIZED) {
         // validationTimeout控制测试连接活跃最长时间，必须小于connectionTimeout,最小值为250ms，默认5000ms
         networkTimeout = getAndSetNetworkTimeout(connection, validationTimeout);
      } else {
         setNetworkTimeout(connection, validationTimeout);
      }
      // 驱动检查，连接检查方式
      checkDriverSupport(connection);
      // 设置只读与事物自动提交
      connection.setReadOnly(isReadOnly);
      connection.setAutoCommit(isAutoCommit);
      // 设置默认的事物隔离
      if (transactionIsolation != defaultTransactionIsolation) {
         connection.setTransactionIsolation(transactionIsolation);
      }
      // catalog:为支持catalog概念的db设置默认的catalog
      // MySQL、Oracle都不支持catalog，支持的有DB2、MS SQL Server
      if (catalog != null) {
         connection.setCatalog(catalog);
      }
      // connectionInitSql是创建连接后执行的SQL，用于判断连接是否成功，默认为空
      executeSql(connection, config.getConnectionInitSql(), true);

      setNetworkTimeout(connection, networkTimeout);
   }

   /**
    * 执行isValid()或者连接检查
    * Execute isValid() or connection test query.
    *
    * @param connection a Connection to check
    */
   private void checkDriverSupport(final Connection connection) throws SQLException {
      if (!isValidChecked) {
         // JDBC4支持isValid()
         if (isUseJdbc4Validation) {
            try {
               connection.isValid(1);
            } catch (Throwable e) {
               LOGGER.error("{} - Failed to execute isValid() for connection, configure connection test query. ({})", poolName, e.getMessage());
               throw e;
            }
         } else {
            // 旧版JDBC，则根据connectionTestQuery进行连接检查
            try {
               executeSql(connection, config.getConnectionTestQuery(), false);
            } catch (Throwable e) {
               LOGGER.error("{} - Failed to execute connection test query. ({})", poolName, e.getMessage());
               throw e;
            }
         }
         // 获取默认的事物隔离级别
         defaultTransactionIsolation = connection.getTransactionIsolation();
         // 初始化连接池隔离界别为默认的事物隔离级别
         if (transactionIsolation == -1) {
            transactionIsolation = defaultTransactionIsolation;
         }

         isValidChecked = true;
      }
   }

   /**
    * 译：设置Statement的超时时间，若当前驱动支持;当驱动非JDBC4且isNetworkTimeoutSupported为false时
    * Set the query timeout, if it is supported by the driver.
    *
    * @param statement  a statement to set the query timeout on
    * @param timeoutSec the number of seconds before timeout
    */
   private void setQueryTimeout(final Statement statement, final int timeoutSec) {
      if (isQueryTimeoutSupported != FALSE) {
         try {
            statement.setQueryTimeout(timeoutSec);
            isQueryTimeoutSupported = TRUE;
         } catch (Throwable e) {
            if (isQueryTimeoutSupported == UNINITIALIZED) {
               isQueryTimeoutSupported = FALSE;
               LOGGER.warn("{} - Failed to set query timeout for statement. ({})", poolName, e.getMessage());
            }
         }
      }
   }

   /**
    * 译：设置network timeout超时时间,如果isUseNetworkTimeout为true且驱动支持该属性，返回之前的network timeout值
    * Set the network timeout, if <code>isUseNetworkTimeout</code> is <code>true</code> and the
    * driver supports it.  Return the pre-existing value of the network timeout.
    *
    * @param connection the connection to set the network timeout on
    * @param timeoutMs  the number of milliseconds before timeout
    * @return the pre-existing network timeout value
    */
   private int getAndSetNetworkTimeout(final Connection connection, final long timeoutMs) {
      // 在setupConnection中若
      if (isNetworkTimeoutSupported != FALSE) {
         try {
            // network timeout原值
            final int originalTimeout = connection.getNetworkTimeout();
            // 设置为新值，并设置超时检查异步任务
            connection.setNetworkTimeout(netTimeoutExecutor, (int) timeoutMs);
            isNetworkTimeoutSupported = TRUE;
            return originalTimeout;
         } catch (Throwable e) {
            // 未定义isNetworkTimeoutSupported的情况
            if (isNetworkTimeoutSupported == UNINITIALIZED) {
               isNetworkTimeoutSupported = FALSE;

               LOGGER.warn("{} - Failed to get/set network timeout for connection. ({})", poolName, e.getMessage());
               if (validationTimeout < SECONDS.toMillis(1)) {
                  LOGGER.warn("{} - A validationTimeout of less than 1 second cannot be honored on drivers without setNetworkTimeout() support.", poolName);
               } else if (validationTimeout % SECONDS.toMillis(1) != 0) {
                  LOGGER.warn("{} - A validationTimeout with fractional second granularity cannot be honored on drivers without setNetworkTimeout() support.", poolName);
               }
            }
         }
      }

      return 0;
   }

   /**
    * 译：设置网络超时时间，
    * 调用场景:(与数据库交互时)
    * 1. quietlyCloseConnection
    * 2. isConnectionAlive,当调用getConnection()时判处连接是否可用
    *    2.1 当非JDBC4时，判断连接是否可用时设置超时时间为validationTimeout
    *    2.2 当非JDBC4时，连接检查后设置连接的超时时间为networkTimeout
    * 3.resetConnectionState, 重置连接状态时
    * 4.setupConnection创建连接时
    * 5.setupConnection初始化连接时
    *    5.1 连接检查
    *    5.2 设置连接超时时间
    * <p>
    * Set the network timeout, if <code>isUseNetworkTimeout</code> is <code>true</code> and the
    * driver supports it.
    *
    * @param connection the connection to set the network timeout on
    * @param timeoutMs  the number of milliseconds before timeout
    * @throws SQLException throw if the connection.setNetworkTimeout() call throws
    */
   private void setNetworkTimeout(final Connection connection, final long timeoutMs) throws SQLException {
      // 只有支持网络超时
      if (isNetworkTimeoutSupported == TRUE) {
         connection.setNetworkTimeout(netTimeoutExecutor, (int) timeoutMs);
      }
   }

   /**
    * 译:执行用户定义的初始化连接的SQL
    * 1. setupConnection#setupConnection检查驱动时,非jdbc4标准使用connectionTestQuerySQL执行检查
    * 2. setupConnection后，执行配置的connectionInitSql
    *
    * Execute the user-specified init SQL.
    *
    * @param connection the connection to initialize
    * @param sql        the SQL to execute
    * @param isCommit   whether to commit the SQL after execution or not
    * @throws SQLException throws if the init SQL execution fails
    */
   private void executeSql(final Connection connection, final String sql, final boolean isCommit) throws SQLException {
      if (sql != null) {
         try (Statement statement = connection.createStatement()) {
            // connection was created a few milliseconds before, so set query timeout is omitted (we assume it will succeed)
            statement.execute(sql);
         }
         // 根据是否只读、自动提交判断
         if (isIsolateInternalQueries && !isReadOnly && !isAutoCommit) {
            if (isCommit) {
               connection.commit();
            } else {
               connection.rollback();
            }
         }
      }
   }

   /**
    * 创建网络超时检查执行器
    *
    * @param dataSource
    * @param dsClassName
    * @param jdbcUrl
    */
   private void createNetworkTimeoutExecutor(final DataSource dataSource, final String dsClassName, final String jdbcUrl) {
      // Temporary hack for MySQL issue: http://bugs.mysql.com/bug.php?id=75615
      if ((dsClassName != null && dsClassName.contains("Mysql")) ||
         (jdbcUrl != null && jdbcUrl.contains("mysql")) ||
         (dataSource != null && dataSource.getClass().getName().contains("Mysql"))) {
         // 当数据库是MySQL时处理该问题
         netTimeoutExecutor = new SynchronousExecutor();
      } else {
         ThreadFactory threadFactory = config.getThreadFactory();
         threadFactory = threadFactory != null ? threadFactory : new DefaultThreadFactory(poolName + " network timeout executor", true);
         ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool(threadFactory);
         executor.setKeepAliveTime(15, SECONDS);
         executor.allowCoreThreadTimeOut(true);
         netTimeoutExecutor = executor;
      }
   }

   /**
    * 译: 设置loginTimeout为connectionTimeout，最小为1000
    * Set the loginTimeout on the specified DataSource.
    *
    * @param dataSource        the DataSource
    * @param connectionTimeout the timeout in milliseconds
    */
   private void setLoginTimeout(final DataSource dataSource, final long connectionTimeout) {
      if (connectionTimeout != Integer.MAX_VALUE) {
         try {
            // 最小值为1000
            dataSource.setLoginTimeout((int) MILLISECONDS.toSeconds(Math.max(1000L, connectionTimeout)));
         } catch (Throwable e) {
            LOGGER.warn("{} - Failed to set login timeout for data source. ({})", poolName, e.getMessage());
         }
      }
   }

   /**
    * 译: 该方法创建一个字符串,提供一套"reset bit". 返回一个可拼接的字符串。不同bit位有不同的意义
    * This will create a string for debug logging. Given a set of "reset bits", this
    * method will return a concatenated string, for example:
    * <p>
    * Input : 0b00110
    * Output: "autoCommit, isolation"
    *
    * @param bits a set of "reset bits"
    * @return a string of which states were reset
    */
   private String stringFromResetBits(final int bits) {
      final StringBuilder sb = new StringBuilder();
      for (int ndx = 0; ndx < RESET_STATES.length; ndx++) {
         if ((bits & (0b1 << ndx)) != 0) {
            sb.append(RESET_STATES[ndx]).append(", ");
         }
      }

      sb.setLength(sb.length() - 2);  // trim trailing comma
      return sb.toString();
   }

   // ***********************************************************************
   //                      Private Static Classes
   // ***********************************************************************

   /**
    * 译: 这是一个特殊的执行器，仅仅围绕一个MySQL尚未被解决的MySQL问题
    * Special executor used only to work around a MySQL issue that has not been addressed.
    * MySQL issue: http://bugs.mysql.com/bug.php?id=75615
    * TODO 仔细分析该问题
    */
   private static class SynchronousExecutor implements Executor {
      /**
       * {@inheritDoc}
       */
      @Override
      public void execute(Runnable command) {
         try {
            command.run();
         } catch (Throwable t) {
            LoggerFactory.getLogger(PoolBase.class).debug("Failed to execute: {}", command, t);
         }
      }
   }

   /**
    * A class that delegates to a MetricsTracker implementation.  The use of a delegate
    * allows us to use the NopMetricsTrackerDelegate when metrics are disabled, which in
    * turn allows the JIT to completely optimize away to callsites to record metrics.
    */
   static class MetricsTrackerDelegate implements AutoCloseable {
      final MetricsTracker tracker;

      protected MetricsTrackerDelegate() {
         this.tracker = null;
      }

      MetricsTrackerDelegate(MetricsTracker tracker) {
         this.tracker = tracker;
      }

      @Override
      public void close() {
         tracker.close();
      }

      void recordConnectionUsage(final PoolEntry poolEntry) {
         tracker.recordConnectionUsageMillis(poolEntry.getMillisSinceBorrowed());
      }

      /**
       * @param poolEntry
       * @param startTime
       */
      void recordBorrowStats(final PoolEntry poolEntry, final long startTime) {
         final long now = ClockSource.INSTANCE.currentTime();
         poolEntry.lastBorrowed = now;
         tracker.recordConnectionAcquiredNanos(ClockSource.INSTANCE.elapsedNanos(startTime, now));
      }

      void recordConnectionTimeout() {
         tracker.recordConnectionTimeout();
      }
   }

   /**
    * A no-op implementation of the MetricsTrackerDelegate that is used when metrics capture is
    * disabled.
    */
   static final class NopMetricsTrackerDelegate extends MetricsTrackerDelegate {
      @Override
      void recordConnectionUsage(final PoolEntry poolEntry) {
         // no-op
      }

      @Override
      public void close() {
         // no-op
      }

      @Override
      void recordBorrowStats(final PoolEntry poolEntry, final long startTime) {
         // no-op
      }

      @Override
      void recordConnectionTimeout() {
         // no-op
      }
   }
}
