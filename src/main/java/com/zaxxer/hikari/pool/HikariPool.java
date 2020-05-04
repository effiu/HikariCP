/*
 * Copyright (C) 2013,2014 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zaxxer.hikari.pool;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariPoolMXBean;
import com.zaxxer.hikari.metrics.MetricsTrackerFactory;
import com.zaxxer.hikari.metrics.PoolStats;
import com.zaxxer.hikari.metrics.dropwizard.CodahaleHealthChecker;
import com.zaxxer.hikari.metrics.dropwizard.CodahaleMetricsTrackerFactory;
import com.zaxxer.hikari.util.ClockSource;
import com.zaxxer.hikari.util.ConcurrentBag;
import com.zaxxer.hikari.util.ConcurrentBag.IBagStateListener;
import com.zaxxer.hikari.util.UtilityElf.DefaultThreadFactory;
import com.zaxxer.hikari.util.SuspendResumeLock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import static com.zaxxer.hikari.pool.PoolEntry.LAST_ACCESS_COMPARABLE;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.STATE_IN_USE;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.STATE_NOT_IN_USE;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.STATE_REMOVED;
import static com.zaxxer.hikari.util.UtilityElf.createThreadPoolExecutor;
import static com.zaxxer.hikari.util.UtilityElf.quietlySleep;

/**
 * 译：这是一个提供了HikariCP基本池行为的连接池类
 * This is the primary connection pool class that provides the basic
 * pooling behavior for HikariCP.
 *
 * @author Brett Wooldridge
 */
public class HikariPool extends PoolBase implements HikariPoolMXBean, IBagStateListener
{
   private final Logger LOGGER = LoggerFactory.getLogger(HikariPool.class);

   private static final ClockSource clockSource = ClockSource.INSTANCE;

   private static final int POOL_NORMAL = 0;
   private static final int POOL_SUSPENDED = 1;
   private static final int POOL_SHUTDOWN = 2;

   private volatile int poolState;

   private final long ALIVE_BYPASS_WINDOW_MS = Long.getLong("com.zaxxer.hikari.aliveBypassWindowMs", MILLISECONDS.toMillis(500));
   private final long HOUSEKEEPING_PERIOD_MS = Long.getLong("com.zaxxer.hikari.housekeeping.periodMs", SECONDS.toMillis(30));

   private final PoolEntryCreator POOL_ENTRY_CREATOR = new PoolEntryCreator();
   /**
    * 记录当前可使用连接总数
    */
   private final AtomicInteger totalConnections;

   /**
    * PoolEntry创建任务
    */
   private final ThreadPoolExecutor addConnectionExecutor;

   /**
    * 关闭连接池内连接的Executor
    */
   private final ThreadPoolExecutor closeConnectionExecutor;

   /**
    * 用于维护连接池内的空闲连接，超过minimumIdle部分将会被销毁
    */
   private final ScheduledThreadPoolExecutor houseKeepingExecutorService;

   private final ConcurrentBag<PoolEntry> connectionBag;

    /**
    * 连接泄漏任务检测
    */
   private final ProxyLeakTask leakTask;

   /**
    * 对信号量{@link java.util.concurrent.Semaphore}的封装
    * 当信号量许可证已满时，获取连接等操作会被阻塞
    */
   private final SuspendResumeLock suspendResumeLock;

   private MetricsTrackerDelegate metricsTracker;

   /**
    * 译:使用个性化配置构造连接池
    * Construct a HikariPool with the specified configuration.
    *
    * @param config a HikariConfig instance
    */
   public HikariPool(final HikariConfig config)
   {
      // 调用PoolBase
      super(config);

      this.connectionBag = new ConcurrentBag<>(this);
      this.totalConnections = new AtomicInteger();
      // 可以通过配置更新，getConnection()方法是否会阻塞
      this.suspendResumeLock = config.isAllowPoolSuspension() ? new SuspendResumeLock() : SuspendResumeLock.FAUX_LOCK;

      // 默认无
      if (config.getMetricsTrackerFactory() != null) {
         setMetricsTrackerFactory(config.getMetricsTrackerFactory());
      }
      else {
         setMetricRegistry(config.getMetricRegistry());
      }
      // 仅仅在通过编程配置或者IoC容器使用。默认无
      setHealthCheckRegistry(config.getHealthCheckRegistry());
      // MBeans是JMX的核心,很少使用默认是false
      registerMBeans(this);
      // 尝试创建一个连接并关闭。创建连接池时检查是否可以建立连接
      checkFailFast();

      ThreadFactory threadFactory = config.getThreadFactory();
      /**
       * 创建数据库连接任务,maximumPoolSize为阻塞队列大小、拒绝策略为丢弃
       */
      this.addConnectionExecutor = createThreadPoolExecutor(config.getMaximumPoolSize(), poolName + " connection adder", threadFactory, new ThreadPoolExecutor.DiscardPolicy());
      /**
       * 关闭连接任务
       */
      this.closeConnectionExecutor = createThreadPoolExecutor(config.getMaximumPoolSize(), poolName + " connection closer", threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());

      if (config.getScheduledExecutorService() == null) {
         threadFactory = threadFactory != null ? threadFactory : new DefaultThreadFactory(poolName + " housekeeper", true);
         this.houseKeepingExecutorService = new ScheduledThreadPoolExecutor(1, threadFactory, new ThreadPoolExecutor.DiscardPolicy());
         // 当houseKeepingExecutorService的shutdown方法被调用时，不再执行线程池内的任务
         this.houseKeepingExecutorService.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
         // 取消任务时立即从任务池中移除任务
         this.houseKeepingExecutorService.setRemoveOnCancelPolicy(true);
      }
      else {
         this.houseKeepingExecutorService = config.getScheduledExecutorService();
      }
      // 设置延时任务池的延时时间以及执行的command
      this.houseKeepingExecutorService.scheduleWithFixedDelay(new HouseKeeper(), 0L, HOUSEKEEPING_PERIOD_MS, MILLISECONDS);
      // 创建防止连接泄漏的任务
      this.leakTask = new ProxyLeakTask(config.getLeakDetectionThreshold(), houseKeepingExecutorService);
}

   /**
    * Get a connection from the pool, or timeout after connectionTimeout milliseconds.
    *
    * @return a java.sql.Connection instance
    * @throws SQLException thrown if a timeout occurs trying to obtain a connection
    */
   public final Connection getConnection() throws SQLException
   {
      return getConnection(connectionTimeout);
   }

   /**
    * 译: 从连接池中获取一个连接，或者超过指定时间后抛出异常
    * Get a connection from the pool, or timeout after the specified number of milliseconds.
    *
    * @param hardTimeout the maximum time to wait for a connection from the pool
    * @return a java.sql.Connection instance
    * @throws SQLException thrown if a timeout occurs trying to obtain a connection
    */
   public final Connection getConnection(final long hardTimeout) throws SQLException
   {
      // 获取一个许可证，当达到最大时会阻塞
      suspendResumeLock.acquire();
      final long startTime = clockSource.currentTime();

      try {
         long timeout = hardTimeout;
         do {
            final PoolEntry poolEntry = connectionBag.borrow(timeout, MILLISECONDS);
            if (poolEntry == null) {
               break; // We timed out... break and throw exception
            }

            final long now = clockSource.currentTime();
            // 判断借到的连接是否可用
            if (poolEntry.isMarkedEvicted() || (clockSource.elapsedMillis(poolEntry.lastAccessed, now) > ALIVE_BYPASS_WINDOW_MS && !isConnectionAlive(poolEntry.connection))) {
               closeConnection(poolEntry, "(connection is evicted or dead)"); // Throw away the dead connection (passed max age or failed alive test)
               timeout = hardTimeout - clockSource.elapsedMillis(startTime);
            }
            else {
               metricsTracker.recordBorrowStats(poolEntry, startTime);
               return poolEntry.createProxyConnection(leakTask.schedule(poolEntry), now);
            }
            // timeout = hardTimeout - 当前时间戳，即超过了入参规定的时间
         } while (timeout > 0L);
      }
      catch (InterruptedException e) {
         throw new SQLException(poolName + " - Interrupted during connection acquisition", e);
      }
      finally {
         suspendResumeLock.release();
      }

      logPoolState("Timeout failure ");
      metricsTracker.recordConnectionTimeout();

      String sqlState = null;
      // 异常处理
      final Throwable originalException = getLastConnectionFailure();
      if (originalException instanceof SQLException) {
         sqlState = ((SQLException) originalException).getSQLState();
      }
      final SQLException connectionException = new SQLTransientConnectionException(poolName + " - Connection is not available, request timed out after " + clockSource.elapsedMillis(startTime) + "ms.", sqlState, originalException);
      if (originalException instanceof SQLException) {
         connectionException.setNextException((SQLException) originalException);
      }
      throw connectionException;
   }

   /**
    * 译：关闭连接池、关闭所有空闲连接，且终止或者关闭活动链接，关闭创建的所有线程池执行器等
    * Shutdown the pool, closing all idle connections and aborting or closing
    * active connections.
    *
    * @throws InterruptedException thrown if the thread is interrupted during shutdown
    */
   public final synchronized void shutdown() throws InterruptedException
   {
      try {
         // 连接池状态
         poolState = POOL_SHUTDOWN;

         LOGGER.info("{} - Close initiated...", poolName);
         logPoolState("Before closing ");
         // 标记池内所有连接
         softEvictConnections();

         // 关闭创建连接执行器，逐个关闭线程池内的任务
         addConnectionExecutor.shutdown();
         // 关闭线程池内锁中的等待队列中的任务
         addConnectionExecutor.awaitTermination(5L, SECONDS);
         // 关闭维护minimumIdleSize的线程池执行器以及任务
         if (config.getScheduledExecutorService() == null && houseKeepingExecutorService != null) {
            houseKeepingExecutorService.shutdown();
            houseKeepingExecutorService.awaitTermination(5L, SECONDS);
         }

         connectionBag.close();
         // 杀死连接执行器
         final ExecutorService assassinExecutor = createThreadPoolExecutor(config.getMaximumPoolSize(), poolName + " connection assassinator",
                                                                           config.getThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());
         try {
            final long start = clockSource.currentTime();
            do {
               abortActiveConnections(assassinExecutor);
               // 标记连接为被销毁状态
               softEvictConnections();
            } while (getTotalConnections() > 0 && clockSource.elapsedMillis(start) < SECONDS.toMillis(5));
         }
         finally {
            assassinExecutor.shutdown();
            assassinExecutor.awaitTermination(5L, SECONDS);
         }

         shutdownNetworkTimeoutExecutor();
         closeConnectionExecutor.shutdown();
         closeConnectionExecutor.awaitTermination(5L, SECONDS);
      }
      finally {
         logPoolState("After closing ");
         unregisterMBeans();
         metricsTracker.close();
         LOGGER.info("{} - Closed.", poolName);
      }
   }

   /**
    * Evict a connection from the pool.
    *
    * @param connection the connection to evict
    */
   public final void evictConnection(Connection connection)
   {
      ProxyConnection proxyConnection = (ProxyConnection) connection;
      proxyConnection.cancelLeakTask();
      // 标记连接
      softEvictConnection(proxyConnection.getPoolEntry(), "(connection evicted by user)", true /* owner */);
   }

   public void setMetricRegistry(Object metricRegistry)
   {
      if (metricRegistry != null) {
         setMetricsTrackerFactory(new CodahaleMetricsTrackerFactory((MetricRegistry) metricRegistry));
      }
      else {
         setMetricsTrackerFactory(null);
      }
   }

   public void setMetricsTrackerFactory(MetricsTrackerFactory metricsTrackerFactory)
   {
      if (metricsTrackerFactory != null) {
         this.metricsTracker = new MetricsTrackerDelegate(metricsTrackerFactory.create(config.getPoolName(), getPoolStats()));
      }
      else {
         this.metricsTracker = new NopMetricsTrackerDelegate();
      }
   }

   public void setHealthCheckRegistry(Object healthCheckRegistry)
   {
      if (healthCheckRegistry != null) {
         CodahaleHealthChecker.registerHealthChecks(this, config, (HealthCheckRegistry) healthCheckRegistry);
      }
   }

   // ***********************************************************************
   //                        IBagStateListener callback
   // ***********************************************************************

   /** {@inheritDoc} */
   @Override
   public Future<Boolean> addBagItem()
   {
      // 提交一个创建PoolEntry的任务
      return addConnectionExecutor.submit(POOL_ENTRY_CREATOR);
   }

   // ***********************************************************************
   //                        HikariPoolMBean methods
   // ***********************************************************************

   /** {@inheritDoc} */
   @Override
   public final int getActiveConnections()
   {
      return connectionBag.getCount(STATE_IN_USE);
   }

   /** {@inheritDoc} */
   @Override
   public final int getIdleConnections()
   {
      return connectionBag.getCount(STATE_NOT_IN_USE);
   }

   /** {@inheritDoc} */
   @Override
   public final int getTotalConnections()
   {
      return connectionBag.size() - connectionBag.getCount(STATE_REMOVED);
   }

   /** {@inheritDoc} */
   @Override
   public final int getThreadsAwaitingConnection()
   {
      return connectionBag.getPendingQueue();
   }

   /** {@inheritDoc} */
   @Override
   public void softEvictConnections()
   {
      for (PoolEntry poolEntry : connectionBag.values()) {
         softEvictConnection(poolEntry, "(connection evicted)", false /* not owner */);
      }
   }

   /** {@inheritDoc} */
   @Override
   public final synchronized void suspendPool()
   {
      if (suspendResumeLock == SuspendResumeLock.FAUX_LOCK) {
         throw new IllegalStateException(poolName + " - is not suspendable");
      }
      else if (poolState != POOL_SUSPENDED) {
         // 线程池需要挂起，获取全部许可证
         suspendResumeLock.suspend();
         poolState = POOL_SUSPENDED;
      }
   }

   /** {@inheritDoc} */
   @Override
   public final synchronized void resumePool()
   {
      if (poolState == POOL_SUSPENDED) {
         poolState = POOL_NORMAL;
         fillPool();
         // 释放全部信号许可证
         suspendResumeLock.resume();
      }
   }

   // ***********************************************************************
   //                           Package methods
   // ***********************************************************************

   /**
    * debug级别记录当前线程池状态
    * Log the current pool state at debug level.
    *
    * @param prefix an optional prefix to prepend the log message
    */
   final void logPoolState(String... prefix)
   {
      if (LOGGER.isDebugEnabled()) {
         LOGGER.debug("{} - {}stats (total={}, active={}, idle={}, waiting={})",
                      poolName, (prefix.length > 0 ? prefix[0] : ""),
                      getTotalConnections(), getActiveConnections(), getIdleConnections(), getThreadsAwaitingConnection());
      }
   }

   /**
    * 译：释放一个连接到连接池中，或者永久关闭连接
    * Release a connection back to the pool, or permanently close it if it is broken.
    *
    * @param poolEntry the PoolBagEntry to release back to the pool
    */
   @Override
   final void releaseConnection(final PoolEntry poolEntry)
   {
      // 记录连接用途，本质记录的是时间
      metricsTracker.recordConnectionUsage(poolEntry);
      //
      connectionBag.requite(poolEntry);
   }

   /**
    * 译：永久关闭指定连接(吞掉任何异常)
    * 以下几种情况会调用closeConnection方法
    *    1. 连接被标记为evict
    *    2. 连接超过最大可空闲时间
    *    3. 连接被借用后发现不可用
    *    4. evict方法
    * Permanently close the real (underlying) connection (eat any exception).
    *
    * @param poolEntry poolEntry having the connection to close
    * @param closureReason reason to close
    */
   final void closeConnection(final PoolEntry poolEntry, final String closureReason)
   {
      if (connectionBag.remove(poolEntry)) {
         final int tc = totalConnections.decrementAndGet();
         if (tc < 0) {
            LOGGER.warn("{} - Unexpected value of totalConnections={}", poolName, tc, new Exception());
         }
         final Connection connection = poolEntry.close();
         // 关闭connection的任务交给另外的线程
         closeConnectionExecutor.execute(new Runnable() {
            @Override
            public void run() {
               quietlyCloseConnection(connection, closureReason);
            }
         });
      }
   }

   // ***********************************************************************
   //                           Private methods
   // ***********************************************************************

   /**
    * Create and add a single connection to the pool.
    */
   private PoolEntry createPoolEntry()
   {
      try {
         // 创建poolEntry以及初始化connection
         final PoolEntry poolEntry = newPoolEntry();
         // 获取配置的最大生存周期，默认30m
         final long maxLifetime = config.getMaxLifetime();
         if (maxLifetime > 0) {
            // variance up to 2.5% of the maxlifetime，方差为最大生存时间的2.5%
            final long variance = maxLifetime > 10_000 ? ThreadLocalRandom.current().nextLong( maxLifetime / 40 ) : 0;
            // 实际生存时间
            final long lifetime = maxLifetime - variance;
            // 设置强制销毁时间，即最大空闲时间
            poolEntry.setFutureEol(houseKeepingExecutorService.schedule(new Runnable() {
               @Override
               public void run() {
                  softEvictConnection(poolEntry, "(connection has passed maxLifetime)", false /* not owner */);
               }
            }, lifetime, MILLISECONDS));
         }

         LOGGER.debug("{} - Added connection {}", poolName, poolEntry.connection);
         return poolEntry;
      }
      catch (Exception e) {
         if (poolState == POOL_NORMAL) {
            LOGGER.debug("{} - Cannot acquire connection from data source", poolName, e);
         }
         return null;
      }
   }

   /**
    * 填充连接池内连接到最小连接数
    * Fill pool up from current idle connections (as they are perceived at the point of execution) to minimumIdle connections.
    */
   private void fillPool()
   {
      // 取值: 最大连接与idle连接减去当前存活数较小的 - 任务池中的任务数为 fill数
      final int connectionsToAdd = Math.min(config.getMaximumPoolSize() - totalConnections.get(), config.getMinimumIdle() - getIdleConnections())
                                   - addConnectionExecutor.getQueue().size();
      // 提交创建PoolEntry任务
      for (int i = 0; i < connectionsToAdd; i++) {
         addBagItem();
      }
      // 若debug日志级别，则当创建PoolEntry任务完成后，打印"After adding"日志
      if (connectionsToAdd > 0 && LOGGER.isDebugEnabled()) {
         addConnectionExecutor.execute(new Runnable() {
            @Override
            public void run() {
               logPoolState("After adding ");
            }
         });
      }
   }

   /**
    * 译：尝试中止或者关闭存活的连接
    * 调用场景: 当关闭线程池时，指定关闭连接的任务池
    * Attempt to abort() active connections, or close() them.
    */
   private void abortActiveConnections(final ExecutorService assassinExecutor)
   {
      for (PoolEntry poolEntry : connectionBag.values(STATE_IN_USE)) {
         try {
            poolEntry.connection.abort(assassinExecutor);
         }
         catch (Throwable e) {
            quietlyCloseConnection(poolEntry.connection, "(connection aborted during shutdown)");
         }
         finally {
            // 关闭connection后关闭poolEntry
            poolEntry.close();
            if (connectionBag.remove(poolEntry)) {
               totalConnections.decrementAndGet();
            }
         }
      }
   }

   /**
    * 译：将池填充到最小大小
    * Fill the pool up to the minimum size.
    */
   private void checkFailFast()
   {
      if (config.isInitializationFailFast()) {
         try {
            // 尝试创建一个连接，并关闭
            newConnection().close();
         }
         catch (Throwable e) {
            try {
               shutdown();
            }
            catch (Throwable ex) {
               e.addSuppressed(ex);
            }

            throw new PoolInitializationException(e);
         }
      }
   }

   /**
    * 达到最大生存时间之后，关闭连接或者标记为被驱逐或者销毁
    * @param poolEntry 线程连接
    * @param reason
    * @param owner
    */
   private void softEvictConnection(final PoolEntry poolEntry, final String reason, final boolean owner)
   {
      if (owner || connectionBag.reserve(poolEntry)) {
         closeConnection(poolEntry, reason);
      }
      else {
         poolEntry.markEvicted();
      }
   }

   private PoolStats getPoolStats()
   {
      return new PoolStats(SECONDS.toMillis(1)) {
         @Override
         protected void update() {
            this.pendingThreads = HikariPool.this.getThreadsAwaitingConnection();
            this.idleConnections = HikariPool.this.getIdleConnections();
            this.totalConnections = HikariPool.this.getTotalConnections();
            this.activeConnections = HikariPool.this.getActiveConnections();
         }
      };
   }

   // ***********************************************************************
   //                      Non-anonymous Inner-classes
   // ***********************************************************************

   /**
    * poolEntry创建器
    */
   private class PoolEntryCreator implements Callable<Boolean>
   {
      @Override
      public Boolean call() throws Exception
      {
         long sleepBackoff = 250L;
         // 当池状态正常、且当前池内连接小于最大连接数时
         while (poolState == POOL_NORMAL && totalConnections.get() < config.getMaximumPoolSize()) {
            // 创建poolEntry
            final PoolEntry poolEntry = createPoolEntry();
            // 创建成功后，totalConnection+1、添加到connectionBag中
            if (poolEntry != null) {
               totalConnections.incrementAndGet();
               connectionBag.add(poolEntry);
               return Boolean.TRUE;
            }

            // failed to get connection from db, sleep and retry
            // 若创建失败，则需要sleep和重试
            quietlySleep(sleepBackoff);
            sleepBackoff = Math.min(SECONDS.toMillis(10), Math.min(connectionTimeout, (long) (sleepBackoff * 1.5)));
         }
         // Pool is suspended or shutdown or at max size
         return Boolean.FALSE;
      }
   }

   /**
    * 译：用于清理空闲连接(超过minimumIdle部分)
    * The house keeping task to retire idle connections.
    */
   private class HouseKeeper implements Runnable
   {
      private volatile long previous = clockSource.plusMillis(clockSource.currentTime(), -HOUSEKEEPING_PERIOD_MS);

      @Override
      public void run()
      {
         // refresh timeouts in case they changed via MBean
         connectionTimeout = config.getConnectionTimeout();
         validationTimeout = config.getValidationTimeout();
         leakTask.updateLeakDetectionThreshold(config.getLeakDetectionThreshold());
         // 连接池中空闲状态连接的最长时间
         final long idleTimeout = config.getIdleTimeout();
         final long now = clockSource.currentTime();

         // Detect retrograde time, allowing +128ms as per NTP spec.
         if (clockSource.plusMillis(now, 128) < clockSource.plusMillis(previous, HOUSEKEEPING_PERIOD_MS)) {
            LOGGER.warn("{} - Retrograde clock change detected (housekeeper delta={}), soft-evicting connections from pool.",
                        clockSource.elapsedDisplayString(previous, now), poolName);
            previous = now;
            softEvictConnections();
            fillPool();
            return;
         }
         else if (now > clockSource.plusMillis(previous, (3 * HOUSEKEEPING_PERIOD_MS) / 2)) {
            // No point evicting for forward clock motion, this merely accelerates connection retirement anyway
            LOGGER.warn("{} - Thread starvation or clock leap detected (housekeeper delta={}).", clockSource.elapsedDisplayString(previous, now), poolName);
         }

         previous = now;

         String afterPrefix = "Pool ";
         if (idleTimeout > 0L) {
            final List<PoolEntry> idleList = connectionBag.values(STATE_NOT_IN_USE);
            int removable = idleList.size() - config.getMinimumIdle();
            if (removable > 0) {
               logPoolState("Before cleanup ");
               afterPrefix = "After cleanup  ";

               // Sort pool entries on lastAccessed
               Collections.sort(idleList, LAST_ACCESS_COMPARABLE);
               for (PoolEntry poolEntry : idleList) {
                  if (clockSource.elapsedMillis(poolEntry.lastAccessed, now) > idleTimeout && connectionBag.reserve(poolEntry)) {
                     closeConnection(poolEntry, "(connection has passed idleTimeout)");
                     if (--removable == 0) {
                        break; // keep min idle cons
                     }
                  }
               }
            }
         }

         logPoolState(afterPrefix);

         fillPool(); // Try to maintain minimum connections
      }
   }

   public static class PoolInitializationException extends RuntimeException
   {
      private static final long serialVersionUID = 929872118275916520L;

      /**
       * Construct an exception, possibly wrapping the provided Throwable as the cause.
       * @param t the Throwable to wrap
       */
      public PoolInitializationException(Throwable t)
      {
         super("Failed to initialize pool: " + t.getMessage(), t);
      }
   }
}
