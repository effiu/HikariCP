/*
 * Copyright (C) 2014 Brett Wooldridge
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
import java.sql.Statement;
import java.util.Comparator;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.util.ClockSource;
import com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry;
import com.zaxxer.hikari.util.FastList;

/**
 * 译：跟踪连接实例的entry
 * Entry used in the ConcurrentBag to track Connection instances.
 *
 * @author Brett Wooldridge
 */
final class PoolEntry implements IConcurrentBagEntry {
   private static final Logger LOGGER = LoggerFactory.getLogger(PoolEntry.class);

   static final Comparator<PoolEntry> LAST_ACCESS_COMPARABLE;
   /**
    * 连接实例
    */
   Connection connection;

   /**
    * 上次访问
    */
   long lastAccessed;

   /**
    * 上次借出
    */
   long lastBorrowed;

   /**
    * 是否被驱逐
    */
   private volatile boolean evict;

   /**
    * 定时任务，当超过最大生存时间时销毁连接
    */
   private volatile ScheduledFuture<?> endOfLife;

   /**
    * statement数量
    */
   private final FastList<Statement> openStatements;

   /**
    * 连接池，用于将该连接归还连接池、销毁或者关闭连接
    */
   private final HikariPool hikariPool;

   /**
    * STATE_NOT_IN_USE = 0;
    * STATE_IN_USE = 1;
    * STATE_REMOVED = -1;
    * STATE_RESERVED = -2;
    */
   private final AtomicInteger state;

   private final boolean isReadOnly;
   private final boolean isAutoCommit;

   static {
      // 比较被借时间
      LAST_ACCESS_COMPARABLE = new Comparator<PoolEntry>() {
         @Override
         public int compare(final PoolEntry entryOne, final PoolEntry entryTwo) {
            return Long.compare(entryOne.lastAccessed, entryTwo.lastAccessed);
         }
      };
   }

   /**
    * 构造方法
    *
    * @param connection   数据库连接
    * @param pool         连接池
    * @param isReadOnly   是否只读
    * @param isAutoCommit 是否自动commit
    */
   PoolEntry(final Connection connection, final PoolBase pool, final boolean isReadOnly, final boolean isAutoCommit) {
      this.connection = connection;
      this.hikariPool = (HikariPool) pool;
      this.isReadOnly = isReadOnly;
      this.isAutoCommit = isAutoCommit;
      // 状态
      this.state = new AtomicInteger();
      // 最后借出时间默认为当前的时间戳，即创建时间
      this.lastAccessed = ClockSource.INSTANCE.currentTime();
      //默认最多创建16个Statements
      this.openStatements = new FastList<>(Statement.class, 16);
   }

   /**
    * 译：释放连接到连接池
    * Release this entry back to the pool.
    *
    * @param lastAccessed last access time-stamp
    */
   void recycle(final long lastAccessed) {
      // 修改当前访问时间
      this.lastAccessed = lastAccessed;
      // 将连接放到连接池中
      hikariPool.releaseConnection(this);
   }

   /**
    * 未来结束的时间
    * TODO, 最大空闲时间
    * @param endOfLife the end future
    */
   void setFutureEol(final ScheduledFuture<?> endOfLife) {
      this.endOfLife = endOfLife;
   }

   /**
    * 通过代理工厂创建连接
    * @param leakTask
    * @param now
    * @return
    */
   Connection createProxyConnection(final ProxyLeakTask leakTask, final long now) {
      return ProxyFactory.getProxyConnection(this, connection, openStatements, leakTask, now, isReadOnly, isAutoCommit);
   }

   /**
    * 重置连接状态，根据proxyConnection修改connection
    * @param proxyConnection
    * @param dirtyBits
    * @throws SQLException
    */
   void resetConnectionState(final ProxyConnection proxyConnection, final int dirtyBits) throws SQLException {
      hikariPool.resetConnectionState(connection, proxyConnection, dirtyBits);
   }

   String getPoolName() {
      return hikariPool.toString();
   }

   boolean isMarkedEvicted() {
      return evict;
   }

   void markEvicted() {
      this.evict = true;
   }

   void evict(final String closureReason) {
      hikariPool.closeConnection(this, closureReason);
   }

   /**
    * 译：返回借出的时间,会在调用{@link #recycle(long)}时调用此方法
    * Returns millis since lastBorrowed
    */
   long getMillisSinceBorrowed() {
      return ClockSource.INSTANCE.elapsedMillis(lastBorrowed);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public String toString() {
      final long now = ClockSource.INSTANCE.currentTime();
      return connection
         + ", accessed " + ClockSource.INSTANCE.elapsedDisplayString(lastAccessed, now) + " ago, "
         + stateToString();
   }

   // ***********************************************************************
   //                      IConcurrentBagEntry methods
   // ***********************************************************************

   /**
    * {@inheritDoc}
    */
   @Override
   public int getState() {
      return state.get();
   }

   /**
    * cas修改, 若当前值==预期值，则将该值原子设置为给定的更新值。
    * {@inheritDoc}
    */
   @Override
   public boolean compareAndSet(int expect, int update) {
      return state.compareAndSet(expect, update);
   }

   /**
    * 修改state
    * {@inheritDoc}
    */
   @Override
   public void lazySet(int update) {
      // 最终设定为给定值
      state.lazySet(update);
   }

   Connection close() {
      ScheduledFuture<?> eol = endOfLife;
      if (eol != null && !eol.isDone() && !eol.cancel(false)) {
         // 过期任务销毁连接返回false
         LOGGER.warn("{} - maxLifeTime expiration task cancellation unexpectedly returned false for connection {}", getPoolName(), connection);
      }

      Connection con = connection;
      // 将poolEntry置空
      connection = null;
      // 定时销毁时间置空
      endOfLife = null;
      return con;
   }

   private String stateToString() {
      switch (state.get()) {
         case STATE_IN_USE:
            return "IN_USE";
         case STATE_NOT_IN_USE:
            return "NOT_IN_USE";
         case STATE_REMOVED:
            return "REMOVED";
         case STATE_RESERVED:
            return "RESERVED";
         default:
            return "Invalid";
      }
   }
}
