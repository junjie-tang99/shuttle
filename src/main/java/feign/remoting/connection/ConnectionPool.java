/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feign.remoting.connection;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import feign.remoting.connection.strategy.ConnectionSelectStrategy;


public class ConnectionPool implements Scannable {
    // ~~~ constants
    /** logger */
    private static final Logger logger = LoggerFactory.getLogger("ConnectionPool");

    /** connections */
    private CopyOnWriteArrayList<Connection> conns = new CopyOnWriteArrayList<Connection>();

    /** strategy */
    private ConnectionSelectStrategy strategy;

    /** timestamp to record the last time this pool be accessed */
    private volatile long lastAccessTimestamp;

    /** whether async create connection done */
    private volatile boolean asyncCreationDone = true;

    
    public ConnectionPool(ConnectionSelectStrategy strategy) {
        this.strategy = strategy;
    }

    // ~~~ members

    //往Connection Pool中添加一个Connection对象
    public void add(Connection connection) {
        markAccess();
        if (null == connection) {
            return;
        }
        //将Connection对象，加入到Connection对象的List中
        boolean res = this.conns.addIfAbsent(connection);
        //如果添加成功，那么并对当前对象的引用计数+1
        if (res) {
            connection.increaseRef();
        }
    }

    
    public boolean contains(Connection connection) {
        return this.conns.contains(connection);
    }

    //从Connection Pool中删除一个Connection对象
    public void removeAndTryClose(Connection connection) {
        if (null == connection) {
            return;
        }
        boolean res = this.conns.remove(connection);
        //如果删除成功，那么将Connection对象的引用计数-1
        if (res) {
            connection.decreaseRef();
        }
        //如果当前Connection没有被引用了，那么执行Close操作
        if (connection.noRef()) {
            connection.close();
        }
    }

    /**
     * remove all connections
     */
    public void removeAllAndTryClose() {
        for (Connection conn : this.conns) {
            removeAndTryClose(conn);
        }
        this.conns.clear();
    }

    //根据Connection的选择策略，从Connection Pool中，选择出一个Connection对象
    public Connection get() {
        markAccess();
        if (null != this.conns) {
            List<Connection> snapshot = new ArrayList<Connection>(this.conns);
            if (snapshot.size() > 0) {
                return this.strategy.select(snapshot);
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    //获取所有的Connection对象列表
    public List<Connection> getAll() {
        markAccess();
        return new ArrayList<Connection>(this.conns);
    }

    /**
     * connection pool size
     *
     * @return
     */
    public int size() {
        return this.conns.size();
    }

    /**
     * is connection pool empty
     *
     * @return
     */
    public boolean isEmpty() {
        return this.conns.isEmpty();
    }

    /**
     * Getter method for property <tt>lastAccessTimestamp</tt>.
     *
     * @return property value of lastAccessTimestamp
     */
    public long getLastAccessTimestamp() {
        return this.lastAccessTimestamp;
    }

    /**
     * do mark the time stamp when access this pool
     */
    private void markAccess() {
        this.lastAccessTimestamp = System.currentTimeMillis();
    }

    /**
     * is async create connection done
     * @return
     */
    public boolean isAsyncCreationDone() {
        return this.asyncCreationDone;
    }

    /**
     * do mark async create connection done
     */
    public void markAsyncCreationDone() {
        this.asyncCreationDone = true;
    }

    /**
     * do mark async create connection start
     */
    public void markAsyncCreationStart() {
        this.asyncCreationDone = false;
    }

    /**
     * @see com.alipay.remoting.Scannable#scan()
     */
    @Override
    public void scan() {
        if (null != this.conns && !this.conns.isEmpty()) {
            for (Connection conn : conns) {
                if (!conn.isFine()) {
                    logger.warn(
                        "Remove bad connection when scanning conns of ConnectionPool - {}:{}",
                        conn.getRemoteIP(), conn.getRemotePort());
                    conn.close();
                    this.removeAndTryClose(conn);
                }
            }
        }
    }
}
