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
package feign.remoting.connection.manager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import feign.remoting.config.ConfigManager;
import feign.remoting.connection.Connection;
import feign.remoting.connection.ConnectionPool;
import feign.remoting.connection.Scannable;
import feign.remoting.connection.Url;
import feign.remoting.connection.factory.ConnectionFactory;
import feign.remoting.connection.handler.ConnectionEventHandler;
import feign.remoting.connection.listener.ConnectionEventListener;
import feign.remoting.connection.strategy.ConnectionSelectStrategy;
import feign.remoting.connection.strategy.RandomSelectStrategy;
import feign.remoting.exception.RemotingException;
import feign.remoting.switches.GlobalSwitch;
import feign.remoting.util.FutureTaskUtil;
import feign.remoting.util.NamedThreadFactory;
import feign.remoting.util.RemotingAddressParser;
import feign.remoting.util.RunStateRecordedFutureTask;
import feign.remoting.util.StringUtils;
import io.netty.bootstrap.Bootstrap;

/**
 * Abstract implementation of connection manager
 *
 * @author xiaomin.cxm
 * @version $Id: DefaultConnectionManager.java, v 0.1 Mar 8, 2016 10:43:51 AM xiaomin.cxm Exp $
 */
public class DefaultConnectionManager implements ConnectionManager, ConnectionHeartbeatManager, Scannable {

    // ~~~ constants
    private static final Logger logger = LoggerFactory.getLogger("DefaultConnectionManager");

    //从connection pool中移除的默认超时时间，时间单位为毫秒
    private static final int DEFAULT_EXPIRE_TIME = 10 * 60 * 1000;

    //当从FutureTask中无法拿到结果时，重试的次数
    private static final int DEFAULT_RETRY_TIMES = 2;

    // ~~~ members
    //设置asyncCreateConnectionExecutor（异步连接创建池）的最小线程数，默认值是3
    private int minPoolSize = ConfigManager.conn_create_tp_min_size();

    //设置asyncCreateConnectionExecutor（异步连接创建池）的最大线程数，默认值是8
    private int maxPoolSize = ConfigManager.conn_create_tp_max_size();

    //设置asyncCreateConnectionExecutor（异步连接创建池）的队列长度，默认值是50
    private int queueSize = ConfigManager.conn_create_tp_queue_size();

    //设置asyncCreateConnectionExecutor（异步连接创建池）的线程的存活时间，默认值是60
    private long keepAliveTime = ConfigManager.conn_create_tp_keepalive();

    //线程池是否已被初始化
    private volatile boolean executorInitialized;

    //用于异步创建Connection连接，这是一个懒初始化
    private Executor asyncCreateConnectionExecutor;

    //全局开关
    private GlobalSwitch globalSwitch;

    //连接池初始化的任务
    protected ConcurrentHashMap<String, RunStateRecordedFutureTask<ConnectionPool>> connTasks;

    //修复连接的任务
    protected ConcurrentHashMap<String, FutureTask<Integer>> healTasks;

    //从连接池中选择连接的策略
    protected ConnectionSelectStrategy connectionSelectStrategy;

    //远程地址解析器
    protected RemotingAddressParser addressParser;

    //连接创建工厂
    protected ConnectionFactory connectionFactory;

    //连接事件处理器
    protected ConnectionEventHandler connectionEventHandler;

    //连接事件监听器
    protected ConnectionEventListener connectionEventListener;

    // ~~~ constructors
    public DefaultConnectionManager() {
    	//连接池初始化的任务
        this.connTasks = new ConcurrentHashMap<String, RunStateRecordedFutureTask<ConnectionPool>>();
        //修复连接的任务
        this.healTasks = new ConcurrentHashMap<String, FutureTask<Integer>>();
        //在选择连接时，不对连接进行监控，即：不过滤掉处于inActive状态的连接
        this.connectionSelectStrategy = new RandomSelectStrategy(globalSwitch);
    }

    public DefaultConnectionManager(ConnectionSelectStrategy connectionSelectStrategy) {
        this();
        this.connectionSelectStrategy = connectionSelectStrategy;
    }

    public DefaultConnectionManager(ConnectionSelectStrategy connectionSelectStrategy,ConnectionFactory connectionFactory) {
        this(connectionSelectStrategy);
        this.connectionFactory = connectionFactory;
    }

    public DefaultConnectionManager(ConnectionFactory connectionFactory,
                                    RemotingAddressParser addressParser,
                                    ConnectionEventHandler connectionEventHandler) {
        this(new RandomSelectStrategy(), connectionFactory);
        this.addressParser = addressParser;
        this.connectionEventHandler = connectionEventHandler;
    }

    public DefaultConnectionManager(ConnectionSelectStrategy connectionSelectStrategy,
                                    ConnectionFactory connectionFactory,
                                    ConnectionEventHandler connectionEventHandler,
                                    ConnectionEventListener connectionEventListener) {
        this(connectionSelectStrategy, connectionFactory);
        this.connectionEventHandler = connectionEventHandler;
        this.connectionEventListener = connectionEventListener;
    }

    public DefaultConnectionManager(ConnectionSelectStrategy connectionSelectStrategy,
                                    ConnectionFactory connctionFactory,
                                    ConnectionEventHandler connectionEventHandler,
                                    ConnectionEventListener connectionEventListener,
                                    GlobalSwitch globalSwitch) {
        this(connectionSelectStrategy, connctionFactory, connectionEventHandler,
            connectionEventListener);
        this.globalSwitch = globalSwitch;
    }

    // ~~~ interface methods

    /**
     * @see feign.remoting.connection.manager.remoting.ConnectionManager#init()
     */
    @Override
    public void init() {
    	//将连接事件处理器的ConnectionManager设置为当前实例
        this.connectionEventHandler.setConnectionManager(this);
        //设置连接事件处理器的监听器
        this.connectionEventHandler.setConnectionEventListener(connectionEventListener);
        //初始化连接工厂
        //创建Bootstrap对象，并设置Handler等信息，但不连接到具体的地址
        this.connectionFactory.init(connectionEventHandler);
    }

    @Override
    public void add(Connection connection) {
    	//获取当前连接所支持的所有URL，并将当前连接以url作为key，加入到ConnectionPool中
        Set<String> poolKeys = connection.getPoolKeys();
        for (String poolKey : poolKeys) {
            this.add(connection, poolKey);
        }
    }

    //以poolKey为key，将Connection对象，加入到Connection Pool中
    //其中，ConnectionPool是空的话，那么进行创建
    @Override
    public void add(Connection connection, String poolKey) {
        ConnectionPool pool = null;
        try {
            // get or create an empty connection pool
            pool = this.getConnectionPoolAndCreateIfAbsent(poolKey, new ConnectionPoolCall());
        } catch (Exception e) {
            // should not reach here.
            logger.error(
                "[NOTIFYME] Exception occurred when getOrCreateIfAbsent an empty ConnectionPool!",
                e);
        }
        //将当前的Connection加入到Connection Pool中
        if (pool != null) {
            pool.add(connection);
        } else {
            // should not reach here.
            logger.error("[NOTIFYME] Connection pool NULL!");
        }
    }

    //通过poolKey（URL），从ConnectionPool中获取到一个connection对象
    @Override
    public Connection get(String poolKey) {
    	//根据Connection的poolKey，从connTasks中获取对应的RunStateRecordedFutureTask任务
    	//并通过RunStateRecordedFutureTask的getAfterRun方法，获取到ConnectioPool对象
        ConnectionPool pool = this.getConnectionPool(this.connTasks.get(poolKey));
        //从ConnectioPool对象中，获取1个connection对象
        return null == pool ? null : pool.get();
    }

    //通过poolKey（URL），从ConnectionPool中获取到所有的connection对象
    @Override
    public List<Connection> getAll(String poolKey) {
        ConnectionPool pool = this.getConnectionPool(this.connTasks.get(poolKey));
        return null == pool ? new ArrayList<Connection>() : pool.getAll();
    }

    //将所有的URL及所对应的Connection都返回
    @Override
    public Map<String, List<Connection>> getAll() {
        Map<String, List<Connection>> allConnections = new HashMap<String, List<Connection>>();
        Iterator<Map.Entry<String, RunStateRecordedFutureTask<ConnectionPool>>> iterator = this
            .getConnPools().entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, RunStateRecordedFutureTask<ConnectionPool>> entry = iterator.next();
            ConnectionPool pool = FutureTaskUtil.getFutureTaskResult(entry.getValue(), logger);
            if (null != pool) {
                allConnections.put(entry.getKey(), pool.getAll());
            }
        }
        return allConnections;
    }

    //将指定连接从ConnectionPool中删除
    @Override
    public void remove(Connection connection) {
        if (null == connection) {
            return;
        }
        Set<String> poolKeys = connection.getPoolKeys();
        if (null == poolKeys || poolKeys.isEmpty()) {
            connection.close();
            logger.warn("Remove and close a standalone connection.");
        } else {
            for (String poolKey : poolKeys) {
                this.remove(connection, poolKey);
            }
        }
    }

    //根据Connection对象以及poolKey，将Connection从ConnectionPool中删除
    //并将改Connection进行Close
    @Override
    public void remove(Connection connection, String poolKey) {
        if (null == connection || StringUtils.isBlank(poolKey)) {
            return;
        }
        ConnectionPool pool = this.getConnectionPool(this.connTasks.get(poolKey));
        if (null == pool) {
        	//实际上调用的是SocketChannel.Close功能
            connection.close();
            logger.warn("Remove and close a standalone connection.");
        } else {
        	//将当前连接冲Connection Pool中删除
            pool.removeAndTryClose(connection);
            //如果当前连接池中，没有连接了，那么将当前URL从连接任务中删除
            //并且，执行ConnectionPool.Clear操作
            if (pool.isEmpty()) {
                this.removeTask(poolKey);
                logger.warn(
                    "Remove and close the last connection in ConnectionPool with poolKey {}",
                    poolKey);
            } else {
                logger
                    .warn(
                        "Remove and close a connection in ConnectionPool with poolKey {}, {} connections left.",
                        poolKey, pool.size());
            }
        }
    }

    //将指定URL的所有连接进行关闭
    @Override
    public void remove(String poolKey) {
        if (StringUtils.isBlank(poolKey)) {
            return;
        }

        RunStateRecordedFutureTask<ConnectionPool> task = this.connTasks.remove(poolKey);
        if (null != task) {
            ConnectionPool pool = this.getConnectionPool(task);
            if (null != pool) {
                pool.removeAllAndTryClose();
                logger.warn("Remove and close all connections in ConnectionPool of poolKey={}",
                    poolKey);
            }
        }
    }

    
    @Override
    public void removeAll() {
        if (null == this.connTasks || this.connTasks.isEmpty()) {
            return;
        }
        if (null != this.connTasks && !this.connTasks.isEmpty()) {
            Iterator<String> iter = this.connTasks.keySet().iterator();
            while (iter.hasNext()) {
                String poolKey = iter.next();
                this.removeTask(poolKey);
                iter.remove();
            }
            logger.warn("All connection pool and connections have been removed!");
        }
    }

    //检查连接是否为null
    //检查连接是否为连接状态
    //检查连接是否为可写状态
    @Override
    public void check(Connection connection) throws RemotingException {
        if (connection == null) {
            throw new RemotingException("Connection is null when do check!");
        }
        if (connection.getChannel() == null || !connection.getChannel().isActive()) {
            this.remove(connection);
            throw new RemotingException("Check connection failed for address: "
                                        + connection.getUrl());
        }
        if (!connection.getChannel().isWritable()) {
            // No remove. Most of the time it is unwritable temporarily.
            throw new RemotingException("Check connection failed for address: "
                                        + connection.getUrl() + ", maybe write overflow!");
        }
    }

    //返回指定URL的所对应Connection Pool已创建连接数
    @Override
    public int count(String poolKey) {
        if (StringUtils.isBlank(poolKey)) {
            return 0;
        }
        ConnectionPool pool = this.getConnectionPool(this.connTasks.get(poolKey));
        if (null != pool) {
            return pool.size();
        } else {
            return 0;
        }
    }

    //为了防止缓存污染和连接泄露，需要定时扫描
    @Override
    public void scan() {
        if (null != this.connTasks && !this.connTasks.isEmpty()) {
            Iterator<String> iter = this.connTasks.keySet().iterator();
            while (iter.hasNext()) {
                String poolKey = iter.next();
                ConnectionPool pool = this.getConnectionPool(this.connTasks.get(poolKey));
                if (null != pool) {
                	//调用连接池的scan功能，遍历该连接池中的所有连接
                	//对于哪些inActive状态的Connection进行Close操作
                    pool.scan();
                    //对于哪些连接池中没有连接，且最后访问时间超过60分钟的连接池，从connTasks中取出
                    if (pool.isEmpty()) {
                        if ((System.currentTimeMillis() - pool.getLastAccessTimestamp()) > DEFAULT_EXPIRE_TIME) {
                            iter.remove();
                            logger.warn("Remove expired pool task of poolKey {} which is empty.",poolKey);
                        }
                    }
                }
            }
        }
    }

    //通过Url对象，从连接池中获取对应的连接对象
    @Override
    public Connection getAndCreateIfAbsent(Url url) throws InterruptedException, RemotingException {
        // get and create a connection pool with initialized connections.
        ConnectionPool pool = this.getConnectionPoolAndCreateIfAbsent(url.getUniqueKey(),new ConnectionPoolCall(url));
        if (null != pool) {
            return pool.get();
        } else {
            logger.error("[NOTIFYME] bug detected! pool here must not be null!");
            return null;
        }
    }

    // If no task cached, create one and initialize the connections.
    // If task cached, check whether the number of connections adequate, if not then heal it.
    @Override
    public void createConnectionAndHealIfNeed(Url url) throws InterruptedException,
                                                      RemotingException {
        ConnectionPool pool = this.getConnectionPoolAndCreateIfAbsent(url.getUniqueKey(),new ConnectionPoolCall(url));
        if (null != pool) {
            healIfNeed(pool, url);
        } else {
            logger.error("[NOTIFYME] bug detected! pool here must not be null!");
        }
    }

    //根据address字符串，创建1个连接对象，但是这个对象不加入到连接池中
    @Override
    public Connection create(String address, int connectTimeout) throws RemotingException {
        Url url = this.addressParser.parse(address);
        url.setConnectTimeout(connectTimeout);
        return create(url);
    }
    
    //根据address的URL对象，创建一个Connection对象，但是这个对象不加入到连接池中
    @Override
    public Connection create(Url url) throws RemotingException {
        Connection conn = null;
        try {
            conn = this.connectionFactory.createConnection(url);
        } catch (Exception e) {
            throw new RemotingException("Create connection failed. The address is "
                                        + url.getOriginUrl(), e);
        }
        return conn;
    }

    //根据IP、Port、超时时间，创建一个Connection对象，但是这个对象不加入到连接池中
    @Override
    public Connection create(String ip, int port, int connectTimeout) throws RemotingException {
        Connection conn = null;
        try {
            conn = this.connectionFactory.createConnection(ip, port, connectTimeout);
        } catch (Exception e) {
            throw new RemotingException("Create connection failed. The address is " + ip + ":"
                                        + port, e);
        }
        return conn;
    }


    //针对某个Connection关闭心跳检查
    @Override
    public void disableHeartbeat(Connection connection) {
        if (null != connection) {
            connection.getChannel().attr(Connection.HEARTBEAT_SWITCH).set(false);
        }
    }

    //针对某个Connection开启心跳检查
    @Override
    public void enableHeartbeat(Connection connection) {
        if (null != connection) {
            connection.getChannel().attr(Connection.HEARTBEAT_SWITCH).set(true);
        }
    }

    // ~~~ private methods

    /**
     * get connection pool from future task
     *
     * @param task
     * @return
     */
    private ConnectionPool getConnectionPool(RunStateRecordedFutureTask<ConnectionPool> task) {
        return FutureTaskUtil.getFutureTaskResult(task, logger);

    }

    //以指定poolKey，获取对应的的ConnectionPool
    private ConnectionPool getConnectionPoolAndCreateIfAbsent(String poolKey,Callable<ConnectionPool> callable)
                                                                                                throws RemotingException,
                                                                                                InterruptedException {
    	
        RunStateRecordedFutureTask<ConnectionPool> initialTask = null;
        ConnectionPool pool = null;
        //重试获取Connection Pool的次数
        int retry = DEFAULT_RETRY_TIMES;

        int timesOfResultNull = 0;
        int timesOfInterrupt = 0;
        
        //===========================================================
        //条件：重试获取Connection Pool的次数 < 重试次数（2） && Pool==null
        //===========================================================
        for (int i = 0; (i < retry) && (pool == null); ++i) {
        	//根据URL，从connTasks（ConcurrentHashMap）获取RunStateRecordedFutureTask
            initialTask = this.connTasks.get(poolKey);
            
            //无法获取到poolKey的连接任务，即：之前对于这个poolKey都没创建过连接池
            if (null == initialTask) {
            	//将ConnectionCall的Callable对象，包装到RunStateRecordedFutureTask中
                initialTask = new RunStateRecordedFutureTask<ConnectionPool>(callable);
                //将ConnectionCall加入到connTasks列表中
                initialTask = this.connTasks.putIfAbsent(poolKey, initialTask);
                if (null == initialTask) {
                    initialTask = this.connTasks.get(poolKey);
                    //执行创建连接池
                    initialTask.run();
                }
            }
            
            //如果之前已经创建过poolKey的线程池
            try {
            	//调用设置到RunStateRecordedFutureTask中的ConnectionCall的Call()方法
                pool = initialTask.get();
                //如果无法获取到connection pool,那么进行重试
                if (null == pool) {
                    if (i + 1 < retry) {
                        timesOfResultNull++;
                        continue;
                    }
                    //如果超过指定次数，仍旧无法获取Connection pool，那么从初始化任务列表中删除该任务
                    this.connTasks.remove(poolKey);
                    String errMsg = "Get future task result null for poolKey [" + poolKey
                                    + "] after [" + (timesOfResultNull + 1) + "] times try.";
                    throw new RemotingException(errMsg);
                }
            } catch (InterruptedException e) {
                if (i + 1 < retry) {
                    timesOfInterrupt++;
                    continue;// retry if interrupted
                }
                this.connTasks.remove(poolKey);
                logger
                    .warn(
                        "Future task of poolKey {} interrupted {} times. InterruptedException thrown and stop retry.",
                        poolKey, (timesOfInterrupt + 1), e);
                throw e;
            } catch (ExecutionException e) {
                // DO NOT retry if ExecutionException occurred
                this.connTasks.remove(poolKey);

                Throwable cause = e.getCause();
                if (cause instanceof RemotingException) {
                    throw (RemotingException) cause;
                } else {
                    FutureTaskUtil.launderThrowable(cause);
                }
            }
        }
        return pool;
    }

    /**
     * remove task and remove all connections
     *
     * @param poolKey
     */
    private void removeTask(String poolKey) {
        RunStateRecordedFutureTask<ConnectionPool> task = this.connTasks.remove(poolKey);
        if (null != task) {
            ConnectionPool pool = FutureTaskUtil.getFutureTaskResult(task, logger);
            if (null != pool) {
                pool.removeAllAndTryClose();
            }
        }
    }

    /**
     * execute heal connection tasks if the actual number of connections in pool is less than expected
     *
     * @param pool
     * @param url
     */
    private void healIfNeed(ConnectionPool pool, Url url) throws RemotingException,
                                                         InterruptedException {
        String poolKey = url.getUniqueKey();
        // only when async creating connections done
        // and the actual size of connections less than expected, the healing task can be run.
        if (pool.isAsyncCreationDone() && pool.size() < url.getConnNum()) {
            FutureTask<Integer> task = this.healTasks.get(poolKey);
            if (null == task) {
                task = new FutureTask<Integer>(new HealConnectionCall(url, pool));
                task = this.healTasks.putIfAbsent(poolKey, task);
                if (null == task) {
                    task = this.healTasks.get(poolKey);
                    task.run();
                }
            }
            try {
                int numAfterHeal = task.get();
                if (logger.isDebugEnabled()) {
                    logger.debug("[NOTIFYME] - conn num after heal {}, expected {}, warmup {}",
                        numAfterHeal, url.getConnNum(), url.isConnWarmup());
                }
            } catch (InterruptedException e) {
                this.healTasks.remove(poolKey);
                throw e;
            } catch (ExecutionException e) {
                this.healTasks.remove(poolKey);
                Throwable cause = e.getCause();
                if (cause instanceof RemotingException) {
                    throw (RemotingException) cause;
                } else {
                    FutureTaskUtil.launderThrowable(cause);
                }
            }
            // heal task is one-off, remove from cache directly after run
            this.healTasks.remove(poolKey);
        }
    }

    //用来异步创建ConnectionPool的任务
    private class ConnectionPoolCall implements Callable<ConnectionPool> {
        private boolean whetherInitConnection;
        private Url url;

        public ConnectionPoolCall() {
            this.whetherInitConnection = false;
        }

        public ConnectionPoolCall(Url url) {
            this.whetherInitConnection = true;
            this.url = url;
        }

        @Override
        public ConnectionPool call() throws Exception {
        	//创建一个新的连接池
            final ConnectionPool pool = new ConnectionPool(connectionSelectStrategy);
            if (whetherInitConnection) {
                try {
                	//在连接池中创建一个线程，其中有1个线程需要同步创建
                    doCreate(this.url, pool, this.getClass().getSimpleName(), 1);
                } catch (Exception e) {
                    pool.removeAllAndTryClose();
                    throw e;
                }
            }
            return pool;
        }

    }

    //对Connection中的连接进行修复
    private class HealConnectionCall implements Callable<Integer> {
        private Url            url;
        private ConnectionPool pool;

        public HealConnectionCall(Url url, ConnectionPool pool) {
            this.url = url;
            this.pool = pool;
        }

        @Override
        public Integer call() throws Exception {
        	//创建连接时，不以同步的方式创建，即：syncCreateNumWhenNotWarmup=0
            doCreate(this.url, this.pool, this.getClass().getSimpleName(), 0);
            return this.pool.size();
        }
    }

    //在连接池中创建对应的连接
    private void doCreate(final Url url, final ConnectionPool pool, final String taskName,
                          final int syncCreateNumWhenNotWarmup) throws RemotingException {
        final int actualNum = pool.size();
        final int expectNum = url.getConnNum();
        //如果连接池中的实际连接数量小于期望的连接数量
        if (actualNum < expectNum) {
            if (logger.isDebugEnabled()) {
                logger.debug("actual num {}, expect num {}, task name {}", actualNum, expectNum,
                    taskName);
            }
            //如果url需要对连接进行warmup，那么根据url中的expectNum，来创建对应数量的Connection数量
            if (url.isConnWarmup()) {
                for (int i = actualNum; i < expectNum; ++i) {
                	//通过ConnectionFactory创建Connection
                    Connection connection = create(url);
                    pool.add(connection);
                }
            //如果在url中未指定warmup参数
            } else {
                if (syncCreateNumWhenNotWarmup < 0 || syncCreateNumWhenNotWarmup > url.getConnNum()) {
                    throw new IllegalArgumentException(
                        "sync create number when not warmup should be [0," + url.getConnNum() + "]");
                }
                // create connection in sync way
                // 根据syncCreateNumWhenNotWarmup参数，创建指定数量的连接
                if (syncCreateNumWhenNotWarmup > 0) {
                    for (int i = 0; i < syncCreateNumWhenNotWarmup; ++i) {
                    	//通过ConnectionFactory创建Connection
                        Connection connection = create(url);
                        //通过将创建出来的连接加入到Connection Pool中
                        pool.add(connection);
                    }
                    //若指定同步创建线程对象的数量==url中设置的连接数量，那么不进行异步创建连接的操作
                    if (syncCreateNumWhenNotWarmup == url.getConnNum()) {
                        return;
                    }
                }
                
                //==========================================
                // 后续开始通过Executor来异步的创建Connection
                //==========================================
                //对asyncCreateConnectionExecutor（异步连接创建池）连接池进行初始化，
                //其中,设置的最小线程数（minPoolSize）是3，设置的最大线程数（maxPoolSize），默认值是8；
                //阻塞队列的模式是：ArrayBlockingQueue
                initializeExecutor();
                
                
                //对于未达到url指定的连接数的情况下，会通过线程池，异步创建连接对象
                pool.markAsyncCreationStart();// mark the start of async
                try {
                    this.asyncCreateConnectionExecutor.execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                for (int i = pool.size(); i < url.getConnNum(); ++i) {
                                    Connection conn = null;
                                    try {
                                        conn = create(url);
                                    } catch (RemotingException e) {
                                        logger
                                            .error(
                                                "Exception occurred in async create connection thread for {}, taskName {}",
                                                url.getUniqueKey(), taskName, e);
                                    }
                                    pool.add(conn);
                                }
                            } finally {
                                pool.markAsyncCreationDone();// mark the end of async
                            }
                        }
                    });
                } catch (RejectedExecutionException e) {
                    pool.markAsyncCreationDone();// mark the end of async when reject
                    throw e;
                }
            } // end of NOT warm up
        } // end of if
    }

    /**
     * initialize executor
     */
    private void initializeExecutor() {
        if (!this.executorInitialized) {
            this.executorInitialized = true;
            this.asyncCreateConnectionExecutor = new ThreadPoolExecutor(minPoolSize, maxPoolSize,
                keepAliveTime, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(queueSize),
                new NamedThreadFactory("Bolt-conn-warmup-executor", true));
        }
    }

    // ~~~ getters and setters

    /**
     * Getter method for property <tt>connectionSelectStrategy</tt>.
     *
     * @return property value of connectionSelectStrategy
     */
    public ConnectionSelectStrategy getConnectionSelectStrategy() {
        return connectionSelectStrategy;
    }

    /**
     * Setter method for property <tt>connectionSelectStrategy</tt>.
     *
     * @param connectionSelectStrategy value to be assigned to property connectionSelectStrategy
     */
    public void setConnectionSelectStrategy(ConnectionSelectStrategy connectionSelectStrategy) {
        this.connectionSelectStrategy = connectionSelectStrategy;
    }

    /**
     * Getter method for property <tt>addressParser</tt>.
     *
     * @return property value of addressParser
     */
    public RemotingAddressParser getAddressParser() {
        return addressParser;
    }

    /**
     * Setter method for property <tt>addressParser</tt>.
     *
     * @param addressParser value to be assigned to property addressParser
     */
    public void setAddressParser(RemotingAddressParser addressParser) {
        this.addressParser = addressParser;
    }

    /**
     * Getter method for property <tt>connctionFactory</tt>.
     *
     * @return property value of connctionFactory
     */
    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    /**
     * Setter method for property <tt>connctionFactory</tt>.
     *
     * @param connectionFactory value to be assigned to property connctionFactory
     */
    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    /**
     * Getter method for property <tt>connectionEventHandler</tt>.
     *
     * @return property value of connectionEventHandler
     */
    public ConnectionEventHandler getConnectionEventHandler() {
        return connectionEventHandler;
    }

    /**
     * Setter method for property <tt>connectionEventHandler</tt>.
     *
     * @param connectionEventHandler value to be assigned to property connectionEventHandler
     */
    public void setConnectionEventHandler(ConnectionEventHandler connectionEventHandler) {
        this.connectionEventHandler = connectionEventHandler;
    }

    /**
     * Getter method for property <tt>connectionEventListener</tt>.
     *
     * @return property value of connectionEventListener
     */
    public ConnectionEventListener getConnectionEventListener() {
        return connectionEventListener;
    }

    /**
     * Setter method for property <tt>connectionEventListener</tt>.
     *
     * @param connectionEventListener value to be assigned to property connectionEventListener
     */
    public void setConnectionEventListener(ConnectionEventListener connectionEventListener) {
        this.connectionEventListener = connectionEventListener;
    }

    /**
     * Getter method for property <tt>connPools</tt>.
     *
     * @return property value of connPools
     */
    public ConcurrentHashMap<String, RunStateRecordedFutureTask<ConnectionPool>> getConnPools() {
        return this.connTasks;
    }
}
