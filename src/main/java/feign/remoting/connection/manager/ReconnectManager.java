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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import feign.remoting.connection.Url;
import feign.remoting.exception.RemotingException;

/**
 * Reconnect manager.
 * 
 * @author yunliang.shi
 * @version $Id: ReconnectManager.java, v 0.1 Mar 11, 2016 5:20:50 PM yunliang.shi Exp $
 */
public class ReconnectManager {
    private static final Logger logger = LoggerFactory.getLogger("CommonDefault");

    class ReconnectTask {
        Url url;
    }

    private final LinkedBlockingQueue<ReconnectTask> tasks = new LinkedBlockingQueue<ReconnectTask>();

    protected final List<Url/* url */> canceled = new CopyOnWriteArrayList<Url>();
    private volatile boolean started;

    private int healConnectionInterval = 1000;

    private final Thread healConnectionThreads;

    private ConnectionManager connectionManager;

    public ReconnectManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        //启动修复连接的线程
        this.healConnectionThreads = new Thread(new HealConnectionRunner());
        this.healConnectionThreads.start();
        this.started = true;
    }

    //调用connectionManager的createConnectionAndHealIfNeed方法，实现对于未达到指定数量的connection pool，
    //往这个pool里面添加connection
    private void doReconnectTask(ReconnectTask task) throws InterruptedException, RemotingException {
        connectionManager.createConnectionAndHealIfNeed(task.url);
    }

    private void addReconnectTask(ReconnectTask task) {
        tasks.add(task);
    }

    public void addCancelUrl(Url url) {
        canceled.add(url);
    }

    public void removeCancelUrl(Url url) {
        canceled.remove(url);
    }

    /**
     * add reconnect task
     * 
     * @param url
     */
    public void addReconnectTask(Url url) {
        ReconnectTask task = new ReconnectTask();
        task.url = url;
        tasks.add(task);
    }

    /**
     * Check task whether is valid, if canceled, is not valid
     * 
     * @param task
     * @return
     */
    private boolean isValidTask(ReconnectTask task) {
        return !canceled.contains(task.url);
    }

    /**
     * stop reconnect thread
     */
    public void stop() {
        if (!this.started) {
            return;
        }
        this.started = false;
        healConnectionThreads.interrupt();
        this.tasks.clear();
        this.canceled.clear();
    }

    //用于修复连接的线程
    private final class HealConnectionRunner implements Runnable {
        private long lastConnectTime = -1;

        @Override
        public void run() {
        	//如果标志位是true的话，执行后续任务
            while (ReconnectManager.this.started) {
                long start = -1;
                ReconnectTask task = null;
                try {
                	//如果重连的时间>0且小于重连周期（1000ms），则线程休眠1个重连周期
                    if (this.lastConnectTime > 0
                        && this.lastConnectTime < ReconnectManager.this.healConnectionInterval
                        || this.lastConnectTime < 0) {
                        Thread.sleep(ReconnectManager.this.healConnectionInterval);
                    }
                    //从LinkedBlockingQueue中，获取待重连的URL
                    try {
                        task = ReconnectManager.this.tasks.take();
                    } catch (InterruptedException e) {
                        // ignore
                    }

                    start = System.currentTimeMillis();
                    //判断当前的重连任务是否处于取消的队列中
                    if (ReconnectManager.this.isValidTask(task)) {
                        try {
                        	//调用connectionManager的createConnectionAndHealIfNeed(task.url)接口
                            ReconnectManager.this.doReconnectTask(task);
                        } catch (InterruptedException e) {
                            throw e;
                        }
                    } else {
                        logger.warn("Invalid reconnect request task {}, cancel list size {}",
                            task.url, canceled.size());
                    }
                    this.lastConnectTime = System.currentTimeMillis() - start;
                } catch (Exception e) {
                    retryWhenException(start, task, e);
                }
            }
        }

        private void retryWhenException(long start, ReconnectTask task, Exception e) {
            if (start != -1) {
                this.lastConnectTime = System.currentTimeMillis() - start;
            }
            if (task != null) {
                logger.warn("reconnect target: {} failed.", task.url, e);
                ReconnectManager.this.addReconnectTask(task);
            }
        }
    }
}
