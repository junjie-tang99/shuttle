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
package feign.remoting.connection.strategy;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import feign.remoting.config.ConfigManager;
import feign.remoting.config.Configs;
import feign.remoting.connection.Connection;
import feign.remoting.connection.ConnectionPool;
import feign.remoting.util.FutureTaskUtil;
import feign.remoting.util.RemotingUtil;
import feign.remoting.util.RunStateRecordedFutureTask;


/**
 * An implemented strategy to monitor connections:
 *   <lu>
 *       <li>each time scheduled, filter connections with {@link Configs#CONN_SERVICE_STATUS_OFF} at first.</li>
 *       <li>then close connections.</li>
 *   </lu>
 *
 * @author tsui
 * @version $Id: ScheduledDisconnectStrategy.java, v 0.1 2017-02-21 14:14 tsui Exp $
 */
public class ScheduledDisconnectStrategy implements ConnectionMonitorStrategy {
    private static final Logger logger = LoggerFactory.getLogger("CommonDefault");

    /** the connections threshold of each {@link Url#uniqueKey} */
    private static final int        CONNECTION_THRESHOLD   = ConfigManager.conn_threshold();

    /** fresh select connections to be closed */
    private Map<String, Connection> freshSelectConnections = new ConcurrentHashMap<String, Connection>();

    /** Retry detect period for ScheduledDisconnectStrategy*/
    private static int              RETRY_DETECT_PERIOD    = ConfigManager.retry_detect_period();

    /** random */
    private Random  random = new Random();

    /**
     * Filter connections to monitor
     *
     * @param connections
     */
    @Override
    public Map<String, List<Connection>> filter(List<Connection> connections) {
    	//服务状态是有效的连接
        List<Connection> serviceOnConnections = new ArrayList<Connection>();
        //服务状态是无效的连接
        List<Connection> serviceOffConnections = new ArrayList<Connection>();
        //过滤完成之后的连接
        Map<String, List<Connection>> filteredConnections = new ConcurrentHashMap<String, List<Connection>>();

        for (Connection connection : connections) {
        	//获取服务状态是ON的连接
            String serviceStatus = (String) connection.getAttribute(Configs.CONN_SERVICE_STATUS);
            if (serviceStatus != null) {
            	//当前连接中Future是否为空，并且
                if (connection.isInvokeFutureMapFinish()
                    && !freshSelectConnections.containsValue(connection)) {
                	//将当前连接加入到服务状态为OFF的队列中
                    serviceOffConnections.add(connection);
                }
            } else {
            	//将当前连接加入到服务状态为ON的队列中
                serviceOnConnections.add(connection);
            }
        }

        filteredConnections.put(Configs.CONN_SERVICE_STATUS_ON, serviceOnConnections);
        filteredConnections.put(Configs.CONN_SERVICE_STATUS_OFF, serviceOffConnections);
        return filteredConnections;
    }

    /**
     * Monitor connections and close connections with status is off
     *
     * @param connPools
     */
    @Override
    public void monitor(Map<String, RunStateRecordedFutureTask<ConnectionPool>> connPools) {
        try {
            if (null != connPools && !connPools.isEmpty()) {
                Iterator<Map.Entry<String, RunStateRecordedFutureTask<ConnectionPool>>> iter = connPools
                    .entrySet().iterator();
                //遍历连接池的future任务
                while (iter.hasNext()) {
                    Map.Entry<String, RunStateRecordedFutureTask<ConnectionPool>> entry = iter.next();
                    String poolKey = entry.getKey();
                    //通过future任务获取连接池
                    ConnectionPool pool = FutureTaskUtil.getFutureTaskResult(entry.getValue(),logger);

                    List<Connection> connections = pool.getAll();
                    //对连接池中的连接进行过滤，得到Service状态分别为ON和OFF的连接list
                    Map<String, List<Connection>> filteredConnectons = this.filter(connections);
                    List<Connection> serviceOnConnections = filteredConnectons.get(Configs.CONN_SERVICE_STATUS_ON);
                    List<Connection> serviceOffConnections = filteredConnectons.get(Configs.CONN_SERVICE_STATUS_OFF);
                    //如果服务状态为ON的连接数量大于阈值（默认是3）
                    if (serviceOnConnections.size() > CONNECTION_THRESHOLD) {
                        Connection freshSelectConnect = serviceOnConnections.get(random.nextInt(serviceOnConnections.size()));
                        freshSelectConnect.setAttribute(Configs.CONN_SERVICE_STATUS,Configs.CONN_SERVICE_STATUS_OFF);
                        Connection lastSelectConnect = freshSelectConnections.remove(poolKey);
                        freshSelectConnections.put(poolKey, freshSelectConnect);
                        closeFreshSelectConnections(lastSelectConnect, serviceOffConnections);

                    } else {
                        if (freshSelectConnections.containsKey(poolKey)) {
                            Connection lastSelectConnect = freshSelectConnections.remove(poolKey);
                            closeFreshSelectConnections(lastSelectConnect, serviceOffConnections);
                        }
                        if (logger.isInfoEnabled()) {
                            logger.info(
                                    "the size of serviceOnConnections [{}] reached CONNECTION_THRESHOLD [{}].",
                                    serviceOnConnections.size(), CONNECTION_THRESHOLD);
                        }
                    }

                    for (Connection offConn : serviceOffConnections) {
                        if (offConn.isFine()) {
                            offConn.close();
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("ScheduledDisconnectStrategy monitor error", e);
        }
    }

    /**
     * close the connection of the fresh select connections
     *
     * @param lastSelectConnect
     * @param serviceOffConnections
     * @throws InterruptedException
     */
    private void closeFreshSelectConnections(Connection lastSelectConnect,
                                             List<Connection> serviceOffConnections)
                                                                                    throws InterruptedException {
        if (null != lastSelectConnect) {
            if (lastSelectConnect.isInvokeFutureMapFinish()) {
                serviceOffConnections.add(lastSelectConnect);
            } else {
                Thread.sleep(RETRY_DETECT_PERIOD);
                if (lastSelectConnect.isInvokeFutureMapFinish()) {
                    serviceOffConnections.add(lastSelectConnect);
                } else {
                    if (logger.isInfoEnabled()) {
                        logger.info("Address={} won't close at this schedule turn",
                            RemotingUtil.parseRemoteAddress(lastSelectConnect.getChannel()));
                    }
                }
            }
        }
    }
}
