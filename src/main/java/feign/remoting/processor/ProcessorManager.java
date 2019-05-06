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
package feign.remoting.processor;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import feign.remoting.config.ConfigManager;
import feign.remoting.enumerate.CommandCode;
import feign.remoting.util.NamedThreadFactory;


/**
 * Manager of processors<br>
 * Maintains the relationship between command and command processor through command code.
 * 
 * @author jiangping
 * @version $Id: ProcessorManager.java, v 0.1 Sept 6, 2015 2:49:47 PM tao Exp $
 */
public class ProcessorManager {
    private static final Logger                                  logger         = LoggerFactory.getLogger("CommonDefault");
    //保存Process的容器
    private ConcurrentHashMap<CommandCode, RemotingProcessor<?>> cmd2processors = new ConcurrentHashMap<CommandCode, RemotingProcessor<?>>(
                                                                                    4);
    //默认的CommandProcessor
    private RemotingProcessor<?> defaultProcessor;

    //Process执行的默认线程池，如果没有给processor指定线程池的话，那么就会用到这个线程池
    private ExecutorService  defaultExecutor;
    //线程池最小线程数，默认值是：20
    private int minPoolSize = ConfigManager.default_tp_min_size();
    //线程池最大线程数，默认值是：400
    private int maxPoolSize = ConfigManager.default_tp_max_size();
    //线程池队列大小，默认值是：600
    private int queueSize = ConfigManager.default_tp_queue_size();
    //线程池中线程的存活时间，默认值是：60秒
    private long keepAliveTime  = ConfigManager.default_tp_keepalive_time();

    public ProcessorManager() {
    	//初始化ProcessorManager时，就会创建对应的Executor
        defaultExecutor = new ThreadPoolExecutor(minPoolSize, maxPoolSize, keepAliveTime,
            TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(queueSize), new NamedThreadFactory(
                "Bolt-default-executor", true));
    }

    //根据CommandCode注册对应的Processor
    public void registerProcessor(CommandCode cmdCode, RemotingProcessor<?> processor) {
        if (this.cmd2processors.containsKey(cmdCode)) {
            logger
                .warn(
                    "Processor for cmd={} is already registered, the processor is {}, and changed to {}",
                    cmdCode, cmd2processors.get(cmdCode).getClass().getName(), processor.getClass()
                        .getName());
        }
        this.cmd2processors.put(cmdCode, processor);
    }

    //设置默认处理器
    public void registerDefaultProcessor(RemotingProcessor<?> processor) {
        if (this.defaultProcessor == null) {
            this.defaultProcessor = processor;
        } else {
            throw new IllegalStateException("The defaultProcessor has already been registered: "
                                            + this.defaultProcessor.getClass());
        }
    }

    //根据CommandCode获取对应的Process，如果无法获取对应的Processor，那么返回默认的Processor
    public RemotingProcessor<?> getProcessor(CommandCode cmdCode) {
        RemotingProcessor<?> processor = this.cmd2processors.get(cmdCode);
        if (processor != null) {
            return processor;
        }
        return this.defaultProcessor;
    }

    /**
     * Getter method for property <tt>defaultExecutor</tt>.
     * 
     * @return property value of defaultExecutor
     */
    public ExecutorService getDefaultExecutor() {
        return defaultExecutor;
    }

    /**
     * Set the default executor.
     * 
     * @param executor
     */
    public void registerDefaultExecutor(ExecutorService executor) {
        this.defaultExecutor = executor;
    }

}