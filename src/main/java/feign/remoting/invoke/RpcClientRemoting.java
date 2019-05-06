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
package feign.remoting.invoke;

import feign.remoting.command.RemotingCommand;
import feign.remoting.command.factory.CommandFactory;
import feign.remoting.connection.Connection;
import feign.remoting.connection.Url;
import feign.remoting.connection.manager.DefaultConnectionManager;
import feign.remoting.exception.RemotingException;
import feign.remoting.future.RpcResponseFuture;
import feign.remoting.util.RemotingAddressParser;
import feign.remoting.util.RemotingUtil;

/**
 * Rpc client remoting
 */
public class RpcClientRemoting extends RpcRemoting {

    public RpcClientRemoting(CommandFactory commandFactory, RemotingAddressParser addressParser,
                             DefaultConnectionManager connectionManager) {
        super(commandFactory, addressParser, connectionManager);
    }

    
    @Override
    public void oneway(Url url, Object request, InvokeContext invokeContext)
                                                                            throws RemotingException,
                                                                            InterruptedException {
    	//根据URL获取Connection对象，并初始化invokeContext
        final Connection conn = getConnectionAndInitInvokeContext(url, invokeContext);
        //使用connectionManager校验Connection对象是否可用，
        //如果为null、isNotActive、isNotWritable，那么抛出异常
        this.connectionManager.check(conn);
        //调用RpcRemoting的(Connection conn,Object request,InvokeContext invokeContext)接口
        this.oneway(conn, request, invokeContext);
    }

    @Override
    public Object invokeSync(Url url, Object request, InvokeContext invokeContext, int timeoutMillis)
                                                                                                     throws RemotingException,
                                                                                                     InterruptedException {
    	//======================
    	//创建连接的核心代码
    	//======================
    	//根据URL获取Connection对象，并初始化invokeContext
    	final Connection conn = getConnectionAndInitInvokeContext(url, invokeContext);
    	
        //使用connectionManager校验Connection对象是否可用，
        //如果为null、isNotActive、isNotWritable，那么抛出异常
        this.connectionManager.check(conn);
        //调用RpcRemoting的(Connection conn,Object request,InvokeContext invokeContext,int timeoutMillis)接口
        return this.invokeSync(conn, request, invokeContext, timeoutMillis);
    }

    @Override
    public RpcResponseFuture invokeWithFuture(Url url, Object request, InvokeContext invokeContext,
                                              int timeoutMillis) throws RemotingException,
                                                                InterruptedException {
        final Connection conn = getConnectionAndInitInvokeContext(url, invokeContext);
        this.connectionManager.check(conn);
        return this.invokeWithFuture(conn, request, invokeContext, timeoutMillis);
    }

    @Override
    public void invokeWithCallback(Url url, Object request, InvokeContext invokeContext,
                                   InvokeCallback invokeCallback, int timeoutMillis)
                                                                                    throws RemotingException,
                                                                                    InterruptedException {
        final Connection conn = getConnectionAndInitInvokeContext(url, invokeContext);
        this.connectionManager.check(conn);
        this.invokeWithCallback(conn, request, invokeContext, invokeCallback, timeoutMillis);
    }

    //InvokeContext的预处理
    @Override
    protected void preProcessInvokeContext(InvokeContext invokeContext, RemotingCommand cmd,
                                           Connection connection) {
    	
    	//如果invokeContext不为空的话，使用Connection中的Channel，进行invokeContext的预处理
        if (null != invokeContext) {
        	//设置bolt.client.local.ip为channel.localAddress().getAddress().getHostAddress()
            invokeContext.putIfAbsent(InvokeContext.CLIENT_LOCAL_IP,
                RemotingUtil.parseLocalIP(connection.getChannel()));
            //设置bolt.client.local.port为channel.localAddress().getPort()
            invokeContext.putIfAbsent(InvokeContext.CLIENT_LOCAL_PORT,
                RemotingUtil.parseLocalPort(connection.getChannel()));
            
            //设置bolt.client.remote.ip为channel.remoteAddress().getAddress().getHostAddress()
            invokeContext.putIfAbsent(InvokeContext.CLIENT_REMOTE_IP,
                RemotingUtil.parseRemoteIP(connection.getChannel()));
            //设置bolt.client.remote.port为channel.remoteAddress().getAddress().getHostAddress()
            invokeContext.putIfAbsent(InvokeContext.CLIENT_REMOTE_PORT,
                RemotingUtil.parseRemotePort(connection.getChannel()));
            
            //设置bolt.invoke.request.id为RemotingCommand.getID
            invokeContext.putIfAbsent(InvokeContext.BOLT_INVOKE_REQUEST_ID, cmd.getId());
        }
    }

  
    protected Connection getConnectionAndInitInvokeContext(Url url, InvokeContext invokeContext)
                                                                                                throws RemotingException,
                                                                                                InterruptedException {
        long start = System.currentTimeMillis();
        Connection conn;
        try {
        	//===============================================
        	//通过ConnectionManager创建Connection，主要工作包括：
        	//===============================================
        	//
            conn = this.connectionManager.getAndCreateIfAbsent(url);
        } finally {
            if (null != invokeContext) {
            	//在InvokeContext中设置创建Connection花费的时间
                invokeContext.putIfAbsent(InvokeContext.CLIENT_CONN_CREATETIME,(System.currentTimeMillis() - start));
            }
        }
        return conn;
    }
}
