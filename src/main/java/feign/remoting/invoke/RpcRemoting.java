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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import feign.remoting.command.RemotingCommand;
import feign.remoting.command.RequestCommand;
import feign.remoting.command.ResponseCommand;
import feign.remoting.command.RpcRequestCommand;
import feign.remoting.command.factory.CommandFactory;
import feign.remoting.connection.Connection;
import feign.remoting.connection.Url;
import feign.remoting.connection.manager.DefaultConnectionManager;
import feign.remoting.enumerate.RpcCommandType;
import feign.remoting.exception.RemotingException;
import feign.remoting.exception.SerializationException;
import feign.remoting.future.RpcResponseFuture;
import feign.remoting.future.RpcResponseResolver;
import feign.remoting.protocol.RpcProtocolManager;
import feign.remoting.switches.ProtocolSwitch;
import feign.remoting.util.RemotingAddressParser;
import feign.remoting.util.RemotingUtil;


/**
 * Rpc remoting capability.
 * 
 * @author jiangping
 * @version $Id: RpcRemoting.java, v 0.1 Mar 6, 2016 9:09:48 PM tao Exp $
 */
public abstract class RpcRemoting extends BaseRemoting {
    static {
        RpcProtocolManager.initProtocols();
    }
    /** logger */
    private static final Logger logger = LoggerFactory.getLogger("RpcRemoting");

    /** address parser to get custom args */
    public RemotingAddressParser    addressParser;

    /** connection manager */
    public DefaultConnectionManager connectionManager;

    /**
     * default constructor
     */
    public RpcRemoting(CommandFactory commandFactory) {
        super(commandFactory);
    }

    /**
     * @param addressParser
     * @param connectionManager
     */
    public RpcRemoting(CommandFactory commandFactory, RemotingAddressParser addressParser,
                       DefaultConnectionManager connectionManager) {
        this(commandFactory);
        this.addressParser = addressParser;
        this.connectionManager = connectionManager;
    }


    public void oneway(final String addr, final Object request, final InvokeContext invokeContext)
                                                                                                  throws RemotingException,
                                                                                                  InterruptedException {
        Url url = this.addressParser.parse(addr);
        //根据URL执行Remoting方法
        this.oneway(url, request, invokeContext);
    }

    public abstract void oneway(final Url url, final Object request,
                                final InvokeContext invokeContext) throws RemotingException,
                                                                  InterruptedException;

    public void oneway(final Connection conn, final Object request,
                       final InvokeContext invokeContext) throws RemotingException {
        RequestCommand requestCommand = (RequestCommand) toRemotingCommand(request, conn, invokeContext, -1);
        requestCommand.setType(RpcCommandType.REQUEST_ONEWAY.value());
        //将Connection中的CLIENT_LOCAL_IP、CLIENT_LOCAL_PORT、CLIENT_REMOTE_IP、
        //CLIENT_REMOTE_PORT、BOLT_INVOKE_REQUEST_ID，设置到invokeContext对象中
        preProcessInvokeContext(invokeContext, requestCommand, conn);
        //调用BaseRemoting.oneway()方法
        super.oneway(conn, requestCommand);
    }


    //同步的RPC调用（使用连接地址直接调用）
    public Object invokeSync(final String addr, final Object request,
                             final InvokeContext invokeContext, final int timeoutMillis)
                                                                                        throws RemotingException,
                                                                                        InterruptedException {
        //将调用地址转换为URL对象
    	Url url = this.addressParser.parse(addr);
        return this.invokeSync(url, request, invokeContext, timeoutMillis);
    }

    //同步的RPC调用（使用连接地址直接调用），具体实现由子类提供
    public abstract Object invokeSync(final Url url, final Object request,
                                      final InvokeContext invokeContext, final int timeoutMillis)
                                                                                                 throws RemotingException,
                                                                                                 InterruptedException;

    //同步的RPC调用
    public Object invokeSync(final Connection conn, final Object request,
                             final InvokeContext invokeContext, final int timeoutMillis)
                                                                                        throws RemotingException,
                                                                                        InterruptedException {
    	//使用Connection、Request、InvokeContext、timeoutMillis参数，构造RpcRequestCommand对象
        RemotingCommand requestCommand = toRemotingCommand(request, conn, invokeContext, timeoutMillis);
        
        //将Connection中的CLIENT_LOCAL_IP、CLIENT_LOCAL_PORT、CLIENT_REMOTE_IP、
        //CLIENT_REMOTE_PORT、BOLT_INVOKE_REQUEST_ID，设置到invokeContext对象中
        preProcessInvokeContext(invokeContext, requestCommand, conn);
        
        //调用BaseRemoting.invokeSync方法进行同步调用，并获取ResponseCommand
        ResponseCommand responseCommand = (ResponseCommand) super.invokeSync(conn, requestCommand, timeoutMillis);
        
        //将invokeContext设置到responseCommand中
        responseCommand.setInvokeContext(invokeContext);

        //将responseCommand转换为ResponseObject
        Object responseObject = RpcResponseResolver.resolveResponseObject(responseCommand,
        		RemotingUtil.parseRemoteAddress(conn.getChannel()));
        
        return responseObject;
    }

    /**
     * Rpc invocation with future returned.<br>
     * Notice! DO NOT modify the request object concurrently when this method is called.
     * 
     * @param addr
     * @param request
     * @param invokeContext 
     * @param timeoutMillis
     * @return
     * @throws RemotingException
     * @throws InterruptedException
     */
    public RpcResponseFuture invokeWithFuture(final String addr, final Object request,
                                              final InvokeContext invokeContext, int timeoutMillis)
                                                                                                   throws RemotingException,
                                                                                                   InterruptedException {
        Url url = this.addressParser.parse(addr);
        return this.invokeWithFuture(url, request, invokeContext, timeoutMillis);
    }

    /**
     * Rpc invocation with future returned.<br>
     * Notice! DO NOT modify the request object concurrently when this method is called.
     * 
     * @param url
     * @param request
     * @param invokeContext
     * @param timeoutMillis
     * @return
     * @throws RemotingException
     * @throws InterruptedException
     */
    public abstract RpcResponseFuture invokeWithFuture(final Url url, final Object request,
                                                       final InvokeContext invokeContext,
                                                       final int timeoutMillis)
                                                                               throws RemotingException,
                                                                               InterruptedException;

    /**
     * Rpc invocation with future returned.<br>
     * Notice! DO NOT modify the request object concurrently when this method is called.
     * 
     * @param conn
     * @param request
     * @param invokeContext
     * @param timeoutMillis
     * @return
     * @throws RemotingException
     */
    public RpcResponseFuture invokeWithFuture(final Connection conn, final Object request,
                                              final InvokeContext invokeContext,
                                              final int timeoutMillis) throws RemotingException {

        RemotingCommand requestCommand = toRemotingCommand(request, conn, invokeContext,
            timeoutMillis);

        preProcessInvokeContext(invokeContext, requestCommand, conn);
        InvokeFuture future = super.invokeWithFuture(conn, requestCommand, timeoutMillis);
        return new RpcResponseFuture(RemotingUtil.parseRemoteAddress(conn.getChannel()), future);
    }

    /**
     * Rpc invocation with callback.<br>
     * Notice! DO NOT modify the request object concurrently when this method is called.
     * 
     * @param addr
     * @param request
     * @param invokeContext 
     * @param invokeCallback
     * @param timeoutMillis
     * @throws RemotingException
     * @throws InterruptedException
     */
    public void invokeWithCallback(String addr, Object request, final InvokeContext invokeContext,
                                   InvokeCallback invokeCallback, int timeoutMillis)
                                                                                    throws RemotingException,
                                                                                    InterruptedException {
        Url url = this.addressParser.parse(addr);
        this.invokeWithCallback(url, request, invokeContext, invokeCallback, timeoutMillis);
    }

    /**
     * Rpc invocation with callback.<br>
     * Notice! DO NOT modify the request object concurrently when this method is called.
     * 
     * @param url
     * @param request
     * @param invokeContext
     * @param invokeCallback
     * @param timeoutMillis
     * @throws RemotingException
     * @throws InterruptedException
     */
    public abstract void invokeWithCallback(final Url url, final Object request,
                                            final InvokeContext invokeContext,
                                            final InvokeCallback invokeCallback,
                                            final int timeoutMillis) throws RemotingException,
                                                                    InterruptedException;

    /**
     * Rpc invocation with callback.<br>
     * Notice! DO NOT modify the request object concurrently when this method is called.
     * 
     * @param conn
     * @param request
     * @param invokeContext 
     * @param invokeCallback
     * @param timeoutMillis
     * @throws RemotingException
     */
    public void invokeWithCallback(final Connection conn, final Object request,
                                   final InvokeContext invokeContext,
                                   final InvokeCallback invokeCallback, final int timeoutMillis)
                                                                                                throws RemotingException {
        RemotingCommand requestCommand = toRemotingCommand(request, conn, invokeContext,
            timeoutMillis);
        preProcessInvokeContext(invokeContext, requestCommand, conn);
        super.invokeWithCallback(conn, requestCommand, invokeCallback, timeoutMillis);
    }

    
    //将应用程序的请求对象，包装成RpcRequestCommand对象
    protected RemotingCommand toRemotingCommand(Object request, Connection conn,
                                                InvokeContext invokeContext, int timeoutMillis)
                                                                                               throws SerializationException {
        //使用RpcCommandFactory，并根据RequestObject构建RpcCommandRequest
    	RpcRequestCommand command = this.getCommandFactory().createRequestCommand(request);
    	
    	//如果InvokeContext对象不为Null
        if (null != invokeContext) {
        	//尝试从invokeContext中获取request command的自定义序列化器
            Object clientCustomSerializer = invokeContext.get(InvokeContext.BOLT_CUSTOM_SERIALIZER);
            if (null != clientCustomSerializer) {
                try {
                	//如果自定义序列化器不为null，则将序列化器设置到RpcRequestCommand的Serializer上
                    command.setSerializer((Byte) clientCustomSerializer);
                } catch (ClassCastException e) {
                    throw new IllegalArgumentException(
                        "Illegal custom serializer [" + clientCustomSerializer
                                + "], the type of value should be [byte], but now is ["
                                + clientCustomSerializer.getClass().getName() + "].");
                }
            }

            // enable crc by default, user can disable by set invoke context `false` for key `InvokeContext.BOLT_CRC_SWITCH`
            Boolean crcSwitch = invokeContext.get(InvokeContext.BOLT_CRC_SWITCH, ProtocolSwitch.CRC_SWITCH_DEFAULT_VALUE);
            //如果InvokeContext中的BOLT_CRC_SWITCH为True，那么开启crc验证码，并将ProtocolSwitch设置到RpcRequestCommand对象上去
            if (null != crcSwitch && crcSwitch) {
                command.setProtocolSwitch(ProtocolSwitch .create(new int[] { ProtocolSwitch.CRC_SWITCH_INDEX }));
            }
        } else {
        	//如果InvokeContext对象为NUll
        	//默认启用crc验证码，并生成对应的ProtocolSwitch，并设置到RpcRequestCommand对象上去
            command.setProtocolSwitch(ProtocolSwitch.create(new int[] { ProtocolSwitch.CRC_SWITCH_INDEX }));
        }
        
        //设置请求的超时时间
        command.setTimeout(timeoutMillis);
        //设置请求的类名
        command.setRequestClass(request.getClass().getName());
        //设置invokeContext
        command.setInvokeContext(invokeContext);
        //请求序列化
        command.serialize();
        
        logDebugInfo(command);
        return command;
    }

    protected abstract void preProcessInvokeContext(InvokeContext invokeContext,
                                                    RemotingCommand cmd, Connection connection);

    /**
     * @param requestCommand
     */
    private void logDebugInfo(RemotingCommand requestCommand) {
        if (logger.isDebugEnabled()) {
            logger.debug("Send request, requestId=" + requestCommand.getId());
        }
    }

    /**
     * @see com.alipay.remoting.BaseRemoting#createInvokeFuture(com.alipay.remoting.RemotingCommand, com.alipay.remoting.InvokeContext)
     */
    @Override
    protected InvokeFuture createInvokeFuture(RemotingCommand request, InvokeContext invokeContext) {
        return new DefaultInvokeFuture(request.getId(), null, null, request.getProtocolCode()
            .getFirstByte(), this.getCommandFactory(), invokeContext);
    }

    /**
     * @see com.alipay.remoting.BaseRemoting#createInvokeFuture(Connection, RemotingCommand, InvokeContext, InvokeCallback)
     */
    @Override
    protected InvokeFuture createInvokeFuture(Connection conn, RemotingCommand request,
                                              InvokeContext invokeContext,
                                              InvokeCallback invokeCallback) {
        return new DefaultInvokeFuture(request.getId(), new RpcInvokeCallbackListener(
            RemotingUtil.parseRemoteAddress(conn.getChannel())), invokeCallback, request
            .getProtocolCode().getFirstByte(), this.getCommandFactory(), invokeContext);
    }
}
