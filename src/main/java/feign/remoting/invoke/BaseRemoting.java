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

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import feign.remoting.command.RemotingCommand;
import feign.remoting.command.factory.CommandFactory;
import feign.remoting.connection.Connection;
import feign.remoting.exception.RemotingException;
import feign.remoting.util.RemotingUtil;
import feign.remoting.util.TimerHolder;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;

/**
 * Base remoting capability.
 */
public abstract class BaseRemoting {
    /** logger */
    private static final Logger logger = LoggerFactory.getLogger("CommonDefault");

    protected CommandFactory    commandFactory;

    public BaseRemoting(CommandFactory commandFactory) {
        this.commandFactory = commandFactory;
    }

    
    //单向调用
    protected void oneway(final Connection conn, final RemotingCommand request) {
        try {
        	//往Connection的Channel中写入RPCRequestCommand
            conn.getChannel().writeAndFlush(request).addListener(new ChannelFutureListener() {
            	//增加写完之后的Listener
                @Override
                public void operationComplete(ChannelFuture f) throws Exception {
                	//如果操作失败
                    if (!f.isSuccess()) {
                        logger.error("Invoke send failed. The address is {}",
                            RemotingUtil.parseRemoteAddress(conn.getChannel()), f.cause());
                    }
                }

            });
        } catch (Exception e) {
            if (null == conn) {
                logger.error("Conn is null");
            } else {
                logger.error("Exception caught when sending invocation. The address is {}",
                    RemotingUtil.parseRemoteAddress(conn.getChannel()), e);
            }
        }
    }
    
    //同步调用
    protected RemotingCommand invokeSync(final Connection conn, final RemotingCommand request,
                                         final int timeoutMillis) throws RemotingException,
                                                                 InterruptedException {
    	
    	//创建DefaultInvokeFuture对象
    	//=======================================
    	//int invokeId, 
    	//InvokeCallbackListener callbackListener,
    	//InvokeCallback callback, 
    	//byte protocol,
    	//CommandFactory commandFactory
    	//InvokeContext invokeContext
    	//=======================================
    	//实际参数为：(request.getId(), null, null, request.getProtocolCode().getFirstByte(), this.getCommandFactory(), invokeContext);
        final InvokeFuture future = createInvokeFuture(request, request.getInvokeContext());
        
        //将InvokeFuture设置到Connection上
        conn.addInvokeFuture(future);
        
        final int requestId = request.getId();
        try {
            conn.getChannel().writeAndFlush(request).addListener(new ChannelFutureListener() {
            	//增加channel中写完之后的Listener
                @Override
                public void operationComplete(ChannelFuture f) throws Exception {
                	//如果操作失败
                    if (!f.isSuccess()) {
                    	//根据requestId，将channelFuture从Connection的invokeFutureMap中删除
                        conn.removeInvokeFuture(requestId);
                        //将DefaultInvokeFuture中的ResponseCommand设置为CLIENT_SEND_ERROR错误
                        //在执行putResponse方法时，会执行CountDownLatch.countDown方法
                        future.putResponse(commandFactory.createSendFailedResponse(conn.getRemoteAddress(), f.cause()));
                        logger.error("Invoke send failed, id={}", requestId, f.cause());
                    }
                }

            });
        } catch (Exception e) {
        	//如果出现异常
            conn.removeInvokeFuture(requestId);
            //将DefaultInvokeFuture中的ResponseCommand设置为CLIENT_SEND_ERROR错误
            //在执行putResponse方法时，会执行CountDownLatch.countDown方法
            future.putResponse(commandFactory.createSendFailedResponse(conn.getRemoteAddress(), e));
            logger.error("Exception caught when sending invocation, id={}", requestId, e);
        }
        
        //调用CountDownLatch.await(timeoutMillis)方法进行等待
        RemotingCommand response = future.waitResponse(timeoutMillis);
        
        //如果获取不到返回值，则创建超时的ResponseCommand
        if (response == null) {
            conn.removeInvokeFuture(requestId);
            response = this.commandFactory.createTimeoutResponse(conn.getRemoteAddress());
            logger.warn("Wait response, request id={} timeout!", requestId);
        }

        return response;
    }

    /**
     * Invocation with callback.
     * 
     * @param conn
     * @param request
     * @param invokeCallback
     * @param timeoutMillis
     * @throws InterruptedException
     */
    protected void invokeWithCallback(final Connection conn, final RemotingCommand request,
                                      final InvokeCallback invokeCallback, final int timeoutMillis) {
    	
    	
        final InvokeFuture future = createInvokeFuture(conn, request, request.getInvokeContext(), invokeCallback);
        conn.addInvokeFuture(future);
        final int requestId = request.getId();
        try {
            Timeout timeout = TimerHolder.getTimer().newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) throws Exception {
                    InvokeFuture future = conn.removeInvokeFuture(requestId);
                    if (future != null) {
                        future.putResponse(commandFactory.createTimeoutResponse(conn
                            .getRemoteAddress()));
                        future.tryAsyncExecuteInvokeCallbackAbnormally();
                    }
                }

            }, timeoutMillis, TimeUnit.MILLISECONDS);
            future.addTimeout(timeout);
            conn.getChannel().writeAndFlush(request).addListener(new ChannelFutureListener() {

                @Override
                public void operationComplete(ChannelFuture cf) throws Exception {
                    if (!cf.isSuccess()) {
                        InvokeFuture f = conn.removeInvokeFuture(requestId);
                        if (f != null) {
                            f.cancelTimeout();
                            f.putResponse(commandFactory.createSendFailedResponse(
                                conn.getRemoteAddress(), cf.cause()));
                            f.tryAsyncExecuteInvokeCallbackAbnormally();
                        }
                        logger.error("Invoke send failed. The address is {}",
                            RemotingUtil.parseRemoteAddress(conn.getChannel()), cf.cause());
                    }
                }

            });
        } catch (Exception e) {
            InvokeFuture f = conn.removeInvokeFuture(requestId);
            if (f != null) {
                f.cancelTimeout();
                f.putResponse(commandFactory.createSendFailedResponse(conn.getRemoteAddress(), e));
                f.tryAsyncExecuteInvokeCallbackAbnormally();
            }
            logger.error("Exception caught when sending invocation. The address is {}",
                RemotingUtil.parseRemoteAddress(conn.getChannel()), e);
        }
    }

    /**
     * Invocation with future returned.
     * 
     * @param conn
     * @param request
     * @param timeoutMillis
     * @return
     */
    protected InvokeFuture invokeWithFuture(final Connection conn, final RemotingCommand request,
                                            final int timeoutMillis) {

        final InvokeFuture future = createInvokeFuture(request, request.getInvokeContext());
        conn.addInvokeFuture(future);
        final int requestId = request.getId();
        try {
            Timeout timeout = TimerHolder.getTimer().newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) throws Exception {
                    InvokeFuture future = conn.removeInvokeFuture(requestId);
                    if (future != null) {
                        future.putResponse(commandFactory.createTimeoutResponse(conn
                            .getRemoteAddress()));
                    }
                }

            }, timeoutMillis, TimeUnit.MILLISECONDS);
            future.addTimeout(timeout);

            conn.getChannel().writeAndFlush(request).addListener(new ChannelFutureListener() {

                @Override
                public void operationComplete(ChannelFuture cf) throws Exception {
                    if (!cf.isSuccess()) {
                        InvokeFuture f = conn.removeInvokeFuture(requestId);
                        if (f != null) {
                            f.cancelTimeout();
                            f.putResponse(commandFactory.createSendFailedResponse(
                                conn.getRemoteAddress(), cf.cause()));
                        }
                        logger.error("Invoke send failed. The address is {}",
                            RemotingUtil.parseRemoteAddress(conn.getChannel()), cf.cause());
                    }
                }

            });
        } catch (Exception e) {
            InvokeFuture f = conn.removeInvokeFuture(requestId);
            if (f != null) {
                f.cancelTimeout();
                f.putResponse(commandFactory.createSendFailedResponse(conn.getRemoteAddress(), e));
            }
            logger.error("Exception caught when sending invocation. The address is {}",
                RemotingUtil.parseRemoteAddress(conn.getChannel()), e);
        }
        return future;
    }



    /**
     * Create invoke future with {@link InvokeContext}.
     * @param request
     * @param invokeContext
     * @return
     */
    protected abstract InvokeFuture createInvokeFuture(final RemotingCommand request,
                                                       final InvokeContext invokeContext);

    /**
     * Create invoke future with {@link InvokeContext}.
     * @param conn
     * @param request
     * @param invokeContext
     * @param invokeCallback
     * @return
     */
    protected abstract InvokeFuture createInvokeFuture(final Connection conn,
                                                       final RemotingCommand request,
                                                       final InvokeContext invokeContext,
                                                       final InvokeCallback invokeCallback);

    protected CommandFactory getCommandFactory() {
        return commandFactory;
    }
}
