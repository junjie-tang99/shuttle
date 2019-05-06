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
package feign.remoting.command.handler;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import feign.remoting.RemotingContext;
import feign.remoting.command.RemotingCommand;
import feign.remoting.command.RequestCommand;
import feign.remoting.command.ResponseCommand;
import feign.remoting.command.RpcCommand;
import feign.remoting.command.factory.CommandFactory;
import feign.remoting.config.RpcConfigManager;
import feign.remoting.enumerate.CommandCode;
import feign.remoting.enumerate.ResponseStatus;
import feign.remoting.enumerate.RpcCommandType;
import feign.remoting.processor.AbstractRemotingProcessor;
import feign.remoting.processor.ProcessorManager;
import feign.remoting.processor.RemotingProcessor;
import feign.remoting.processor.RpcHeartBeatProcessor;
import feign.remoting.processor.RpcRequestProcessor;
import feign.remoting.processor.RpcResponseProcessor;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;

/**
 * Rpc command handler.
 * 
 * @author jiangping
 * @version $Id: RpcServerHandler.java, v 0.1 2015-8-31 PM7:43:06 tao Exp $
 */
@Sharable
public class RpcCommandHandler implements CommandHandler {

    private static final Logger logger = LoggerFactory.getLogger("RpcCommandHandler");
    /** All processors */
    ProcessorManager            processorManager;

    CommandFactory              commandFactory;

    /**
     * Constructor. Initialize the processor manager and register processors.
     */
    public RpcCommandHandler(CommandFactory commandFactory) {
        this.commandFactory = commandFactory;
        //创建ProcessorManager，主要通过CommandCode，来注册对应Command的处理器
        this.processorManager = new ProcessorManager();
        //注册Request的Processor
        this.processorManager.registerProcessor(CommandCode.RPC_REQUEST,
            new RpcRequestProcessor(this.commandFactory));
        //注册Response的Processor
        this.processorManager.registerProcessor(CommandCode.RPC_RESPONSE,
            new RpcResponseProcessor());
        //注册Heartbeat的Processor
        this.processorManager.registerProcessor(CommandCode.HEARTBEAT,
            new RpcHeartBeatProcessor());
        //注册默认的处理器，主要是将没有对应Processor的Command打印出来
        this.processorManager
            .registerDefaultProcessor(new AbstractRemotingProcessor<RemotingCommand>() {
                @Override
                public void doProcess(RemotingContext ctx, RemotingCommand msg) throws Exception {
                    logger.error("No processor available for command code {}, msgId {}",
                        msg.getCmdCode(), msg.getId());
                }
            });
    }

    /**
     * @see CommandHandler#handleCommand(RemotingContext, Object)
     */
    @Override
    public void handleCommand(RemotingContext ctx, Object msg) throws Exception {
        this.handle(ctx, msg);
    }

    //处理Command
    private void handle(final RemotingContext ctx, final Object msg) {
        try {
        	//如果待处理的Netty Message是一个List，即：已经通过批量解析
            if (msg instanceof List) {
            	//创建一个解析任务
                final Runnable handleTask = new Runnable() {
                    @Override
                    public void run() {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Batch message! size={}", ((List<?>) msg).size());
                        }
                        //遍历Batch List，并执行Command的处理
                        for (final Object m : (List<?>) msg) {
                        	//执行Process函数
                            RpcCommandHandler.this.process(ctx, m);
                        }
                    }
                };
                //判断Batch List的解析任务，是放在默认的线程池中执行，默认值：是
                if (RpcConfigManager.dispatch_msg_list_in_default_executor()) {
                    // If msg is list ,then the batch submission to biz threadpool can save io thread.
                    // See com.alipay.remoting.decoder.ProtocolDecoder
                    processorManager.getDefaultExecutor().execute(handleTask);
                } else {
                	//如果不放在线程池中执行，则在当前线程支持
                    handleTask.run();
                }
            } else {
            	//执行Process函数
                process(ctx, msg);
            }
        } catch (final Throwable t) {
            processException(ctx, msg, t);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void process(RemotingContext ctx, Object msg) {
        try {
        	//将Netty Message转换为RpcCommand
            final RpcCommand cmd = (RpcCommand) msg;
            //根据CommandCode，找到对应的Processor
            final RemotingProcessor processor = processorManager.getProcessor(cmd.getCmdCode());
            //Process执行处理
            processor.process(ctx, cmd, processorManager.getDefaultExecutor());
        } catch (final Throwable t) {
            processException(ctx, msg, t);
        }
    }

    private void processException(RemotingContext ctx, Object msg, Throwable t) {
        if (msg instanceof List) {
            for (final Object m : (List<?>) msg) {
                processExceptionForSingleCommand(ctx, m, t);
            }
        } else {
            processExceptionForSingleCommand(ctx, msg, t);
        }
    }

    /*
     * Return error command if necessary.
     */
    private void processExceptionForSingleCommand(RemotingContext ctx, Object msg, Throwable t) {
        final int id = ((RpcCommand) msg).getId();
        final String emsg = "Exception caught when processing "
                            + ((msg instanceof RequestCommand) ? "request, id=" : "response, id=");
        logger.warn(emsg + id, t);
        if (msg instanceof RequestCommand) {
            final RequestCommand cmd = (RequestCommand) msg;
            if (cmd.getType() != RpcCommandType.REQUEST_ONEWAY.value()) {
                if (t instanceof RejectedExecutionException) {
                    final ResponseCommand response = this.commandFactory.createExceptionResponse(
                        id, ResponseStatus.SERVER_THREADPOOL_BUSY);
                    // RejectedExecutionException here assures no response has been sent back
                    // Other exceptions should be processed where exception was caught, because here we don't known whether ack had been sent back.
                    ctx.getChannelContext().writeAndFlush(response)
                        .addListener(new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture future) throws Exception {
                                if (future.isSuccess()) {
                                    if (logger.isInfoEnabled()) {
                                        logger
                                            .info(
                                                "Write back exception response done, requestId={}, status={}",
                                                id, response.getResponseStatus());
                                    }
                                } else {
                                    logger.error(
                                        "Write back exception response failed, requestId={}", id,
                                        future.cause());
                                }
                            }

                        });
                }
            }
        }
    }

    /**
     * @see CommandHandler#registerProcessor(com.alipay.remoting.CommandCode, RemotingProcessor)
     */
    @Override
    public void registerProcessor(CommandCode cmd,
                                  @SuppressWarnings("rawtypes") RemotingProcessor processor) {
        this.processorManager.registerProcessor(cmd, processor);
    }

    /**
     * @see CommandHandler#registerDefaultExecutor(java.util.concurrent.ExecutorService)
     */
    @Override
    public void registerDefaultExecutor(ExecutorService executor) {
        this.processorManager.registerDefaultExecutor(executor);
    }

    /**
     * @see CommandHandler#getDefaultExecutor()
     */
    @Override
    public ExecutorService getDefaultExecutor() {
        return this.processorManager.getDefaultExecutor();
    }
}
