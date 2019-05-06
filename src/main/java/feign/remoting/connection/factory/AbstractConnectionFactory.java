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
package feign.remoting.connection.factory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import feign.remoting.ConfigurableInstance;
import feign.remoting.codec.factory.CodecFactory;
import feign.remoting.config.ConfigManager;
import feign.remoting.connection.Connection;
import feign.remoting.connection.Url;
import feign.remoting.connection.handler.ConnectionEventHandler;
import feign.remoting.enumerate.ConnectionEventType;
import feign.remoting.protocol.ProtocolCode;
import feign.remoting.protocol.RpcProtocolV2;
import feign.remoting.util.NamedThreadFactory;
import feign.remoting.util.NettyEventLoopUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

/**
 * 用于创建Connection对象的工厂
 */
public abstract class AbstractConnectionFactory implements ConnectionFactory {

    private static final Logger logger = LoggerFactory.getLogger(AbstractConnectionFactory.class);

    private static final EventLoopGroup workerGroup = NettyEventLoopUtil.newEventLoopGroup(Runtime
                                                        .getRuntime().availableProcessors() + 1,
                                                        new NamedThreadFactory(
                                                            "bolt-netty-client-worker", true));

    private final ConfigurableInstance  	confInstance;
    private final CodecFactory          codecFactory;
    private final ChannelHandler        heartbeatHandler;
    private final ChannelHandler        dispatcher;

    protected Bootstrap                 bootstrap;

    public AbstractConnectionFactory(CodecFactory codecFactory, ChannelHandler heartbeatHandler,
                                     ChannelHandler dispatcher, ConfigurableInstance confInstance) {
        if (codecFactory == null) {
            throw new IllegalArgumentException("null codecFactory");
        }
        if (dispatcher == null) {
            throw new IllegalArgumentException("null dispatcher handler");
        }

        this.confInstance = confInstance;
        this.codecFactory = codecFactory;
        this.heartbeatHandler = heartbeatHandler;
        this.dispatcher = dispatcher;
    }

    //初始化Netty Client
    @Override
    public void init(final ConnectionEventHandler connectionEventHandler) {
        bootstrap = new Bootstrap();
        bootstrap.group(workerGroup).channel(NettyEventLoopUtil.getClientSocketChannelClass())
            .option(ChannelOption.TCP_NODELAY, ConfigManager.tcp_nodelay())
            .option(ChannelOption.SO_REUSEADDR, ConfigManager.tcp_so_reuseaddr())
            .option(ChannelOption.SO_KEEPALIVE, ConfigManager.tcp_so_keepalive());

        // init netty write buffer water mark
        // TODO: 暂时注释
        //initWriteBufferWaterMark();

        //设置ByteBuf的分配模式，支持池化的分配和非池化分配
        if (ConfigManager.netty_buffer_pooled()) {
            this.bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        } else {
            this.bootstrap.option(ChannelOption.ALLOCATOR, UnpooledByteBufAllocator.DEFAULT);
        }

        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel channel) {
                ChannelPipeline pipeline = channel.pipeline();
                pipeline.addLast("decoder", codecFactory.newDecoder());
                pipeline.addLast("encoder", codecFactory.newEncoder());
                //获取是否开启Channel空闲检测开关，默认开启
                boolean idleSwitch = ConfigManager.tcp_idle_switch();
                //获取客户端Channel空闲状态时间，默认15000ms
                int idleTime = ConfigManager.tcp_idle();
                if (idleSwitch) {
                	//如果Channel的读、写空闲时间超过idelTime，那么会触发IdleStatEvent
                    pipeline.addLast("idleStateHandler", new IdleStateHandler(idleTime, idleTime, 0, TimeUnit.MILLISECONDS));
                    pipeline.addLast("heartbeatHandler", heartbeatHandler);
                }
                //添加连接事件处理器
                pipeline.addLast("connectionEventHandler", connectionEventHandler);
                //添加通用事件转发器
                pipeline.addLast("dispatcherHandler", dispatcher);
            }
        });
    }

    @Override
    public Connection createConnection(Url url) throws Exception {
    	//通过Netty的BootStrap，连接到URL所指定的地址，并获取连接的Channel
        Channel channel = doCreateConnection(url.getIp(), url.getPort(), url.getConnectTimeout());
        //将Netty的Channel对象包装成Connection对象
        Connection conn = new Connection(channel, ProtocolCode.fromBytes(url.getProtocol()),url.getVersion(), url);
        //往Pipline中发送一个ConnectionEventType.CONNECT的消息
        channel.pipeline().fireUserEventTriggered(ConnectionEventType.CONNECT);
        return conn;
    }

    @Override
    public Connection createConnection(String targetIP, int targetPort, int connectTimeout)throws Exception {
    	//通过Netty的BootStrap，连接到URL所指定的地址，并获取连接的Channel
        Channel channel = doCreateConnection(targetIP, targetPort, connectTimeout);
        //将Netty的Channel对象包装成Connection对象
        Connection conn = new Connection(channel,
            ProtocolCode.fromBytes(RpcProtocolV2.PROTOCOL_CODE), RpcProtocolV2.PROTOCOL_VERSION_1,
            new Url(targetIP, targetPort));
        //往Pipline中发送一个ConnectionEventType.CONNECT的消息
        channel.pipeline().fireUserEventTriggered(ConnectionEventType.CONNECT);
        return conn;
    }

    @Override
    public Connection createConnection(String targetIP, int targetPort, byte version,int connectTimeout) throws Exception {
    	//通过Netty的BootStrap，连接到URL所指定的地址，并获取连接的Channel
        Channel channel = doCreateConnection(targetIP, targetPort, connectTimeout);
        //将Netty的Channel对象包装成Connection对象
        Connection conn = new Connection(channel,
            ProtocolCode.fromBytes(RpcProtocolV2.PROTOCOL_CODE), version, new Url(targetIP,targetPort));
        //往Pipline中发送一个ConnectionEventType.CONNECT的消息
        channel.pipeline().fireUserEventTriggered(ConnectionEventType.CONNECT);
        return conn;
    }

    /**
     * init netty write buffer water mark
     */
//    private void initWriteBufferWaterMark() {
//        int lowWaterMark = this.confInstance.netty_buffer_low_watermark();
//        int highWaterMark = this.confInstance.netty_buffer_high_watermark();
//        if (lowWaterMark > highWaterMark) {
//            throw new IllegalArgumentException(
//                String
//                    .format(
//                        "[client side] bolt netty high water mark {%s} should not be smaller than low water mark {%s} bytes)",
//                        highWaterMark, lowWaterMark));
//        } else {
//            logger.warn(
//                "[client side] bolt netty low water mark is {} bytes, high water mark is {} bytes",
//                lowWaterMark, highWaterMark);
//        }
//        this.bootstrap.option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(
//            lowWaterMark, highWaterMark));
//    }
    
    //根据目标IP、目标端口以及连接超时时间，创建Netty的Channel对象
    protected Channel doCreateConnection(String targetIP, int targetPort, int connectTimeout)throws Exception {
        // prevent unreasonable value, at least 1000
    	//连接超时最大为1000ms
        connectTimeout = Math.max(connectTimeout, 1000);
        String address = targetIP + ":" + targetPort;
        if (logger.isDebugEnabled()) {
            logger.debug("connectTimeout of address [{}] is [{}].", address, connectTimeout);
        }
        //设置连接的超时时间
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout);
        //发起连接请求
        ChannelFuture future = bootstrap.connect(new InetSocketAddress(targetIP, targetPort));
        //等待连接，这句话比较重要，如果没有，则后面的this.ChannelFuture.isDone()为false的。  
        //原因在于：connect方法是异步的，awaitUninterruptibly方式阻塞主线程，等待服务端返回，从而实现同步  
        //如果不用awaitUninterruptibly，可以采用this.ChannelFuture.addListener方法进行回调  
        future.awaitUninterruptibly();
        //如果是Channel连接超时，那么isDone会返回False
        if (!future.isDone()) {
            String errMsg = "Create connection to " + address + " timeout!";
            logger.warn(errMsg);
            throw new Exception(errMsg);
        }
        if (future.isCancelled()) {
            String errMsg = "Create connection to " + address + " cancelled by user!";
            logger.warn(errMsg);
            throw new Exception(errMsg);
        }
        if (!future.isSuccess()) {
            String errMsg = "Create connection to " + address + " error!";
            logger.warn(errMsg);
            throw new Exception(errMsg, future.cause());
        }
        return future.channel();
    }
}
