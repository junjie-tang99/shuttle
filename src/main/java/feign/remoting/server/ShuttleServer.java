package feign.remoting.server;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import feign.remoting.codec.factory.CodecFactory;
import feign.remoting.codec.factory.RpcCodecFactory;
import feign.remoting.command.factory.RpcCommandFactory;
import feign.remoting.config.ConfigManager;
import feign.remoting.connection.Connection;
import feign.remoting.connection.Url;
import feign.remoting.connection.handler.ConnectionEventHandler;
import feign.remoting.connection.handler.RpcConnectionEventHandler;
import feign.remoting.connection.listener.ConnectionEventListener;
import feign.remoting.connection.manager.DefaultConnectionManager;
import feign.remoting.connection.processor.ConnectionEventProcessor;
import feign.remoting.connection.strategy.RandomSelectStrategy;
import feign.remoting.dispatcher.DispatcherHandler;
import feign.remoting.enumerate.CommandCode;
import feign.remoting.enumerate.ConnectionEventType;
import feign.remoting.exception.RemotingException;
import feign.remoting.idle.ServerIdleHandler;
import feign.remoting.invoke.InvokeContext;
import feign.remoting.invoke.RpcRemoting;
import feign.remoting.invoke.RpcServerRemoting;
import feign.remoting.processor.RemotingProcessor;
import feign.remoting.processor.UserProcessor;
import feign.remoting.processor.UserProcessorRegisterHelper;
import feign.remoting.protocol.ProtocolCode;
import feign.remoting.protocol.ProtocolManager;
import feign.remoting.switches.GlobalSwitch;
import feign.remoting.util.NamedThreadFactory;
import feign.remoting.util.NettyEventLoopUtil;
import feign.remoting.util.RemotingAddressParser;
import feign.remoting.util.RemotingUtil;
import feign.remoting.util.RpcAddressParser;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

public class ShuttleServer extends AbstractRemotingServer{
	/** logger */
    private static final Logger logger = LoggerFactory.getLogger("ShuttleServer");
    
    //Netty服务器的引导类
    private ServerBootstrap bootstrap;
    
    //Netty的Boss线程池，这个线程池中的线程不应该是守护线程，而是应该被手工关闭
    private final EventLoopGroup bossGroup = 
    		NettyEventLoopUtil.newEventLoopGroup(1,
    				new NamedThreadFactory("Rpc-netty-server-boss",false));
    
    //Netty的Worker线程池，默认提供的线程数量是操作系统Cpu数量的2倍
    private static final EventLoopGroup workerGroup = 
    		NettyEventLoopUtil.newEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2, 
    				new NamedThreadFactory("Rpc-netty-server-worker",true));
    
    /** channelFuture */
    private ChannelFuture channelFuture;

    //初始化用户自定义Processor
    private ConcurrentHashMap<String, UserProcessor<?>> userProcessors = new ConcurrentHashMap<String, UserProcessor<?>>(4);

    /** address parser to get custom args */
    private RemotingAddressParser addressParser;

    //连接管理器：管理连接对象，包括创建、添加、删除、检查是否可用等等
    private DefaultConnectionManager connectionManager;
    
    //连接事件的处理器：处理连接事件，继承ChannelDuplexHandler
    private ConnectionEventHandler connectionEventHandler;
    
    //连接事件监听器：监听连接事件的触发，然后执行对应的逻辑
    private ConnectionEventListener connectionEventListener = new ConnectionEventListener();

    //提供远程调用的能力，例如：oneway、invokeSync、invokeWithFuture、invokeWithCallback
    protected RpcRemoting rpcRemoting;

    //提供Command的编/解码能力
    private CodecFactory codecFactory = new RpcCodecFactory();

    //设置Worker线程池中，执行IO操作及业务操作的比例
    //默认参数：IO操作占70%，业务操作占30%
    static {
        if (workerGroup instanceof NioEventLoopGroup) {
            ((NioEventLoopGroup) workerGroup).setIoRatio(ConfigManager.netty_io_ratio());
        } else if (workerGroup instanceof EpollEventLoopGroup) {
            ((EpollEventLoopGroup) workerGroup).setIoRatio(ConfigManager.netty_io_ratio());
        }
    }
    
    //不开启连接管理器的Construct
    public ShuttleServer(int port) {
        this(port, false);
    }

    //不开启连接管理器的Construct
    public ShuttleServer(String ip, int port) {
        this(ip, port, false);
    }

    public ShuttleServer(int port, boolean manageConnection) {
        super(port);
        //如果manageConnection标志位为true，那么开启连接管理器
        if (manageConnection) {
            this.switches().turnOn(GlobalSwitch.SERVER_MANAGE_CONNECTION_SWITCH);
        }
    }
    
    public ShuttleServer(String ip, int port, boolean manageConnection) {
        super(ip, port);
        //如果manageConnection标志位为true，那么开启连接管理器
        if (manageConnection) {
            this.switches().turnOn(GlobalSwitch.SERVER_MANAGE_CONNECTION_SWITCH);
        }
    }

    //开启连接管理器的server
    public ShuttleServer(int port, boolean manageConnection, boolean syncStop) {
        this(port, manageConnection);
        //如果syncStop标志位为true，那么开启同步将服务端停止的功能
        if (syncStop) {
            this.switches().turnOn(GlobalSwitch.SERVER_SYNC_STOP);
        }
    }
    
    //初始化Shuttle服务器
    @Override
    protected void doInit() {
        if (this.addressParser == null) {
            this.addressParser = new RpcAddressParser();
        }
        //判断是否开启了连接管理器
        if (this.switches().isOn(GlobalSwitch.SERVER_MANAGE_CONNECTION_SWITCH)) {
        	//创建默认的连接管理器，并使用从连接池中随机选择连接线程的策略
        	this.connectionManager = new DefaultConnectionManager(new RandomSelectStrategy());
        	this.connectionEventHandler.setConnectionManager(this.connectionManager);
        	
        	//设置Connection事件处理器
            this.connectionEventHandler = new RpcConnectionEventHandler(switches());
            //设置Connection事件监听器
            this.connectionEventHandler.setConnectionEventListener(this.connectionEventListener);
        } else {
        	//设置Connection事件处理器
            this.connectionEventHandler = new ConnectionEventHandler(switches());
            //设置Connection事件监听器
            this.connectionEventHandler.setConnectionEventListener(this.connectionEventListener);
        }
        
        //初始化远程调用能力initRpcRemoting()
        this.rpcRemoting = new RpcServerRemoting(new RpcCommandFactory(), this.addressParser,this.connectionManager);
        
        //创建Netty服务器的引导类
        this.bootstrap = new ServerBootstrap();
        //设置Netty服务器的Boss和Worker
        this.bootstrap.group(bossGroup, workerGroup)
        	//根据操作系统类似，返回NIOServerSocketChannel.class或者EpollServerSocketChannel.class
            .channel(NettyEventLoopUtil.getServerSocketChannelClass())
            //设置ServerChannel(BOSS)的ACCEPT队列长度，默认设置为1024
            //SYN队列长度由/proc/sys/net/ipv4/tcp_max_syn_backlog设置
            .option(ChannelOption.SO_BACKLOG, ConfigManager.tcp_so_backlog())
            //设置ServerChannel(BOSS)允许重复使用本地地址和端口
            .option(ChannelOption.SO_REUSEADDR, ConfigManager.tcp_so_reuseaddr())
            //设置Socket Channel不启动Nagle算法
            .childOption(ChannelOption.TCP_NODELAY, ConfigManager.tcp_nodelay())
            //设置Socket Channel的TCP状态为KeepAlvie
            .childOption(ChannelOption.SO_KEEPALIVE, ConfigManager.tcp_so_keepalive());

        //===================================
        // TODO: 暂时注释掉设置TCP写缓存的高水位线
        //===================================
        //initWriteBufferWaterMark();

        //设置byteBuf的是否使用内存池的策略
        if (ConfigManager.netty_buffer_pooled()) {
            this.bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        } else {
            this.bootstrap.option(ChannelOption.ALLOCATOR, UnpooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.ALLOCATOR, UnpooledByteBufAllocator.DEFAULT);
        }

        //设置Epoll的工作方式：LevelTriger或者EdgeTriger
        //========================================================================
        //LT(level triggered)是epoll缺省的工作方式，并且同时支持block和no-block socket.
        //在这种做法中，内核告诉你一个文件描述符是否就绪了，然后你可以对这个就绪的fd进行IO操作。
        //如果你不作任何操作，内核还是会继续通知你的。所以，这种模式编程出错误可能性要小一点。
        //传统的select/poll都是这种模型的代表
        //========================================================================
        //ET(edge-triggered)是高速工作方式，只支持no-block socket，它效率要比LT更高。
        //ET与LT的区别在于，当一个新的事件到来时，ET模式下当然可以从epoll_wait调用中获取到这个事件，
        //可是如果这次没有把这个事件对应的套接字缓冲区处理完，在这个套接字中没有新的事件再次到来时，
        //在ET模式下是无法再次从epoll_wait调用中获取这个事件的。
        NettyEventLoopUtil.enableTriggeredMode(bootstrap);

        //获取是否开启服务端空闲检测开关，默认开启
        final boolean idleSwitch = ConfigManager.tcp_idle_switch();
        //获取服务端Channel空闲状态时间，默认9000ms
        final int idleTime = ConfigManager.tcp_server_idle();
        
        //创建服务端空闲状态的ChannelHandler，用于处理心跳检测
        final ChannelHandler serverIdleHandler = new ServerIdleHandler();
        
        final DispatcherHandler dispatcherHandler = new DispatcherHandler(true, this.userProcessors);
        
        //设置Socket Channel的Channel Handler
        this.bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel channel) {
                ChannelPipeline pipeline = channel.pipeline();
                //创建Command编码及解码的Channel Handler
                pipeline.addLast("decoder", codecFactory.newDecoder());
                pipeline.addLast("encoder", codecFactory.newEncoder());
                //若开启服务端空闲检测开关后，若channel的读写空闲时间超过idleTime
                //那么在serverIdleHandler中，该连接会被关闭
                if (idleSwitch) {
                    pipeline.addLast("idleStateHandler", new IdleStateHandler(0, 0, idleTime,TimeUnit.MILLISECONDS));
                    pipeline.addLast("serverIdleHandler", serverIdleHandler);
                }
                //设置连接事件处理器
                pipeline.addLast("connectionEventHandler", connectionEventHandler);
                //设置通用事件处理器
                pipeline.addLast("dispatcherHandler", dispatcherHandler);
                createConnection(channel);
            }

            //如果开启连接管理器，那么将当前channel bind到connection之后，并将当前connection添加DefaultConnectionManager中进行管理
            //如果未开启连接管理器，那么仅仅创建一个Connection并bind当前Channel
            private void createConnection(SocketChannel channel) {
                Url url = addressParser.parse(RemotingUtil.parseRemoteAddress(channel));
                if (switches().isOn(GlobalSwitch.SERVER_MANAGE_CONNECTION_SWITCH)) {
                    connectionManager.add(new Connection(channel, url), url.getUniqueKey());
                } else {
                    new Connection(channel, url);
                }
                //触发连接完成的自定义事件
                channel.pipeline().fireUserEventTriggered(ConnectionEventType.CONNECT);
            }
        });
    }

    //启动Netty服务器
    @Override
    protected boolean doStart() throws InterruptedException {
        this.channelFuture = this.bootstrap.bind(new InetSocketAddress(ip(), port())).sync();
        return this.channelFuture.isSuccess();
    }

    //停止Netty服务器
    @Override
    protected boolean doStop() {
        if (null != this.channelFuture) {
            this.channelFuture.channel().close();
        }
        if (this.switches().isOn(GlobalSwitch.SERVER_SYNC_STOP)) {
            this.bossGroup.shutdownGracefully().awaitUninterruptibly();
        } else {
            this.bossGroup.shutdownGracefully();
        }
        //如果开启了连接管理器，那么会将所有的连接进行关闭
        if (this.switches().isOn(GlobalSwitch.SERVER_MANAGE_CONNECTION_SWITCH)
            && null != this.connectionManager) {
            this.connectionManager.removeAll();
            logger.warn("Close all connections from server side!");
        }
        logger.warn("Rpc Server stopped!");
        return true;
    }
    //=====================
    // One way 方式调用
    //=====================
    //使用URL字符串地址，例如：127.0.0.1:12200?key1=value1&key2=value2，来发起Oneway调用请求
    //在调用过程中，使用字符串地址找到可用的客户端连接，如果找不到，则抛出异常
    //与rpc客户机不同，这里的地址参数不起作用，因为rpc服务器不会创建连接。
    public void oneway(final String addr, final Object request) throws RemotingException,InterruptedException {
        check();
        this.rpcRemoting.oneway(addr, request, null);
    }
    //使用URL字符串地址（127.0.0.1:12200?key1=value1&key2=value2）+ InvokeContext，来发起Oneway调用请求
    public void oneway(final String addr, final Object request, final InvokeContext invokeContext) throws RemotingException,InterruptedException {
        check();
        this.rpcRemoting.oneway(addr, request, invokeContext);
    }
    //使用URL对象，发起Oneway调用请求
    public void oneway(final Url url, final Object request) throws RemotingException,InterruptedException {
        check();
        this.rpcRemoting.oneway(url, request, null);
    }
    //使用URL对象+invokeContext，发起Oneway调用请求
    public void oneway(final Url url, final Object request, final InvokeContext invokeContext) throws RemotingException,InterruptedException {
        check();
        this.rpcRemoting.oneway(url, request, invokeContext);
    }
    //使用Connection对象，发起Oneway调用请求
    //注意：不需要启用连接管理器
    public void oneway(final Connection conn, final Object request) throws RemotingException {
        this.rpcRemoting.oneway(conn, request, null);
    }
    //使用Connection对象+invokeContext，发起Oneway调用请求
    //注意：不需要启用连接管理器
    public void oneway(final Connection conn, final Object request,final InvokeContext invokeContext) throws RemotingException {
        this.rpcRemoting.oneway(conn, request, invokeContext);
    }
    
    
    //=====================
    // 同步方式调用
    //=====================
    //使用URL字符串地址，例如：127.0.0.1:12200?key1=value1&key2=value2，来发起同步调用请求
    public Object invokeSync(final String addr, final Object request, final int timeoutMillis) throws RemotingException,InterruptedException {
        check();
        return this.rpcRemoting.invokeSync(addr, request, null, timeoutMillis);
    }

    //使用URL字符串地址 + invokeContext，来发起同步调用请求
    public Object invokeSync(final String addr, final Object request,
                             final InvokeContext invokeContext, final int timeoutMillis) throws RemotingException,InterruptedException {
        check();
        return this.rpcRemoting.invokeSync(addr, request, invokeContext, timeoutMillis);
    }

    //使用URL对象，来发起同步调用请求
    public Object invokeSync(Url url, Object request, int timeoutMillis) throws RemotingException,InterruptedException {
        check();
        return this.rpcRemoting.invokeSync(url, request, null, timeoutMillis);
    }

    //使用URL对象+invokeContext，来发起同步调用请求
    public Object invokeSync(final Url url, final Object request,
                             final InvokeContext invokeContext, final int timeoutMillis) throws RemotingException,InterruptedException {
        check();
        return this.rpcRemoting.invokeSync(url, request, invokeContext, timeoutMillis);
    }

    //使用Connection对象，来发起同步调用请求
    //注意：不需要启用连接管理器
    public Object invokeSync(final Connection conn, final Object request, final int timeoutMillis) throws RemotingException,InterruptedException {
        return this.rpcRemoting.invokeSync(conn, request, null, timeoutMillis);
    }

    //使用Connection对象+invokeContext，来发起同步调用请求
    //注意：不需要启用连接管理器
    public Object invokeSync(final Connection conn, final Object request,
                             final InvokeContext invokeContext, final int timeoutMillis) throws RemotingException,InterruptedException {
        return this.rpcRemoting.invokeSync(conn, request, invokeContext, timeoutMillis);
    }

    //针对指定的协议、指定的command，设置对应的Processor
    @Override
    public void registerProcessor(byte protocolCode, CommandCode cmd, RemotingProcessor<?> processor) {
    	//从协议管理器中，通过protocolCode获取指定的protocol对象
        ProtocolManager.getProtocol(ProtocolCode.fromBytes(protocolCode))
        //从protocol对象中获取CommandHandler，并通过指定的commandcode，设置对应的processor
        	.getCommandHandler()
            	.registerProcessor(cmd, processor);	
    }

    //针对指定的协议，设置业务线程池
    @Override
    public void registerDefaultExecutor(byte protocolCode, ExecutorService executor) {
        ProtocolManager.getProtocol(ProtocolCode.fromBytes(protocolCode)).getCommandHandler()
            .registerDefaultExecutor(executor);
    }

    //对不同类型的Connection Event（CONNECT, CLOSE, EXCEPTION）注册
    public void addConnectionEventProcessor(ConnectionEventType type, ConnectionEventProcessor processor) {
        this.connectionEventListener.addConnectionEventProcessor(type, processor);
    }

    //注册用户自定义的Processor
    @Override
    public void registerUserProcessor(UserProcessor<?> processor) {
    	//将参数中的processor注册到this.userProcessors的Map中
    	//userProcessors的key为processor.interest()
        UserProcessorRegisterHelper.registerUserProcessor(processor, this.userProcessors);
    }
    
    //检查url字符串是否处于连接状态
    public boolean isConnected(String remoteAddr) {
        Url url = this.rpcRemoting.addressParser.parse(remoteAddr);
        return this.isConnected(url);
    }

    //检查url对象是否处于连接状态
    public boolean isConnected(Url url) {
        Connection conn = this.rpcRemoting.connectionManager.get(url.getUniqueKey());
        if (null != conn) {
            return conn.isFine();
        }
        return false;
    }

    //检查是否开启了连接管理器
    private void check() {
        if (!this.switches().isOn(GlobalSwitch.SERVER_MANAGE_CONNECTION_SWITCH)) {
            throw new UnsupportedOperationException(
                "Please enable connection manage feature of Rpc Server before call this method! See comments in constructor RpcServer(int port, boolean manageConnection) to find how to enable!");
        }
    }

    //=============================
    // ~~~ getter and setter
    //=============================
    
    //获取地址解析器
    public RemotingAddressParser getAddressParser() {
        return this.addressParser;
    }

    //设置地址解析器
    public void setAddressParser(RemotingAddressParser addressParser) {
        this.addressParser = addressParser;
    }

    //获取连接管理器
    public DefaultConnectionManager getConnectionManager() {
        return connectionManager;
    }
    
}
