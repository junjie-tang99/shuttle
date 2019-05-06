package feign.remoting.connection;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import feign.remoting.invoke.InvokeFuture;
import feign.remoting.protocol.ProtocolCode;
import feign.remoting.protocol.RpcProtocolV2;
import feign.remoting.util.ConcurrentHashSet;
import feign.remoting.util.RemotingUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.AttributeKey;

//主要是对于Socket Channel的抽象及包装
public class Connection {
    private static final Logger logger = LoggerFactory.getLogger("Connection");
    
    private Channel channel;
    private final ConcurrentHashMap<Integer, InvokeFuture> invokeFutureMap = new ConcurrentHashMap<Integer, InvokeFuture>(4);

    /** Attribute key for connection */
    public static final AttributeKey<Connection> CONNECTION = AttributeKey.valueOf("connection");
    /** Attribute key for heartbeat count */
    public static final AttributeKey<Integer> HEARTBEAT_COUNT  = AttributeKey.valueOf("heartbeatCount");

    /** Attribute key for heartbeat switch for each connection */
    public static final AttributeKey<Boolean> HEARTBEAT_SWITCH = AttributeKey.valueOf("heartbeatSwitch");

    /** Attribute key for protocol */
    public static final AttributeKey<ProtocolCode> PROTOCOL = AttributeKey.valueOf("protocol");
    
    private ProtocolCode protocolCode;

    /** Attribute key for version */
    public static final AttributeKey<Byte> VERSION = AttributeKey.valueOf("version");
    private byte version = RpcProtocolV2.PROTOCOL_VERSION_1;

    private Url url;

    private final ConcurrentHashMap<Integer/* id */, String/* poolKey */> id2PoolKey = new ConcurrentHashMap<Integer, String>(256);

    private Set<String> poolKeys = new ConcurrentHashSet<String>();

    private AtomicBoolean closed = new AtomicBoolean(false);

    private final ConcurrentHashMap<String/* attr key*/, Object /*attr value*/> attributes = new ConcurrentHashMap<String, Object>();

    /** the reference count used for this connection. If equals 2, it means this connection has been referenced 2 times */
    private final AtomicInteger referenceCount   = new AtomicInteger();

    /** no reference of the current connection */
    private static final int NO_REFERENCE     = 0;

    public Connection(Channel channel) {
        this.channel = channel;
        //将当前Connection对象附在当前的Channel上
        this.channel.attr(CONNECTION).set(this);
    }

    public Connection(Channel channel, Url url) {
        this(channel);
        this.url = url;
        //将当前的url，设置到poolKeys上
        this.poolKeys.add(url.getUniqueKey());
    }

    public Connection(Channel channel, ProtocolCode protocolCode, Url url) {
        this(channel, url);
        this.protocolCode = protocolCode;
        this.init();
    }

    public Connection(Channel channel, ProtocolCode protocolCode, byte version, Url url) {
        this(channel, url);
        this.protocolCode = protocolCode;
        this.version = version;
        this.init();
    }

    /**
     * Initialization.
     */
    private void init() {
        this.channel.attr(HEARTBEAT_COUNT).set(new Integer(0));
        this.channel.attr(PROTOCOL).set(this.protocolCode);
        this.channel.attr(VERSION).set(this.version);
        this.channel.attr(HEARTBEAT_SWITCH).set(true);
    }

    //检查当前的Channel是否处于active状态
    public boolean isFine() {
        return this.channel != null && this.channel.isActive();
    }

    //增加当前Connection的引用次数
    public void increaseRef() {
        this.referenceCount.getAndIncrement();
    }

    //减少当前Connection的引用次数
    public void decreaseRef() {
        this.referenceCount.getAndDecrement();
    }

    //判断当前Connection的引用次数是否为0
    public boolean noRef() {
        return this.referenceCount.get() == NO_REFERENCE;
    }

    /**
     * Get the address of the remote peer.
     *
     * @return
     */
    public InetSocketAddress getRemoteAddress() {
        return (InetSocketAddress) this.channel.remoteAddress();
    }

    /**
     * Get the remote IP.
     *
     * @return
     */
    public String getRemoteIP() {
        return RemotingUtil.parseRemoteIP(this.channel);
    }

    /**
     * Get the remote port.
     *
     * @return
     */
    public int getRemotePort() {
        return RemotingUtil.parseRemotePort(this.channel);
    }

    /**
     * Get the address of the local peer.
     *
     * @return
     */
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) this.channel.localAddress();
    }

    /**
     * Get the local IP.
     *
     * @return
     */
    public String getLocalIP() {
        return RemotingUtil.parseLocalIP(this.channel);
    }

    /**
     * Get the local port.
     *
     * @return
     */
    public int getLocalPort() {
        return RemotingUtil.parseLocalPort(this.channel);
    }

    /**
     * Get the netty channel of the connection.
     *
     * @return
     */
    public Channel getChannel() {
        return this.channel;
    }

    //根据InvokeID获取InvokeFuture对象
    public InvokeFuture getInvokeFuture(int id) {
        return this.invokeFutureMap.get(id);
    }

    //将InvokeFuture对象添加到invokeFutureMap中
    public InvokeFuture addInvokeFuture(InvokeFuture future) {
        return this.invokeFutureMap.putIfAbsent(future.invokeId(), future);
    }

    //根据InvokeID删除InvokeFuture对象
    public InvokeFuture removeInvokeFuture(int id) {
        return this.invokeFutureMap.remove(id);
    }

    //获取InvokeFutreMap中的所有InvokeFutre对象，
    //1、给这些InvokeFutre对象设置一个ConnectionClosedResponse的Command
    //2、取消InvokeFutre对象的超时时间
    //3、？？
    public void onClose() {
        Iterator<Entry<Integer, InvokeFuture>> iter = invokeFutureMap.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<Integer, InvokeFuture> entry = iter.next();
            iter.remove();
            InvokeFuture future = entry.getValue();
            if (future != null) {
                future.putResponse(future.createConnectionClosedResponse(this.getRemoteAddress()));
                future.cancelTimeout();
                future.tryAsyncExecuteInvokeCallbackAbnormally();
            }
        }
    }

    /**
     * Close the connection.
     */
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                if (this.getChannel() != null) {
                    this.getChannel().close().addListener(new ChannelFutureListener() {

                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            if (logger.isInfoEnabled()) {
                                logger
                                    .info(
                                        "Close the connection to remote address={}, result={}, cause={}",
                                        RemotingUtil.parseRemoteAddress(Connection.this
                                            .getChannel()), future.isSuccess(), future.cause());
                            }
                        }

                    });
                }
            } catch (Exception e) {
                logger.warn("Exception caught when closing connection {}",
                    RemotingUtil.parseRemoteAddress(Connection.this.getChannel()), e);
            }
        }
    }

    //判断所有的InvokeFuture是否已被处理完
    public boolean isInvokeFutureMapFinish() {
        return invokeFutureMap.isEmpty();
    }

    /**
     * add a pool key to list
     *
     * @param poolKey
     */
    public void addPoolKey(String poolKey) {
        poolKeys.add(poolKey);
    }

    /**
     * get all pool keys
     */
    public Set<String> getPoolKeys() {
        return new HashSet<String>(poolKeys);
    }

    /**
     * remove pool key
     *
     * @param poolKey
     */
    public void removePoolKey(String poolKey) {
        poolKeys.remove(poolKey);
    }

    /**
     * Getter method for property <tt>url</tt>.
     *
     * @return property value of url
     */
    public Url getUrl() {
        return url;
    }

    /**
     * add Id to group Mapping
     *
     * @param id
     * @param poolKey
     */
    public void addIdPoolKeyMapping(Integer id, String poolKey) {
        this.id2PoolKey.put(id, poolKey);
    }

    /**
     * remove id to group Mapping
     *
     * @param id
     * @return
     */
    public String removeIdPoolKeyMapping(Integer id) {
        return this.id2PoolKey.remove(id);
    }

    /**
     * Set attribute key=value.
     *
     * @param key
     * @param value
     */
    public void setAttribute(String key, Object value) {
        attributes.put(key, value);
    }

    /**
     * set attribute if key absent.
     *
     * @param key
     * @param value
     * @return
     */
    public Object setAttributeIfAbsent(String key, Object value) {
        return attributes.putIfAbsent(key, value);
    }

    /**
     * Remove attribute.
     *
     * @param key
     */
    public void removeAttribute(String key) {
        attributes.remove(key);
    }

    /**
     * Get attribute.
     *
     * @param key
     * @return
     */
    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    /**
     * Clear attribute.
     */
    public void clearAttributes() {
        attributes.clear();
    }

    /**
     * Getter method for property <tt>invokeFutureMap</tt>.
     *
     * @return property value of invokeFutureMap
     */
    public ConcurrentHashMap<Integer, InvokeFuture> getInvokeFutureMap() {
        return invokeFutureMap;
    }
}
