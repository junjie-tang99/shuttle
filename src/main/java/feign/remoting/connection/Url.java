package feign.remoting.connection;

import java.lang.ref.SoftReference;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import feign.remoting.config.Configs;
import feign.remoting.protocol.RpcProtocolV2;
import feign.remoting.util.RemotingAddressParser;

/**
 * URL definition.
 * 
 * @author xiaomin.cxm
 * @version $Id: Url.java, v 0.1 Mar 11, 2016 6:01:59 PM xiaomin.cxm Exp $
 */
public class Url {
    /** origin url */
    private String     originUrl;

    /** ip, can be number format or hostname format*/
    private String     ip;

    /** port, should be integer between (0, 65535]*/
    private int        port;

    /** unique key of this url */
    private String     uniqueKey;

    /** URL args: timeout value when do connect */
    private int        connectTimeout;

    /** URL args: protocol */
    private byte       protocol;

    /** URL args: version */
    private byte       version = RpcProtocolV2.PROTOCOL_VERSION_1;

    //每个URL的连接数量
    private int        connNum;

    /** URL agrs: whether need warm up connection */
    private boolean    connWarmup;

    /** URL agrs: all parsed args of each originUrl */
    private Properties properties;

    /**
     * Constructor with originUrl
     * 
     * @param originUrl
     */
    protected Url(String originUrl) {
        this.originUrl = originUrl;
    }

    /**
     * Constructor with ip and port
     * <ul>
     * <li>Initialize ip:port as {@link Url#originUrl} </li>
     * <li>Initialize {@link Url#originUrl} as {@link Url#uniqueKey} </li>
     * </ul> 
     * 
     * @param ip
     * @param port
     */
    public Url(String ip, int port) {
        this(ip + RemotingAddressParser.COLON + port);
        this.ip = ip;
        this.port = port;
        this.uniqueKey = this.originUrl;
    }

    /**
     * Constructor with originUrl, ip and port
     * 
     * <ul>
     * <li>Initialize @param originUrl as {@link Url#originUrl} </li>
     * <li>Initialize ip:port as {@link Url#uniqueKey} </li>
     * </ul> 
     * 
     * @param originUrl
     * @param ip
     * @param port
     */
    public Url(String originUrl, String ip, int port) {
        this(originUrl);
        this.ip = ip;
        this.port = port;
        this.uniqueKey = ip + RemotingAddressParser.COLON + port;
    }

    /**
     * Constructor with originUrl, ip, port and properties
     * 
     * <ul>
     * <li>Initialize @param originUrl as {@link Url#originUrl} </li>
     * <li>Initialize ip:port as {@link Url#uniqueKey} </li>
     * <li>Initialize @param properties as {@link Url#properties} </li>
     * </ul> 
     * 
     * @param originUrl
     * @param ip
     * @param port
     * @param properties
     */
    public Url(String originUrl, String ip, int port, Properties properties) {
        this(originUrl, ip, port);
        this.properties = properties;
    }

    /**
     * Constructor with originUrl, ip, port, uniqueKey and properties
     * 
     * <ul>
     * <li>Initialize @param originUrl as {@link Url#originUrl} </li>
     * <li>Initialize @param uniqueKey as {@link Url#uniqueKey} </li>
     * <li>Initialize @param properties as {@link Url#properties} </li>
     * </ul>
     * 
     * @param originUrl
     * @param ip
     * @param port
     * @param uniqueKey
     * @param properties
     */
    public Url(String originUrl, String ip, int port, String uniqueKey, Properties properties) {
        this(originUrl, ip, port);
        this.uniqueKey = uniqueKey;
        this.properties = properties;
    }

    /**
     * Get property value according to property key
     * 
     * @param key
     * @return property value
     */
    public String getProperty(String key) {
        if (properties == null) {
            return null;
        }
        return properties.getProperty(key);
    }

    public String getOriginUrl() {
        return originUrl;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public String getUniqueKey() {
        return uniqueKey;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        if (connectTimeout <= 0) {
            throw new IllegalArgumentException("Illegal value of connection number [" + connNum
                                               + "], must be a positive integer].");
        }
        this.connectTimeout = connectTimeout;
    }

    public byte getProtocol() {
        return protocol;
    }

    public void setProtocol(byte protocol) {
        this.protocol = protocol;
    }

    public int getConnNum() {
        return connNum;
    }

    public void setConnNum(int connNum) {
        if (connNum <= 0 || connNum > Configs.MAX_CONN_NUM_PER_URL) {
            throw new IllegalArgumentException("Illegal value of connection number [" + connNum
                                               + "], must be an integer between ["
                                               + Configs.DEFAULT_CONN_NUM_PER_URL + ", "
                                               + Configs.MAX_CONN_NUM_PER_URL + "].");
        }
        this.connNum = connNum;
    }

    public boolean isConnWarmup() {
        return connWarmup;
    }

    public void setConnWarmup(boolean connWarmup) {
        this.connWarmup = connWarmup;
    }

    public Properties getProperties() {
        return properties;
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Url url = (Url) obj;
        if (this.getOriginUrl().equals(url.getOriginUrl())) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                 + ((this.getOriginUrl() == null) ? 0 : this.getOriginUrl().hashCode());
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Origin url [" + this.originUrl + "], Unique key [" + this.uniqueKey + "].");
        return sb.toString();
    }

    public static final String PROTOCOL_KEY = "_PROTOCOL";
    
    public static final String VERSION_KEY = "_VERSION";
    
	public static final String CONNECT_TIMEOUT_KEY  = "_CONNECTTIMEOUT";
	
	public static final String CONNECTION_NUM_KEY = "_CONNECTIONNUM";
	
	public static final String CONNECTION_WARMUP_KEY = "_CONNECTIONWARMUP";
    
    /** Use {@link SoftReference} to cache parsed urls. Key is the original url. */
    public static ConcurrentHashMap<String, SoftReference<Url>> parsedUrls  = new ConcurrentHashMap<String, SoftReference<Url>>();

    /** for unit test only, indicate this object have already been GCed */
    public static volatile boolean isCollected = false;

    /** logger */
    private static final Logger logger = LoggerFactory.getLogger("Url");

    /**
     * @throws Throwable
     */
    @Override
    protected void finalize() throws Throwable {
        try {
            isCollected = true;
            parsedUrls.remove(this.getOriginUrl());
        } catch (Exception e) {
            logger.error("Exception occurred when do finalize for Url [{}].", this.getOriginUrl(),
                e);
        }
    }

    public byte getVersion() {
        return version;
    }

    public void setVersion(byte version) {
        this.version = version;
    }
}