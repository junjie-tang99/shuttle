package feign.remoting;

import feign.remoting.connection.Connection;
import feign.remoting.invoke.InvokeContext;

/**
 * basic info for biz
 * 
 * @author xiaomin.cxm
 * @version $Id: BizContext.java, v 0.1 Jan 6, 2016 10:35:04 PM xiaomin.cxm Exp $
 */
public interface BizContext {
    /**
     * get remote address
     * 
     * @return
     */
    String getRemoteAddress();

    /**
     * get remote host ip
     * 
     * @return
     */
    String getRemoteHost();

    /**
     * get remote port
     * 
     * @return
     */
    int getRemotePort();

    /**
     * get the connection of this request
     *
     * @return
     */
    Connection getConnection();

    /**
     * check whether request already timeout
     *
     * @return true if already timeout, you can log some useful info and then discard this request.
     */
    boolean isRequestTimeout();

    /**
     * get the timeout value from rpc client.
     *
     * @return
     */
    int getClientTimeout();

    /**
     * get the arrive time stamp
     *
     * @return
     */
    long getArriveTimestamp();

    /**
     * put a key and value
     * 
     * @return
     */
    void put(String key, String value);

    /**
     * get value
     * 
     * @param key
     * @return
     */
    String get(String key);

    /**
     * get invoke context.
     *
     * @return
     */
    InvokeContext getInvokeContext();
}
