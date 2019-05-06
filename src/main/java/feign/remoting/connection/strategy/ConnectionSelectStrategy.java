package feign.remoting.connection.strategy;

import java.util.List;

import feign.remoting.connection.Connection;

/**
 * Select strategy from connection pool
 * 
 * @author xiaomin.cxm
 * @version $Id: ConnectionSelectStrategy.java, v 0.1 Mar 14, 2016 11:06:57 AM xiaomin.cxm Exp $
 */
public interface ConnectionSelectStrategy {
    /**
     * select strategy
     * 
     * @param conns
     * @return
     */
    Connection select(List<Connection> conns);
}
