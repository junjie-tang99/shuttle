package feign.remoting.enumerate;

/**
 * Event triggered by connection state.
 * 
 * @author jiangping
 * @version $Id: ConnectionEventType.java, v 0.1 Mar 4, 2016 8:03:27 PM tao Exp $
 */
public enum ConnectionEventType {
    CONNECT, CLOSE, EXCEPTION;
}
