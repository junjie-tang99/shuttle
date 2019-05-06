package feign.remoting.invoke;

import java.util.concurrent.Executor;

/**
 * Invoke callback.
 * 
 * @author jiangping
 * @version $Id: InvokeCallback.java, v 0.1 2015-9-30 AM10:24:26 tao Exp $
 */
public interface InvokeCallback {

    /**
     * Response received.
     * 
     * @param result
     */
    void onResponse(final Object result);

    /**
     * Exception caught.
     * 
     * @param e
     */
    void onException(final Throwable e);

    /**
     * User defined executor.
     * 
     * @return
     */
    Executor getExecutor();

}
