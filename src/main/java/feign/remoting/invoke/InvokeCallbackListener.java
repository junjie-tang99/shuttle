package feign.remoting.invoke;


/**
 * Listener to listen response and invoke callback.
 * 
 * @author jiangping
 * @version $Id: InvokeCallbackListener.java, v 0.1 2015-9-21 PM5:17:08 tao Exp $
 */
public interface InvokeCallbackListener {
    /**
     * Response arrived.
     * 
     * @param future
     */
    void onResponse(final InvokeFuture future);

    /**
     * Get the remote address.
     * 
     * @return
     */
    String getRemoteAddress();
}
