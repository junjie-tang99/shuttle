package feign.remoting.invoke;

import java.net.InetSocketAddress;

import feign.remoting.command.RemotingCommand;
import io.netty.util.Timeout;

/**
 * The future of an invocation.
 * 
 * @author jiangping
 * @version $Id: InvokeFuture.java, v 0.1 2015-9-21 PM5:30:35 tao Exp $
 */
public interface InvokeFuture {
    /**
     * Wait response with timeout.
     *
     * @param timeoutMillis time out in millisecond
     * @return remoting command
     * @throws InterruptedException if interrupted
     */
    RemotingCommand waitResponse(final long timeoutMillis) throws InterruptedException;

    /**
     * Wait response with unlimit timeout
     *
     * @return remoting command
     * @throws InterruptedException if interrupted
     */
    RemotingCommand waitResponse() throws InterruptedException;

    /**
     * Create a remoting command response when connection closed
     *
     * @param responseHost target host
     * @return remoting command
     */
    RemotingCommand createConnectionClosedResponse(InetSocketAddress responseHost);

    /**
     * Put the response to the future.
     *
     * @param response remoting command
     */
    void putResponse(final RemotingCommand response);

    /**
     * Get the id of the invocation.
     * @return invoke id
     */
    int invokeId();

    /**
     * Execute the callback.
     */
    void executeInvokeCallback();

    /**
     * Asynchronous execute the callback abnormally.
     */
    void tryAsyncExecuteInvokeCallbackAbnormally();

    /**
     * Set the cause if exception caught.
     */
    void setCause(Throwable cause);

    /**
     * Get the cause of exception of the future.
     * @return the cause
     */
    Throwable getCause();

    /**
     * Get the application callback of the future.
     * @return get invoke callback
     */
    InvokeCallback getInvokeCallback();

    /**
     * Add timeout for the future.
     */
    void addTimeout(Timeout timeout);

    /**
     * Cancel the timeout.
     */
    void cancelTimeout();

    /**
     * Whether the future is done.
     * @return true if the future is done
     */
    boolean isDone();

    /**
     * Get application classloader.
     * @return application classloader
     */
    ClassLoader getAppClassLoader();

    /**
     * Get the protocol code of command.
     * @return protocol code
     */
    byte getProtocolCode();

    /**
     * set invoke context
     */
    void setInvokeContext(InvokeContext invokeContext);

    /**
     * Get invoke context.
     * @return the invoke context
     */
    InvokeContext getInvokeContext();
}

