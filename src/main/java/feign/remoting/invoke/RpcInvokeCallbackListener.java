package feign.remoting.invoke;

import java.util.concurrent.RejectedExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import feign.remoting.command.ResponseCommand;
import feign.remoting.command.RpcResponseCommand;
import feign.remoting.enumerate.ResponseStatus;
import feign.remoting.exception.CodecException;
import feign.remoting.exception.ConnectionClosedException;
import feign.remoting.exception.InvokeException;
import feign.remoting.exception.InvokeServerBusyException;
import feign.remoting.exception.InvokeServerException;
import feign.remoting.exception.InvokeTimeoutException;


/**
 * Listener which listens the Rpc invoke result, and then invokes the call back.
 * 
 * @author jiangping
 * @version $Id: RpcInvokeCallbackListener.java, v 0.1 2015-9-30 AM10:36:34 tao Exp $
 */
public class RpcInvokeCallbackListener implements InvokeCallbackListener {

    private static final Logger logger = LoggerFactory.getLogger("RpcRemoting");

    private String              address;

    public RpcInvokeCallbackListener() {

    }

    public RpcInvokeCallbackListener(String address) {
        this.address = address;
    }

    /** 
     * @see com.alipay.remoting.InvokeCallbackListener#onResponse(com.alipay.remoting.InvokeFuture)
     */
    @Override
    public void onResponse(InvokeFuture future) {
        InvokeCallback callback = future.getInvokeCallback();
        if (callback != null) {
            CallbackTask task = new CallbackTask(this.getRemoteAddress(), future);
            if (callback.getExecutor() != null) {
                // There is no need to switch classloader, because executor is provided by user.
                try {
                    callback.getExecutor().execute(task);
                } catch (RejectedExecutionException e) {
                    logger.warn("Callback thread pool busy.");
                }
            } else {
                task.run();
            }
        }
    }

    class CallbackTask implements Runnable {

        InvokeFuture future;
        String       remoteAddress;

        /**
         * 
         */
        public CallbackTask(String remoteAddress, InvokeFuture future) {
            this.remoteAddress = remoteAddress;
            this.future = future;
        }

        /** 
         * @see java.lang.Runnable#run()
         */
        @Override
        public void run() {
            InvokeCallback callback = future.getInvokeCallback();
            // a lot of try-catches to protect thread pool
            ResponseCommand response = null;

            try {
                response = (ResponseCommand) future.waitResponse(0);
            } catch (InterruptedException e) {
                String msg = "Exception caught when getting response from InvokeFuture. The address is "
                             + this.remoteAddress;
                logger.error(msg, e);
            }
            if (response == null || response.getResponseStatus() != ResponseStatus.SUCCESS) {
                try {
                    Exception e;
                    if (response == null) {
                        e = new InvokeException("Exception caught in invocation. The address is "
                                                + this.remoteAddress + " responseStatus:"
                                                + ResponseStatus.UNKNOWN, future.getCause());
                    } else {
                        response.setInvokeContext(future.getInvokeContext());
                        switch (response.getResponseStatus()) {
                            case TIMEOUT:
                                e = new InvokeTimeoutException(
                                    "Invoke timeout when invoke with callback.The address is "
                                            + this.remoteAddress);
                                break;
                            case CONNECTION_CLOSED:
                                e = new ConnectionClosedException(
                                    "Connection closed when invoke with callback.The address is "
                                            + this.remoteAddress);
                                break;
                            case SERVER_THREADPOOL_BUSY:
                                e = new InvokeServerBusyException(
                                    "Server thread pool busy when invoke with callback.The address is "
                                            + this.remoteAddress);
                                break;
                            case SERVER_EXCEPTION:
                                String msg = "Server exception when invoke with callback.Please check the server log! The address is "
                                             + this.remoteAddress;
                                RpcResponseCommand resp = (RpcResponseCommand) response;
                                resp.deserialize();
                                Object ex = resp.getResponseObject();
                                if (ex != null && ex instanceof Throwable) {
                                    e = new InvokeServerException(msg, (Throwable) ex);
                                } else {
                                    e = new InvokeServerException(msg);
                                }
                                break;
                            default:
                                e = new InvokeException(
                                    "Exception caught in invocation. The address is "
                                            + this.remoteAddress + " responseStatus:"
                                            + response.getResponseStatus(), future.getCause());

                        }
                    }
                    callback.onException(e);
                } catch (Throwable e) {
                    logger
                        .error(
                            "Exception occurred in user defined InvokeCallback#onException() logic, The address is {}",
                            this.remoteAddress, e);
                }
            } else {
                ClassLoader oldClassLoader = null;
                try {
                    if (future.getAppClassLoader() != null) {
                        oldClassLoader = Thread.currentThread().getContextClassLoader();
                        Thread.currentThread().setContextClassLoader(future.getAppClassLoader());
                    }
                    response.setInvokeContext(future.getInvokeContext());
                    RpcResponseCommand rpcResponse = (RpcResponseCommand) response;
                    response.deserialize();
                    try {
                        callback.onResponse(rpcResponse.getResponseObject());
                    } catch (Throwable e) {
                        logger
                            .error(
                                "Exception occurred in user defined InvokeCallback#onResponse() logic.",
                                e);
                    }
                } catch (CodecException e) {
                    logger
                        .error(
                            "CodecException caught on when deserialize response in RpcInvokeCallbackListener. The address is {}.",
                            this.remoteAddress, e);
                } catch (Throwable e) {
                    logger.error(
                        "Exception caught in RpcInvokeCallbackListener. The address is {}",
                        this.remoteAddress, e);
                } finally {
                    if (oldClassLoader != null) {
                        Thread.currentThread().setContextClassLoader(oldClassLoader);
                    }
                }
            } // enf of else
        } // end of run
    }

    /** 
     * @see com.alipay.remoting.InvokeCallbackListener#getRemoteAddress()
     */
    @Override
    public String getRemoteAddress() {
        return this.address;
    }
}
