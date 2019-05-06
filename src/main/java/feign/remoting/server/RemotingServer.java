package feign.remoting.server;

import java.util.concurrent.ExecutorService;

import feign.remoting.enumerate.CommandCode;
import feign.remoting.processor.RemotingProcessor;
import feign.remoting.processor.UserProcessor;

public interface RemotingServer {

    /**
     * Start the server.
     */
    boolean start();

    /**
     * Stop the server.
     *
     * Remoting server can not be used any more after stop.
     * If you need, you should destroy it, and instantiate another one.
     */
    boolean stop();

    /**
     * Get the ip of the server.
     *
     * @return ip
     */
    String ip();

    /**
     * Get the port of the server.
     *
     * @return listened port
     */
    int port();

    /**
     * Register processor for command with the command code.
     *
     * @param protocolCode protocol code
     * @param commandCode command code
     * @param processor processor
     */
    void registerProcessor(byte protocolCode, CommandCode commandCode,
                           RemotingProcessor<?> processor);

    /**
     * Register default executor service for server.
     *
     * @param protocolCode protocol code
     * @param executor the executor service for the protocol code
     */
    void registerDefaultExecutor(byte protocolCode, ExecutorService executor);

    /**
     * Register user processor.
     *
     * @param processor user processor which can be a single-interest processor or a multi-interest processor
     */
    void registerUserProcessor(UserProcessor<?> processor);
}
