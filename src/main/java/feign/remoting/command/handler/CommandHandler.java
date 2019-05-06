package feign.remoting.command.handler;

import java.util.concurrent.ExecutorService;

import feign.remoting.enumerate.CommandCode;
import feign.remoting.RemotingContext;
import feign.remoting.processor.RemotingProcessor;


/**
 * Command handler.
 * 
 * @author jiangping
 * @version $Id: CommandHandler.java, v 0.1 2015-12-14 PM4:03:55 tao Exp $
 */
public interface CommandHandler {
    /**
     * Handle the command.
     * 
     * @param ctx
     * @param msg
     * @throws Exception
     */
    void handleCommand(RemotingContext ctx, Object msg) throws Exception;

    /**
     * Register processor for command with specified code.
     * 
     * @param cmd
     * @param processor
     */
    void registerProcessor(CommandCode cmd, RemotingProcessor<?> processor);

    /**
     * Register default executor for the handler.
     * 
     * @param executor
     */
    void registerDefaultExecutor(ExecutorService executor);

    /**
     * Get default executor for the handler.
     */
    ExecutorService getDefaultExecutor();

}
