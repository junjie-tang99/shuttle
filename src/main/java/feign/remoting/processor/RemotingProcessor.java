package feign.remoting.processor;

import java.util.concurrent.ExecutorService;

import feign.remoting.RemotingContext;
import feign.remoting.command.RemotingCommand;


/**
 * Remoting processor processes remoting commands.
 * 
 * @author jiangping
 * @version $Id: RemotingProcessor.java, v 0.1 Dec 22, 2015 11:48:43 AM tao Exp $
 */
public interface RemotingProcessor<T extends RemotingCommand> {

    /**
     * Process the remoting command.
     * 
     * @param ctx
     * @param msg
     * @param defaultExecutor
     * @throws Exception
     */
    void process(RemotingContext ctx, T msg, ExecutorService defaultExecutor) throws Exception;

    /**
     * Get the executor.
     * 
     * @return
     */
    ExecutorService getExecutor();

    /**
     * Set executor.
     * 
     * @param executor
     */
    void setExecutor(ExecutorService executor);

}
