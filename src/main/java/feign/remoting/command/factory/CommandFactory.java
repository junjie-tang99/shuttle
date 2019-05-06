package feign.remoting.command.factory;

import java.net.InetSocketAddress;

import feign.remoting.command.RemotingCommand;
import feign.remoting.enumerate.ResponseStatus;

/**
 * Command factory
 * 
 * @author xiaomin.cxm
 * @version $Id: CommandFactory.java, v 0.1 Mar 10, 2016 11:24:24 AM yunliang.shi Exp $
 */
public interface CommandFactory {
    // ~~~ create request command

    /**
     * create a request command with request object
     *
     * @param requestObject the request object included in request command
     * @param <T>
     * @return
     */
    <T extends RemotingCommand> T createRequestCommand(final Object requestObject);

    // ~~~ create response command

    /**
     * create a normal response with response object
     * @param responseObject
     * @param requestCmd
     * @param <T>
     * @return
     */
    <T extends RemotingCommand> T createResponse(final Object responseObject,
                                                 RemotingCommand requestCmd);

    <T extends RemotingCommand> T createExceptionResponse(int id, String errMsg);

    <T extends RemotingCommand> T createExceptionResponse(int id, final Throwable t, String errMsg);

    <T extends RemotingCommand> T createExceptionResponse(int id, ResponseStatus status);

    <T extends RemotingCommand> T createExceptionResponse(int id, ResponseStatus status,
                                                          final Throwable t);

    <T extends RemotingCommand> T createTimeoutResponse(final InetSocketAddress address);

    <T extends RemotingCommand> T createSendFailedResponse(final InetSocketAddress address,
                                                           Throwable throwable);

    <T extends RemotingCommand> T createConnectionClosedResponse(final InetSocketAddress address,
                                                                 String message);
}

