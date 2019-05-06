package feign.remoting.command;

import feign.remoting.enumerate.CommandCode;
import feign.remoting.enumerate.RpcCommandType;

/**
 * Command of request.
 * 
 * @author jiangping
 * @version $Id: RequestCommand.java, v 0.1 2015-9-10 AM10:27:59 tao Exp $
 */
public abstract class RequestCommand extends RpcCommand {

    /** For serialization  */
    private static final long serialVersionUID = -3457717009326601317L;
    //请求的超时时间，如果是-1代表永不超时
    private int               timeout          = -1;

    public RequestCommand() {
        super(RpcCommandType.REQUEST.value());
    }

    public RequestCommand(CommandCode code) {
        super(RpcCommandType.REQUEST.value(), code);
    }

    public RequestCommand(byte type, CommandCode code) {
        super(type, code);
    }

    public RequestCommand(byte version, byte type, CommandCode code) {
        super(version, type, code);
    }

    /**
     * Getter method for property <tt>timeout</tt>.
     * 
     * @return property value of timeout
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * Setter method for property <tt>timeout</tt>.
     * 
     * @param timeout value to be assigned to property timeout
     */
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }
}