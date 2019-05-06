package feign.remoting.command;

import java.net.InetSocketAddress;

import feign.remoting.enumerate.CommandCode;
import feign.remoting.enumerate.ResponseStatus;
import feign.remoting.enumerate.RpcCommandType;

/**
 * Command of response.
 * 
 * @author jiangping
 * @version $Id: ResponseCommand.java, v 0.1 2015-9-10 AM10:31:34 tao Exp $
 */
public class ResponseCommand extends RpcCommand {

    /** For serialization  */
    private static final long serialVersionUID = -5194754228565292441L;
    //响应状态，包括SUCESS、ERROR、SERVER_EXCEPTION
    private ResponseStatus    responseStatus;
    //响应时间
    private long              responseTimeMillis;
    //响应的主机
    private InetSocketAddress responseHost;
    //异常信息
    private Throwable         cause;

    public ResponseCommand() {
        super(RpcCommandType.RESPONSE.value());
    }

    public ResponseCommand(CommandCode code) {
        super(RpcCommandType.RESPONSE.value(), code);
    }

    public ResponseCommand(int id) {
        super(RpcCommandType.RESPONSE.value());
        this.setId(id);
    }

    public ResponseCommand(CommandCode code, int id) {
        super(RpcCommandType.RESPONSE.value(), code);
        this.setId(id);
    }

    public ResponseCommand(byte version, byte type, CommandCode code, int id) {
        super(version, type, code);
        this.setId(id);
    }

    /**
     * Getter method for property <tt>responseTimeMillis</tt>.
     * 
     * @return property value of responseTimeMillis
     */
    public long getResponseTimeMillis() {
        return responseTimeMillis;
    }

    /**
     * Setter method for property <tt>responseTimeMillis</tt>.
     * 
     * @param responseTimeMillis value to be assigned to property responseTimeMillis
     */
    public void setResponseTimeMillis(long responseTimeMillis) {
        this.responseTimeMillis = responseTimeMillis;
    }

    /**
     * Getter method for property <tt>responseHost</tt>.
     * 
     * @return property value of responseHost
     */
    public InetSocketAddress getResponseHost() {
        return responseHost;
    }

    /**
     * Setter method for property <tt>responseHost</tt>.
     * 
     * @param responseHost value to be assigned to property responseHost
     */
    public void setResponseHost(InetSocketAddress responseHost) {
        this.responseHost = responseHost;
    }

    /**
     * Getter method for property <tt>responseStatus</tt>.
     * 
     * @return property value of responseStatus
     */
    public ResponseStatus getResponseStatus() {
        return responseStatus;
    }

    /**
     * Setter method for property <tt>responseStatus</tt>.
     * 
     * @param responseStatus value to be assigned to property responseStatus
     */
    public void setResponseStatus(ResponseStatus responseStatus) {
        this.responseStatus = responseStatus;
    }

    /**
     * Getter method for property <tt>cause</tt>.
     * 
     * @return property value of cause
     */
    public Throwable getCause() {
        return cause;
    }

    /**
     * Setter method for property <tt>cause</tt>.
     * 
     * @param cause value to be assigned to property cause
     */
    public void setCause(Throwable cause) {
        this.cause = cause;
    }

}
