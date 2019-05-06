package feign.remoting.exception;


/**
 * Exception when thread pool busy of process server
 * 
 * @author jiangping
 * @version $Id: InvokeServerBusyException.java, v 0.1 2015-10-9 AM11:16:10 tao Exp $
 */
public class InvokeServerBusyException extends RemotingException {
    /** For serialization  */
    private static final long serialVersionUID = 4480283862377034355L;

    /**
     * Default constructor.
     */
    public InvokeServerBusyException() {
    }

    public InvokeServerBusyException(String msg) {
        super(msg);
    }

    public InvokeServerBusyException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
