package feign.remoting.exception;

/**
 * Exception when invoke timeout
 * 
 * @author jiangping
 * @version $Id: InvokeTimeoutException.java, v 0.1 2015-9-28 PM3:35:53 tao Exp $
 */
public class InvokeTimeoutException extends RemotingException {

    /** For serialization  */
    private static final long serialVersionUID = -7772633244795043476L;

    /**
     * Default constructor.
     */
    public InvokeTimeoutException() {
    }

    /**
     * Constructor.
     *
     * @param msg the detail message
     */
    public InvokeTimeoutException(String msg) {
        super(msg);
    }

    /**
     * Constructor.
     *
     * @param msg the detail message
     * @param cause the cause
     */
    public InvokeTimeoutException(String msg, Throwable cause) {
        super(msg, cause);
    }

}
