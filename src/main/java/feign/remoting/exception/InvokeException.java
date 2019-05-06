package feign.remoting.exception;

/**
 * Exception when invoke failed
 * 
 * @author jiangping
 * @version $Id: InvokeException.java, v 0.1 2015-10-5 PM8:19:36 tao Exp $
 */
public class InvokeException extends RemotingException {
    /** For serialization  */
    private static final long serialVersionUID = -3974514863386363570L;

    /**
     * Default constructor.
     */
    public InvokeException() {
    }

    public InvokeException(String msg) {
        super(msg);
    }

    public InvokeException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
