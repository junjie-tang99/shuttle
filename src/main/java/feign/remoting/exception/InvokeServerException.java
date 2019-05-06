package feign.remoting.exception;

/**
 * Server exception caught when invoking
 * 
 * @author jiangping
 * @version $Id: InvokeServerException.java, v 0.1 2015-10-9 AM11:16:10 tao Exp $
 */
public class InvokeServerException extends RemotingException {
    /** For serialization  */
    private static final long serialVersionUID = 4480283862377034355L;

    /**
     * Default constructor.
     */
    public InvokeServerException() {
    }

    public InvokeServerException(String msg) {
        super(msg);
    }

    public InvokeServerException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
