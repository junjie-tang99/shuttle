package feign.remoting.exception;

/**
 * Exception when invoke send failed
 *
 * @author jiangping
 * @version $Id: InvokeSendFailedException.java, v 0.1 2015-10-3 PM 12:32:45 tao Exp $
 */
public class InvokeSendFailedException extends RemotingException {

    /** For serialization  */
    private static final long serialVersionUID = 4832257777758730796L;

    /**
     * Default constructor.
     */
    public InvokeSendFailedException() {
    }

    public InvokeSendFailedException(String msg) {
        super(msg);
    }

    public InvokeSendFailedException(String msg, Throwable cause) {
        super(msg, cause);
    }

}