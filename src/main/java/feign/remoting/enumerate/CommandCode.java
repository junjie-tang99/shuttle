package feign.remoting.enumerate;

/**
 * Command code for rpc remoting command.
 * @author jiangping
 * @version $Id: RpcCommandCode.java, v 0.1 2015-9-21 PM5:05:59 tao Exp $
 */
public enum CommandCode {

	HEARTBEAT((short) 0), RPC_REQUEST((short) 1), RPC_RESPONSE((short) 2);

    private short value;

    CommandCode(short value) {
        this.value = value;
    }

    public short value() {
        return this.value;
    }

    public static CommandCode valueOf(short value) {
        switch (value) {
        	case 0:
        		return HEARTBEAT;
            case 1:
                return RPC_REQUEST;
            case 2:
                return RPC_RESPONSE;
        }
        throw new IllegalArgumentException("Unknown Rpc command code value: " + value);
    }

}