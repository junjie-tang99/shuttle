package feign.remoting.enumerate;

public enum RpcCommandType {
	RESPONSE((byte) 0x00),REQUEST((byte) 0x01),REQUEST_ONEWAY((byte) 0x02);
    
    private byte value;
    
    RpcCommandType(byte value){
    	this.value = value;
    }
    
    public byte value() {
    	return this.value;
    }

    public static RpcCommandType valueOf(byte value) {
        switch (value) {
        	case (byte) 0x00:
        		return RESPONSE;
            case (byte) 0x01:
                return REQUEST;
            case (byte)0x02:
                return REQUEST_ONEWAY;
        }
        throw new IllegalArgumentException("Unknown Rpc command type value: " + value);
    }
    
}
