package feign.remoting.protocol;

public class RpcProtocolManager {
    public static final int DEFAULT_PROTOCOL_CODE_LENGTH = 1;

    public static void initProtocols() {
        //ProtocolManager.registerProtocol(new RpcProtocol(), RpcProtocol.PROTOCOL_CODE);
        ProtocolManager.registerProtocol(new RpcProtocolV2(), RpcProtocolV2.PROTOCOL_CODE);
    }
}
