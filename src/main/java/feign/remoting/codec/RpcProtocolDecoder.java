package feign.remoting.codec;


import io.netty.buffer.ByteBuf;

/**
 * Rpc protocol decoder.
 *
 * @author tsui
 * @version $Id: RpcProtocolDecoder.java, v 0.1 2018-03-27 19:28 tsui Exp $
 */
public class RpcProtocolDecoder extends ProtocolCodeBasedDecoder {
    public static final int MIN_PROTOCOL_CODE_WITH_VERSION = 2;

    public RpcProtocolDecoder(int protocolCodeLength) {
        super(protocolCodeLength);
    }

    @Override
    protected byte decodeProtocolVersion(ByteBuf in) {
        in.resetReaderIndex();
        if (in.readableBytes() >= protocolCodeLength + DEFAULT_PROTOCOL_VERSION_LENGTH) {
            byte rpcProtocolCodeByte = in.readByte();
            if (rpcProtocolCodeByte >= MIN_PROTOCOL_CODE_WITH_VERSION) {
                return in.readByte();
            }
        }
        return DEFAULT_ILLEGAL_PROTOCOL_VERSION_LENGTH;
    }
}
