package feign.remoting.codec.factory;


import feign.remoting.codec.ProtocolCodeBasedEncoder;
import feign.remoting.codec.RpcProtocolDecoder;
import feign.remoting.protocol.ProtocolCode;
import feign.remoting.protocol.RpcProtocolManager;
import feign.remoting.protocol.RpcProtocolV2;
import io.netty.channel.ChannelHandler;

/**
 * @author muyun.cyt
 * @version 2018/6/26 下午3:51
 */
public class RpcCodecFactory implements CodecFactory {

    @Override
    public ChannelHandler newEncoder() {
        return new ProtocolCodeBasedEncoder(ProtocolCode.fromBytes(RpcProtocolV2.PROTOCOL_CODE));
    }

    @Override
    public ChannelHandler newDecoder() {
        return new RpcProtocolDecoder(RpcProtocolManager.DEFAULT_PROTOCOL_CODE_LENGTH);
    }
}
