package feign.remoting.codec;

import java.io.Serializable;

import feign.remoting.connection.Connection;
import feign.remoting.protocol.Protocol;
import feign.remoting.protocol.ProtocolCode;
import feign.remoting.protocol.ProtocolManager;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.Attribute;

/**
 * Protocol code based newEncoder, the main newEncoder for a certain protocol, which is lead by one or multi bytes (magic code).
 *
 * Notice: this is stateless can be noted as {@link io.netty.channel.ChannelHandler.Sharable}
 * @author jiangping
 * @version $Id: ProtocolCodeBasedEncoder.java, v 0.1 2015-12-11 PM 7:30:30 tao Exp $
 */
@ChannelHandler.Sharable
public class ProtocolCodeBasedEncoder extends MessageToByteEncoder<Serializable> {

    /** default protocol code */
    protected ProtocolCode defaultProtocolCode;

    public ProtocolCodeBasedEncoder(ProtocolCode defaultProtocolCode) {
        super();
        this.defaultProtocolCode = defaultProtocolCode;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Serializable msg, ByteBuf out)
                                                                                   throws Exception {
        Attribute<ProtocolCode> att = ctx.channel().attr(Connection.PROTOCOL);
        ProtocolCode protocolCode;
        if (att == null || att.get() == null) {
            protocolCode = this.defaultProtocolCode;
        } else {
            protocolCode = att.get();
        }
        Protocol protocol = ProtocolManager.getProtocol(protocolCode);
        protocol.getEncoder().encode(ctx, msg, out);
    }

}
