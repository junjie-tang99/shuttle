package feign.remoting.codec.factory;

import io.netty.channel.ChannelHandler;

/**
 * Codec interface.
 *
 * @author chengyi (mark.lx@antfin.com) 2018-06-20 21:07
 */
public interface CodecFactory {

    /**
     * Create an encoder instance.
     *
     * @return new encoder instance
     */
    ChannelHandler newEncoder();

    /**
     * Create an decoder instance.
     *
     * @return new decoder instance
     */
    ChannelHandler newDecoder();
}