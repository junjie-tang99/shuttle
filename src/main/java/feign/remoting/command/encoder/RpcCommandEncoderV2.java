/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feign.remoting.command.encoder;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import feign.remoting.command.RequestCommand;
import feign.remoting.command.ResponseCommand;
import feign.remoting.command.RpcCommand;
import feign.remoting.connection.Connection;
import feign.remoting.protocol.RpcProtocolV2;
import feign.remoting.switches.ProtocolSwitch;
import feign.remoting.util.CrcUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;

/**
 * Encode remoting command into ByteBuf v2.
 * 
 * @author jiangping
 * @version $Id: RpcCommandEncoderV2.java, v 0.1 2017-05-27 PM8:11:27 tao Exp $
 */
public class RpcCommandEncoderV2 implements CommandEncoder {
    /** logger */
    private static final Logger logger = LoggerFactory.getLogger("RpcCommandEncoderV2");

    /**
     * @see CommandEncoder#encode(ChannelHandlerContext, Serializable, ByteBuf)
     */
    @Override
    public void encode(ChannelHandlerContext ctx, Serializable msg, ByteBuf out) throws Exception {
        try {
            if (msg instanceof RpcCommand) {
            	/*
            	 * * * * * * * * * * * * * * * * * * * * * *
            	 * 固定部分，如果是Req，那么是24个字节，如果是Res，那么是22个字节
            	 *  * * * * * * * * * * * * * * * * * * * *
				 * proto: magic code for protocol 协议的魔数,1字节
				 * ver: version for protocol 协议版本,1字节
				 * type: request/response/request oneway Rpc命令类型,1字节
				 * cmdcode: code for remoting command 远程命令代码,2字节
				 * ver2:version for remoting command 远程命令版本,1字节
				 * requestId: id of request 请求编号,4字节
				 * codec: code for codec 序列化代码,1字节
				 * switch: function switch 协议功能开关,1字节
				 * (req)timeout: request timeout. 当命令类型是请求时,此位置为超时时间,4个字节
				 * (resp)respStatus: response status 当命令类型是回复时，此位置为回复状态,2个字节
				 * classLen: length of request or response class name 请求类和回复类的长度,2个字节
				 * headerLen: length of header 头部长度,2个字节
				 * cotentLen: length of content 内容长度,4个字节
				 * * * * * * * * * * * * * * * * * * * * * * *
            	 * 动态部分
            	 *  * * * * * * * * * * * * * * * * * * * *
				 * className 类名
				 * header 内容
				 * content 内容
				 * crc (optional) 帧的CRC32(当ver1 > 1时存在)
				 */
            	//获取ByteBuf当前处于可写的index
                int index = out.writerIndex();
                RpcCommand cmd = (RpcCommand) msg;
                //写入协议的MagicWord
                out.writeByte(RpcProtocolV2.PROTOCOL_CODE);
                //在Channel上获取Protocol的VERSOIN属性
                Attribute<Byte> version = ctx.channel().attr(Connection.VERSION);
                //Protocol的VERSOIN属性，默认值是1
                byte ver = RpcProtocolV2.PROTOCOL_VERSION_1;
                if (version != null && version.get() != null) {
                    ver = version.get();
                }
                
                out.writeByte(ver);
                out.writeByte(cmd.getType());
                out.writeShort(((RpcCommand) msg).getCmdCode().value());
                out.writeByte(cmd.getVersion());
                out.writeInt(cmd.getId());
                out.writeByte(cmd.getSerializer());
                //=================================================
                //协议开关，使用BitMap来对开关进行控制，最多支持8个种开关
                //=================================================
                out.writeByte(cmd.getProtocolSwitch().toByte());
                
                //如果指令是请求类型
                if (cmd instanceof RequestCommand) {
                    //需要设置超时时间
                    out.writeInt(((RequestCommand) cmd).getTimeout());
                }
                //如果指令是响应类型
                if (cmd instanceof ResponseCommand) {
                    //需要设置响应状态码
                    ResponseCommand response = (ResponseCommand) cmd;
                    out.writeShort(response.getResponseStatus().getValue());
                }
                //设置协议中请求类名的长度
                out.writeShort(cmd.getClazzLength());
                //设置协议中Header的长度
                out.writeShort(cmd.getHeaderLength());
                //设置协议中Body的长度
                out.writeInt(cmd.getContentLength());
                
                if (cmd.getClazzLength() > 0) {
                    out.writeBytes(cmd.getClazz());
                }
                if (cmd.getHeaderLength() > 0) {
                    out.writeBytes(cmd.getHeader());
                }
                if (cmd.getContentLength() > 0) {
                    out.writeBytes(cmd.getContent());
                }
                
                //=============================================
                //如果开启了CRC校验功能，那么在协议最后，增加crc码
                //=============================================
                if (ver == RpcProtocolV2.PROTOCOL_VERSION_2
                    && cmd.getProtocolSwitch().isOn(ProtocolSwitch.CRC_SWITCH_INDEX)) {
                    // compute the crc32 and write to out
                    byte[] frame = new byte[out.readableBytes()];
                    out.getBytes(index, frame);
                    out.writeInt(CrcUtil.crc32(frame));
                }
            } else {
                String warnMsg = "msg type [" + msg.getClass() + "] is not subclass of RpcCommand";
                logger.warn(warnMsg);
            }
        } catch (Exception e) {
            logger.error("Exception caught!", e);
            throw e;
        }
    }
}
