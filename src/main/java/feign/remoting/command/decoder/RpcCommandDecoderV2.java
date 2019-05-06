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
package feign.remoting.command.decoder;

import java.net.InetSocketAddress;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import feign.remoting.command.HeartbeatAckCommand;
import feign.remoting.command.HeartbeatCommand;
import feign.remoting.command.RequestCommand;
import feign.remoting.command.ResponseCommand;
import feign.remoting.command.RpcRequestCommand;
import feign.remoting.command.RpcResponseCommand;
import feign.remoting.enumerate.CommandCode;
import feign.remoting.enumerate.ResponseStatus;
import feign.remoting.enumerate.RpcCommandType;
import feign.remoting.protocol.RpcProtocolV2;
import feign.remoting.switches.ProtocolSwitch;
import feign.remoting.util.CrcUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * Command decoder for Rpc v2.
 * 
 * @author jiangping
 * @version $Id: RpcCommandDecoderV2.java, v 0.1 2017-05-27 PM5:15:26 tao Exp $
 */
public class RpcCommandDecoderV2 implements CommandDecoder {

    private static final Logger logger = LoggerFactory.getLogger("RpcCommandDecoderV2");

    private int lessLen;

    {
        lessLen = RpcProtocolV2.getResponseHeaderLength() < RpcProtocolV2.getRequestHeaderLength() ? RpcProtocolV2
            .getResponseHeaderLength() : RpcProtocolV2.getRequestHeaderLength();
    }

    /**
     * @see CommandDecoder#decode(ChannelHandlerContext, ByteBuf, List)
     */
    @Override
    public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // the less length between response header and request header
    	//保证可读取的字节数，协议的最小字节数
        if (in.readableBytes() >= lessLen) {
            in.markReaderIndex();
            //获取的协议Magic Word
            byte protocol = in.readByte();
            in.resetReaderIndex();
            //如果是RpcProtocolV2协议
            if (protocol == RpcProtocolV2.PROTOCOL_CODE) {
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
            	
                if (in.readableBytes() > 3) {
                    int startIndex = in.readerIndex();
                    in.markReaderIndex();
                    in.readByte(); //protocol code
                    byte version = in.readByte(); //protocol version
                    byte type = in.readByte(); //type
                    //如果Command的Type是请求类型
                    if (type == RpcCommandType.REQUEST.value() || type == RpcCommandType.REQUEST_ONEWAY.value()) {
                        //判断当前可读的字节数是否大于Request协议头的长度，否则会无法解析该协议
                        if (in.readableBytes() >= RpcProtocolV2.getRequestHeaderLength() - 3) {
                        	//读取Comand的种类，例如：Heartbeat，Request，Response等
                            short cmdCode = in.readShort();
                            //读取Command的版本号
                            byte ver2 = in.readByte();
                            //读取请求ID
                            int requestId = in.readInt();
                            //读取序列化器类型
                            byte serializer = in.readByte();
                            //读取开关值
                            byte protocolSwitchValue = in.readByte();
                            //读取超时时间
                            int timeout = in.readInt();
                            //读取业务类名的长度
                            short classLen = in.readShort();
                            //读取Header的长度
                            short headerLen = in.readShort();
                            //读取内容长度
                            int contentLen = in.readInt();
                            
                            byte[] clazz = null;
                            byte[] header = null;
                            byte[] content = null;

                            // decide the at-least bytes length for each version
                            int lengthAtLeastForV1 = classLen + headerLen + contentLen;
                            //判断是否开启了CRC校验
                            boolean crcSwitchOn = ProtocolSwitch.isOn(ProtocolSwitch.CRC_SWITCH_INDEX, protocolSwitchValue);
                            int lengthAtLeastForV2 = classLen + headerLen + contentLen;
                            if (crcSwitchOn) {
                                lengthAtLeastForV2 += 4;// crc int
                            }

                            // 读取动态协议内容
                            //if ((version == RpcProtocolV2.PROTOCOL_VERSION_1 && in.readableBytes() >= lengthAtLeastForV1)
                            //    || (version == RpcProtocolV2.PROTOCOL_VERSION_1 && in
                            //        .readableBytes() >= lengthAtLeastForV2)) {
                        	if (in.readableBytes() >= lengthAtLeastForV2) {	
                                if (classLen > 0) {
                                    clazz = new byte[classLen];
                                    in.readBytes(clazz);
                                }
                                if (headerLen > 0) {
                                    header = new byte[headerLen];
                                    in.readBytes(header);
                                 }
                                if (contentLen > 0) {
                                    content = new byte[contentLen];
                                    in.readBytes(content);
                                }
                                if (version == RpcProtocolV2.PROTOCOL_VERSION_2 && crcSwitchOn) {
                                	//如果CRC码校验不通过，则会抛出异常
                                    checkCRC(in, startIndex);
                                }
                            } else {// not enough data
                                in.resetReaderIndex();
                                return;
                            }
                            
                            //根据请求的种类，创建对应的Command对象
                            RequestCommand command;
                            if (cmdCode == CommandCode.HEARTBEAT.value()) {
                            	//如果是心跳的指令，那么创建HeartbeatCommand
                                command = new HeartbeatCommand();
                            } else {
                            	//如果请求类的指令，那么创建RpcRequestCommand
                                command = createRequestCommand(cmdCode);
                            }
                            command.setType(type);
                            command.setVersion(ver2);
                            command.setId(requestId);
                            command.setSerializer(serializer);
                            command.setProtocolSwitch(ProtocolSwitch.create(protocolSwitchValue));
                            command.setTimeout(timeout);
                            command.setClazz(clazz);
                            command.setHeader(header);
                            command.setContent(content);
                            
                            //解析完成后，将Command对象加入到Command List中
                            out.add(command);
                        } else {
                        	//如果可读的字节数不够，那么重新将ByteBuf的Index设置到头部
                            in.resetReaderIndex();
                        }
                        
                    } else if (type == RpcCommandType.RESPONSE.value()) {
                        //decode response
                        if (in.readableBytes() >= RpcProtocolV2.getResponseHeaderLength() - 3) {
                            short cmdCode = in.readShort();
                            byte ver2 = in.readByte();
                            int requestId = in.readInt();
                            byte serializer = in.readByte();
                            byte protocolSwitchValue = in.readByte();
                            short status = in.readShort();
                            short classLen = in.readShort();
                            short headerLen = in.readShort();
                            int contentLen = in.readInt();
                            byte[] clazz = null;
                            byte[] header = null;
                            byte[] content = null;

                            // decide the at-least bytes length for each version
                            int lengthAtLeastForV1 = classLen + headerLen + contentLen;
                            boolean crcSwitchOn = ProtocolSwitch.isOn( ProtocolSwitch.CRC_SWITCH_INDEX, protocolSwitchValue);
                            int lengthAtLeastForV2 = classLen + headerLen + contentLen;
                            if (crcSwitchOn) {
                                lengthAtLeastForV2 += 4;// crc int
                            }

                            // continue read
                            //if ((version == RpcProtocolV2.PROTOCOL_VERSION_1 && in.readableBytes() >= lengthAtLeastForV1)
                            //    || (version == RpcProtocolV2.PROTOCOL_VERSION_2 && in
                            //        .readableBytes() >= lengthAtLeastForV2)) {
                            if (in.readableBytes() >= lengthAtLeastForV2) {	
                                if (classLen > 0) {
                                    clazz = new byte[classLen];
                                    in.readBytes(clazz);
                                }
                                if (headerLen > 0) {
                                    header = new byte[headerLen];
                                    in.readBytes(header);
                                }
                                if (contentLen > 0) {
                                    content = new byte[contentLen];
                                    in.readBytes(content);
                                }
                                if (version == RpcProtocolV2.PROTOCOL_VERSION_2 && crcSwitchOn) {
                                    checkCRC(in, startIndex);
                                }
                            } else {// not enough data
                                in.resetReaderIndex();
                                return;
                            }
                            
                            
                            ResponseCommand command;
                            if (cmdCode == CommandCode.HEARTBEAT.value()) {
                                command = new HeartbeatAckCommand();
                            } else {
                                command = createResponseCommand(cmdCode);
                            }
                            command.setType(type);
                            command.setVersion(ver2);
                            command.setId(requestId);
                            command.setSerializer(serializer);
                            command.setProtocolSwitch(ProtocolSwitch.create(protocolSwitchValue));
                            command.setResponseStatus(ResponseStatus.valueOf(status));
                            command.setClazz(clazz);
                            command.setHeader(header);
                            command.setContent(content);
                            command.setResponseTimeMillis(System.currentTimeMillis());
                            command.setResponseHost((InetSocketAddress) ctx.channel().remoteAddress());

                            out.add(command);
                        } else {
                            in.resetReaderIndex();
                        }
                    } else {
                        String emsg = "Unknown command type: " + type;
                        logger.error(emsg);
                        throw new RuntimeException(emsg);
                    }
                }

            } else {
                String emsg = "Unknown protocol: " + protocol;
                logger.error(emsg);
                throw new RuntimeException(emsg);
            }

        }
    }

    private void checkCRC(ByteBuf in, int startIndex) {
    	//获取到协议结尾处的index
        int endIndex = in.readerIndex();
        //获取到生成的CRC Code
        int expectedCrc = in.readInt();
        //初始化协议包
        byte[] frame = new byte[endIndex - startIndex];
        //将bytebuf中的数据，存入到协议包中
        in.getBytes(startIndex, frame, 0, endIndex - startIndex);
        //根据协议包的数据，生成实际的CRC Code
        int actualCrc = CrcUtil.crc32(frame);
        if (expectedCrc != actualCrc) {
            String err = "CRC check failed!";
            logger.error(err);
            throw new RuntimeException(err);
        }
    }

    private ResponseCommand createResponseCommand(short cmdCode) {
        ResponseCommand command = new RpcResponseCommand();
        command.setCmdCode(CommandCode.valueOf(cmdCode));
        return command;
    }

    private RpcRequestCommand createRequestCommand(short cmdCode) {
        RpcRequestCommand command = new RpcRequestCommand();
        command.setCmdCode(CommandCode.valueOf(cmdCode));
        command.setArriveTime(System.currentTimeMillis());
        return command;
    }

}
