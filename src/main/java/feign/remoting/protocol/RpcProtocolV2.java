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
package feign.remoting.protocol;

import feign.remoting.command.decoder.CommandDecoder;
import feign.remoting.command.decoder.RpcCommandDecoderV2;
import feign.remoting.command.encoder.CommandEncoder;
import feign.remoting.command.encoder.RpcCommandEncoderV2;
import feign.remoting.command.factory.CommandFactory;
import feign.remoting.command.factory.RpcCommandFactory;
import feign.remoting.command.handler.CommandHandler;
import feign.remoting.command.handler.RpcCommandHandler;
import feign.remoting.command.trigger.HeartbeatTrigger;
import feign.remoting.command.trigger.RpcHeartbeatTrigger;

/**
 * Request command protocol for v2
 * 0     1     2           4           6           8          10     11     12          14         16
 * +-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+------+-----+-----+-----+-----+
 * |proto| ver1|type | cmdcode   |ver2 |   requestId           |codec|switch|   timeout             |
 * +-----------+-----------+-----------+-----------+-----------+------------+-----------+-----------+
 * |classLen   |headerLen  |contentLen             |           ...                                  |
 * +-----------+-----------+-----------+-----------+                                                +
 * |               className + header  + content  bytes                                             |
 * +                                                                                                +
 * |                               ... ...                                  | CRC32(optional)       |
 * +------------------------------------------------------------------------------------------------+
 * 
 * Response command protocol for v2
 * 0     1     2     3     4           6           8          10     11    12          14          16
 * +-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+------+-----+-----+-----+-----+
 * |proto| ver1| type| cmdcode   |ver2 |   requestId           |codec|switch|respstatus |  classLen |
 * +-----------+-----------+-----------+-----------+-----------+------------+-----------+-----------+
 * |headerLen  | contentLen            |                      ...                                   |
 * +-----------------------------------+                                                            +
 * |               className + header  + content  bytes                                             |
 * +                                                                                                +
 * |                               ... ...                                  | CRC32(optional)       |
 * +------------------------------------------------------------------------------------------------+
 * respstatus: response status
 * 
 * @author jiangping
 * @version $Id: RpcProtocolV2.java, v 0.1 2017-05-27 PM7:04:04 tao Exp $
 */
public class RpcProtocolV2 implements Protocol {
    /* because the design defect, the version is neglected in RpcProtocol, so we design RpcProtocolV2 and add protocol version. */
    public static final byte PROTOCOL_CODE       = (byte) 2;
    /** version 1, is the same with RpcProtocol */
    public static final byte PROTOCOL_VERSION_1  = (byte) 1;
    /** version 2, is the protocol version for RpcProtocolV2 */
    public static final byte PROTOCOL_VERSION_2  = (byte) 2;

    /**
     * in contrast to protocol v1,
     * one more byte is used as protocol version,
     * and another one is userd as protocol switch
     */
    private static final int REQUEST_HEADER_LEN  = 24;
    private static final int RESPONSE_HEADER_LEN = 22;
    private CommandEncoder   encoder;
    private CommandDecoder   decoder;
    private HeartbeatTrigger heartbeatTrigger;
    private CommandHandler   commandHandler;
    private CommandFactory   commandFactory;

    public RpcProtocolV2() {
        this.encoder = new RpcCommandEncoderV2();
        this.decoder = new RpcCommandDecoderV2();
        this.commandFactory = new RpcCommandFactory();
        this.heartbeatTrigger = new RpcHeartbeatTrigger(this.commandFactory);
        this.commandHandler = new RpcCommandHandler(this.commandFactory);
    }

    public static int getRequestHeaderLength() {
        return RpcProtocolV2.REQUEST_HEADER_LEN;
    }

    public static int getResponseHeaderLength() {
        return RpcProtocolV2.RESPONSE_HEADER_LEN;
    }

    @Override
    public CommandEncoder getEncoder() {
        return this.encoder;
    }

    @Override
    public CommandDecoder getDecoder() {
        return this.decoder;
    }

    @Override
    public HeartbeatTrigger getHeartbeatTrigger() {
        return this.heartbeatTrigger;
    }

    @Override
    public CommandHandler getCommandHandler() {
        return this.commandHandler;
    }

    @Override
    public CommandFactory getCommandFactory() {
        return this.commandFactory;
    }
}
