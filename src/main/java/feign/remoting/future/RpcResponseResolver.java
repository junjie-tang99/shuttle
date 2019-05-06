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
package feign.remoting.future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import feign.remoting.command.ResponseCommand;
import feign.remoting.command.RpcResponseCommand;
import feign.remoting.enumerate.ResponseStatus;
import feign.remoting.exception.CodecException;
import feign.remoting.exception.ConnectionClosedException;
import feign.remoting.exception.DeserializationException;
import feign.remoting.exception.InvokeException;
import feign.remoting.exception.InvokeSendFailedException;
import feign.remoting.exception.InvokeServerBusyException;
import feign.remoting.exception.InvokeServerException;
import feign.remoting.exception.InvokeTimeoutException;
import feign.remoting.exception.RemotingException;
import feign.remoting.exception.SerializationException;
import feign.remoting.util.StringUtils;

/**
 * Resolve response object from response command.
 * 
 * @author jiangping
 * @version $Id: RpcResponseResolver.java, v 0.1 2015-10-8 PM2:47:29 tao Exp $
 */
public class RpcResponseResolver {
    private static final Logger logger = LoggerFactory.getLogger("RpcRemoting");


    //解析Response Comand，并返回响应对象
    public static Object resolveResponseObject(ResponseCommand responseCommand, String addr) throws RemotingException {
    	//根据ResponseCommand的responseStatus，判断是否存在错误
    	preProcess(responseCommand, addr);
    	
    	//若响应结果为成功，那么将responseCommand转换为ResponseObject
        if (responseCommand.getResponseStatus() == ResponseStatus.SUCCESS) {
            return toResponseObject(responseCommand);
        } else {
        	//若响应结果为ERROR、UNKNOWN、ERROR_COMM、NO_PROCESSOR的情况
            String msg = String.format("Rpc invocation exception: %s, the address is %s, id=%s",
                responseCommand.getResponseStatus(), addr, responseCommand.getId());
            logger.warn(msg);
            //抛出InvokeException异常
            if (responseCommand.getCause() != null) {
                throw new InvokeException(msg, responseCommand.getCause());
            } else {
                throw new InvokeException(msg + ", please check the server log for more.");
            }
        }

    }

    //根据ResponseCommand的responseStatus，判断是否存在错误
    //若存在错误，则抛出相应的异常Exception
    private static void preProcess(ResponseCommand responseCommand, String addr) throws RemotingException {
        RemotingException e = null;
        String msg = null;
        //如果responseCommand为null，则认为调用超时，并返回InvokeTimeoutException
        if (responseCommand == null) {
            msg = String.format("Rpc invocation timeout[responseCommand null]! the address is %s", addr);
            e = new InvokeTimeoutException(msg);
        } else {
        	//如果responseCommand不为null，那么获取返回状态
            switch (responseCommand.getResponseStatus()) {
            	//如果是超时的状态码，则返回InvokeTimeoutException
                case TIMEOUT:
                    msg = String.format("Rpc invocation timeout[responseCommand TIMEOUT]! the address is %s", addr);
                    e = new InvokeTimeoutException(msg);
                    break;
                //如果是客户端发送错误，则返回InvokeSendFailedException
                case CLIENT_SEND_ERROR:
                    msg = String.format("Rpc invocation send failed! the address is %s", addr);
                    e = new InvokeSendFailedException(msg, responseCommand.getCause());
                    break;
                //如果是连接关闭，则返回ConnectionClosedException
                case CONNECTION_CLOSED:
                    msg = String.format("Connection closed! the address is %s", addr);
                    e = new ConnectionClosedException(msg);
                    break;
                //如果是线程池繁忙，则返回InvokeServerBusyException
                case SERVER_THREADPOOL_BUSY:
                    msg = String.format("Server thread pool busy! the address is %s, id=%s", addr, responseCommand.getId());
                    e = new InvokeServerBusyException(msg);
                    break;
                //如果是编解码错误，则返回CodecException
                case CODEC_EXCEPTION:
                    msg = String.format("Codec exception! the address is %s, id=%s", addr, responseCommand.getId());
                    e = new CodecException(msg);
                    break;
                //如果是服务端序列化错误，则返回SerializationException
                case SERVER_SERIAL_EXCEPTION:
                    msg = String
                        .format(
                            "Server serialize response exception! the address is %s, id=%s, serverSide=true",
                            addr, responseCommand.getId());
                    e = new SerializationException(detailErrMsg(msg, responseCommand),toThrowable(responseCommand), true);
                    break;
                //如果是服务端反序列化错误，则返回DeserializationException
                case SERVER_DESERIAL_EXCEPTION:
                    msg = String
                        .format(
                            "Server deserialize request exception! the address is %s, id=%s, serverSide=true",
                            addr, responseCommand.getId());
                    e = new DeserializationException(detailErrMsg(msg, responseCommand),toThrowable(responseCommand), true);
                    break;
                //如果是服务端错误，则返回InvokeServerException
                case SERVER_EXCEPTION:
                    msg = String.format(
                        "Server exception! Please check the server log, the address is %s, id=%s",
                        addr, responseCommand.getId());
                    e = new InvokeServerException(detailErrMsg(msg, responseCommand),
                        toThrowable(responseCommand));
                    break;
                //如果不存在错误，则不返回异常Exception
                default:
                    break;
            }
        }
        //如果存在错误消息，则打印Log日志
        if (StringUtils.isNotBlank(msg)) {
            logger.warn(msg);
        }
        //如果存在错误，则抛出响应的异常
        if (null != e) {
            throw e;
        }
    }


    //将remoting response command转换成object对象
    private static Object toResponseObject(ResponseCommand responseCommand) throws CodecException {
        RpcResponseCommand response = (RpcResponseCommand) responseCommand;
        //对RpcResponseCommand进行反序列化
        response.deserialize();
        //获取Application Response Object
        return response.getResponseObject();
    }

    /**
     * Convert remoting response command to throwable if it is a throwable, otherwise return null.
     */
    private static Throwable toThrowable(ResponseCommand responseCommand) throws CodecException {
        RpcResponseCommand resp = (RpcResponseCommand) responseCommand;
        resp.deserialize();
        Object ex = resp.getResponseObject();
        if (ex != null && ex instanceof Throwable) {
            return (Throwable) ex;
        }
        return null;
    }

    /**
     * Detail your error msg with the error msg returned from response command
     */
    private static String detailErrMsg(String clientErrMsg, ResponseCommand responseCommand) {
        RpcResponseCommand resp = (RpcResponseCommand) responseCommand;
        if (StringUtils.isNotBlank(resp.getErrorMsg())) {
            return String.format("%s, ServerErrorMsg:%s", clientErrMsg, resp.getErrorMsg());
        } else {
            return String.format("%s, ServerErrorMsg:null", clientErrMsg);
        }
    }
}
