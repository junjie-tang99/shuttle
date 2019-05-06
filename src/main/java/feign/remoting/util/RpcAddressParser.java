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
package feign.remoting.util;

import java.lang.ref.SoftReference;
import java.util.Properties;

import feign.remoting.config.Configs;
import feign.remoting.connection.Url;
import feign.remoting.protocol.RpcProtocolV2;


/**
 * This is address parser for RPC.
 * <h3>Normal format</h3>
 * <pre>host:port?paramkey1=paramvalue1&amp;paramkey2=paramvalue2</pre>
 * 
 * <h4>Normal format example</h4>
 * <pre>127.0.0.1:12200?KEY1=VALUE1&KEY2=VALUE2</pre>
 * 
 * <h4>Illegal format example</h4>
 * <pre>
 * 127.0.0.1
 * 127.0.0.1:
 * 127.0.0.1:12200?
 * 127.0.0.1:12200?key1=
 * 127.0.0.1:12200?key1=value1&
 * </pre>
 * 
 * @author xiaomin.cxm
 * @version $Id: RpcAddressParser.java, v 0.1 Mar 11, 2016 5:56:45 PM xiaomin.cxm Exp $
 */
public class RpcAddressParser implements RemotingAddressParser {
	
    /**
     * @see com.alipay.remoting.RemotingAddressParser#parse(java.lang.String)
     */
    @Override
    public Url parse(String url) {
        if (StringUtils.isBlank(url)) {
            throw new IllegalArgumentException("Illegal format address string [" + url
                                               + "], should not be blank! ");
        }
        //尝试从Cache中获取之前解析过的URL对象
        Url parsedUrl = this.tryGet(url);
        if (null != parsedUrl) {
            return parsedUrl;
        }
        //无法从Cache中获取到URL对象
        String ip = null;
        String port = null;
        Properties properties = null;

        int size = url.length();
        int pos = 0;
        for (int i = 0; i < size; ++i) {
        	//查找到url字符串中的:字符
            if (COLON == url.charAt(i)) {
            	//找到ip地址的字符串，即：截取从0到:字符间的字符串
                ip = url.substring(pos, i);
                pos = i;
                //不支持:字符，在URL字符串的末尾
                if (i == size - 1) {
                    throw new IllegalArgumentException("Illegal format address string [" + url
                                                       + "], should not end with COLON[:]! ");
                }
                break;
            }
            //如果没有:字符，则抛出异常
            if (i == size - 1) {
                throw new IllegalArgumentException("Illegal format address string [" + url
                                                   + "], must have one COLON[:]! ");
            }
        }

        for (int i = pos; i < size; ++i) {
        	//查找?字符
            if (QUES == url.charAt(i)) {
            	//获取port字符串
                port = url.substring(pos + 1, i);
                pos = i;
                if (i == size - 1) {
                    //如果字符串是以?结尾，则抛出异常
                    throw new IllegalArgumentException("Illegal format address string [" + url
                                                       + "], should not end with QUES[?]! ");
                }
                break;
            }
            //不是以?结尾
            if (i == size - 1) {
                port = url.substring(pos + 1, i + 1);
                pos = size;
            }
        }

        //解析URL字符串中的properties参数
        if (pos < (size - 1)) {
            properties = new Properties();
            while (pos < (size - 1)) {
                String key = null;
                String value = null;
                for (int i = pos; i < size; ++i) {
                    if (EQUAL == url.charAt(i)) {
                        key = url.substring(pos + 1, i);
                        pos = i;
                        if (i == size - 1) {
                            // should not end with EQUAL
                            throw new IllegalArgumentException(
                                "Illegal format address string [" + url
                                        + "], should not end with EQUAL[=]! ");
                        }
                        break;
                    }
                    if (i == size - 1) {
                        // must have one EQUAL
                        throw new IllegalArgumentException("Illegal format address string [" + url
                                                           + "], must have one EQUAL[=]! ");
                    }
                }
                for (int i = pos; i < size; ++i) {
                    if (AND == url.charAt(i)) {
                        value = url.substring(pos + 1, i);
                        pos = i;
                        if (i == size - 1) {
                            // should not end with AND
                            throw new IllegalArgumentException("Illegal format address string ["
                                                               + url
                                                               + "], should not end with AND[&]! ");
                        }
                        break;
                    }
                    // end without more AND
                    if (i == size - 1) {
                        value = url.substring(pos + 1, i + 1);
                        pos = size;
                    }
                }
                properties.put(key, value);
            }
        }
        parsedUrl = new Url(url, ip, Integer.parseInt(port), properties);
        
        this.initUrlArgs(parsedUrl);
        //将解析后的url对象，添加到URL的缓存cache中
        Url.parsedUrls.put(url, new SoftReference<Url>(parsedUrl));
        return parsedUrl;
    }

    /**
     * @see com.alipay.remoting.RemotingAddressParser#parseUniqueKey(java.lang.String)
     */
    @Override
    public String parseUniqueKey(String url) {
        boolean illegal = false;
        if (StringUtils.isBlank(url)) {
            illegal = true;
        }

        String uniqueKey = StringUtils.EMPTY;
        String addr = url.trim();
        String[] sectors = StringUtils.split(addr, QUES);
        if (!illegal && sectors.length == 2 && StringUtils.isNotBlank(sectors[0])) {
            uniqueKey = sectors[0].trim();
        } else {
            illegal = true;
        }

        if (illegal) {
            throw new IllegalArgumentException("Illegal format address string: " + url);
        }
        return uniqueKey;
    }

    //从url地址中解析参数值
    @Override
    public String parseProperty(String addr, String propKey) {
        if (addr.contains("?") && !addr.endsWith("?")) {
            String part = addr.split("\\?")[1];
            for (String item : part.split("&")) {
                String[] kv = item.split("=");
                String k = kv[0];
                if (k.equals(propKey)) {
                    return kv[1];
                }
            }
        }
        return null;
    }

    /**
     * @see com.alipay.remoting.RemotingAddressParser#initUrlArgs(Url)
     */
    @Override
    public void initUrlArgs(Url url) {
    	//从URL对象中，获取_CONNECTTIMEOUT属性
        String connTimeoutStr = url.getProperty(Url.CONNECT_TIMEOUT_KEY);
        //获取连接超时的默认配置，默认值为:1000毫秒
        int connTimeout = Configs.DEFAULT_CONNECT_TIMEOUT;
        //将连接超时时间，从字符串转换为整形
        if (StringUtils.isNotBlank(connTimeoutStr)) {
            if (StringUtils.isNumeric(connTimeoutStr)) {
                connTimeout = Integer.parseInt(connTimeoutStr);
            } else {
                throw new IllegalArgumentException(
                    "Url args illegal value of key [" + Url.CONNECT_TIMEOUT_KEY
                            + "] must be positive integer! The origin url is ["
                            + url.getOriginUrl() + "]");
            }
        }
        //设置URL对象的超时时间参数
        url.setConnectTimeout(connTimeout);
        //从URL对象中，获取_PROTOCOL属性
        String protocolStr = url.getProperty(Url.PROTOCOL_KEY);
        //默认是v2版本，即值为2
        byte protocol = RpcProtocolV2.PROTOCOL_CODE;
        if (StringUtils.isNotBlank(protocolStr)) {
            if (StringUtils.isNumeric(protocolStr)) {
                protocol = Byte.parseByte(protocolStr);
            } else {
                throw new IllegalArgumentException(
                    "Url args illegal value of key [" + Url.PROTOCOL_KEY
                            + "] must be positive integer! The origin url is ["
                            + url.getOriginUrl() + "]");
            }
        }
        url.setProtocol(protocol);

        //获取URL对象中的version属性
        String versionStr = url.getProperty(Url.VERSION_KEY);
        //默认是v1版本，即值为1
        byte version = RpcProtocolV2.PROTOCOL_VERSION_1;
        if (StringUtils.isNotBlank(versionStr)) {
            if (StringUtils.isNumeric(versionStr)) {
                version = Byte.parseByte(versionStr);
            } else {
                throw new IllegalArgumentException(
                    "Url args illegal value of key [" + Url.VERSION_KEY
                            + "] must be positive integer! The origin url is ["
                            + url.getOriginUrl() + "]");
            }
        }
        url.setVersion(version);

        //从URL中获取_CONNECTIONNUM参数值，该参数用来表示当前URL最多能创建多少个Connection对象
        String connNumStr = url.getProperty(Url.CONNECTION_NUM_KEY);
        //默认配置是：每个URL创建1个Connection对象
        int connNum = Configs.DEFAULT_CONN_NUM_PER_URL;
        if (StringUtils.isNotBlank(connNumStr)) {
            if (StringUtils.isNumeric(connNumStr)) {
                connNum = Integer.parseInt(connNumStr);
            } else {
                throw new IllegalArgumentException(
                    "Url args illegal value of key [" + Url.CONNECTION_NUM_KEY
                            + "] must be positive integer! The origin url is ["
                            + url.getOriginUrl() + "]");
            }
        }
        url.setConnNum(connNum);

        //从URL中获取_CONNECTIONWARMUP参数值，该参数用来表示是否预先针对当前URL创建Connection对象
        String connWarmupStr = url.getProperty(Url.CONNECTION_WARMUP_KEY);
        //默认配置是：不开启WARMUP
        boolean connWarmup = false;
        if (StringUtils.isNotBlank(connWarmupStr)) {
            connWarmup = Boolean.parseBoolean(connWarmupStr);
        }
        url.setConnWarmup(connWarmup);
    }

    /**
     * try get from cache
     * 
     * @param url
     * @return
     */
    private Url tryGet(String url) {
        SoftReference<Url> softRef = Url.parsedUrls.get(url);
        return (null == softRef) ? null : softRef.get();
    }
}