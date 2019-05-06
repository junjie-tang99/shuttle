package feign.remoting.command;

import java.io.UnsupportedEncodingException;

import feign.remoting.config.Configs;
import feign.remoting.enumerate.CommandCode;
import feign.remoting.exception.DeserializationException;
import feign.remoting.exception.SerializationException;
import feign.remoting.invoke.InvokeContext;
import feign.remoting.serialization.CustomSerializer;
import feign.remoting.serialization.CustomSerializerManager;
import feign.remoting.serialization.SerializerManager;
import feign.remoting.util.IDGenerator;


/**
 * Request command for Rpc.
 * 
 * @author jiangping
 * @version $Id: RpcRequestCommand.java, v 0.1 2015-9-25 PM2:13:35 tao Exp $
 */
public class RpcRequestCommand extends RequestCommand {
    /** For serialization  */
    private static final long serialVersionUID = -4602613826188210946L;
    //请求头
    private Object            requestHeader;
    //请求的对象
    private Object            requestObject;
    //请求的业务类名
    private String            requestClass;

    private CustomSerializer  customSerializer;
    
    //请求的到达时间
    private transient long    arriveTime       = -1;

    /**
     * create request command without id
     */
    public RpcRequestCommand() {
        super(CommandCode.RPC_REQUEST);
    }

    /**
     * create request command with id and request object
     * @param request request object
     */
    public RpcRequestCommand(Object request) {
        super(CommandCode.RPC_REQUEST);
        this.requestObject = request;
        this.setId(IDGenerator.nextId());
    }

    //将请求类的名称序列成byte数组
    //在序列化时，需要指定字符集，否则会使用系统默认的字符集进行序列化
    //当服务器之间的字符集不一致时，会出现乱码的情况
    @Override
    public void serializeClazz() throws SerializationException {
        if (this.requestClass != null) {
            try {
                byte[] clz = this.requestClass.getBytes(Configs.DEFAULT_CHARSET);
                this.setClazz(clz);
            } catch (UnsupportedEncodingException e) {
                throw new SerializationException("Unsupported charset: " + Configs.DEFAULT_CHARSET,
                    e);
            }
        }
    }

    //将请求类的名称，从byte数组反序列化为字符串
    //在反序列化时，需要指定字符集，否则会使用系统默认的字符集进行序列化
    //当服务器之间的字符集不一致时，会出现乱码的情况    
    @Override
    public void deserializeClazz() throws DeserializationException {
        if (this.getClazz() != null && this.getRequestClass() == null) {
            try {
                this.setRequestClass(new String(this.getClazz(), Configs.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                throw new DeserializationException("Unsupported charset: "
                                                   + Configs.DEFAULT_CHARSET, e);
            }
        }
    }

    @Override
    public void serializeHeader(InvokeContext invokeContext) throws SerializationException {
        if (this.getCustomSerializer() != null) {
            try {
                this.getCustomSerializer().serializeHeader(this, invokeContext);
            } catch (SerializationException e) {
                throw e;
            } catch (Exception e) {
                throw new SerializationException(
                    "Exception caught when serialize header of rpc request command!", e);
            }
        }
    }

    @Override
    public void deserializeHeader(InvokeContext invokeContext) throws DeserializationException {
        if (this.getHeader() != null && this.getRequestHeader() == null) {
            if (this.getCustomSerializer() != null) {
                try {
                    this.getCustomSerializer().deserializeHeader(this);
                } catch (DeserializationException e) {
                    throw e;
                } catch (Exception e) {
                    throw new DeserializationException(
                        "Exception caught when deserialize header of rpc request command!", e);
                }
            }
        }
    }

    @Override
    public void serializeContent(InvokeContext invokeContext) throws SerializationException {
        if (this.requestObject != null) {
            try {
                if (this.getCustomSerializer() != null
                    && this.getCustomSerializer().serializeContent(this, invokeContext)) {
                    return;
                }
                //使用Hession或者其他的序列化器，来序列化内容
                this.setContent(SerializerManager.getSerializer(this.getSerializer()).serialize(this.requestObject));
            } catch (SerializationException e) {
                throw e;
            } catch (Exception e) {
                throw new SerializationException(
                    "Exception caught when serialize content of rpc request command!", e);
            }
        }
    }

    @Override
    public void deserializeContent(InvokeContext invokeContext) throws DeserializationException {
        if (this.getRequestObject() == null) {
            try {
                if (this.getCustomSerializer() != null
                    && this.getCustomSerializer().deserializeContent(this)) {
                    return;
                }
                if (this.getContent() != null) {
                    this.setRequestObject(SerializerManager.getSerializer(this.getSerializer())
                        .deserialize(this.getContent(), this.requestClass));
                }
            } catch (DeserializationException e) {
                throw e;
            } catch (Exception e) {
                throw new DeserializationException(
                    "Exception caught when deserialize content of rpc request command!", e);
            }
        }
    }

    /**
     * Getter method for property <tt>requestObject</tt>.
     * 
     * @return property value of requestObject
     */
    public Object getRequestObject() {
        return requestObject;
    }

    /**
     * Setter method for property <tt>requestObject</tt>.
     * 
     * @param requestObject value to be assigned to property requestObject
     */
    public void setRequestObject(Object requestObject) {
        this.requestObject = requestObject;
    }

    /**
     * Getter method for property <tt>requestHeader</tt>.
     * 
     * @return property value of requestHeader
     */
    public Object getRequestHeader() {
        return requestHeader;
    }

    /**
     * Setter method for property <tt>requestHeader</tt>.
     * 
     * @param requestHeader value to be assigned to property requestHeader
     */
    public void setRequestHeader(Object requestHeader) {
        this.requestHeader = requestHeader;
    }

    /**
     * Getter method for property <tt>requestClass</tt>.
     * 
     * @return property value of requestClass
     */
    public String getRequestClass() {
        return requestClass;
    }

    /**
     * Setter method for property <tt>requestClass</tt>.
     * 
     * @param requestClass value to be assigned to property requestClass
     */
    public void setRequestClass(String requestClass) {
        this.requestClass = requestClass;
    }

    /**
     * Getter method for property <tt>customSerializer</tt>.
     * 
     * @return property value of customSerializer
     */
    public CustomSerializer getCustomSerializer() {
        if (this.customSerializer != null) {
            return customSerializer;
        }
        //如果请求的类名不为空，那么获取对应的序列化器
        if (this.requestClass != null) {
            this.customSerializer = CustomSerializerManager.getCustomSerializer(this.requestClass);
        }
        //如果无法通过类名获取序列化器，那么使用CommandCode获取序列化器
        if (this.customSerializer == null) {
            this.customSerializer = CustomSerializerManager.getCustomSerializer(this.getCmdCode());
        }
        return this.customSerializer;
    }

    /**
     * Getter method for property <tt>arriveTime</tt>.
     * 
     * @return property value of arriveTime
     */
    public long getArriveTime() {
        return arriveTime;
    }

    /**
     * Setter method for property <tt>arriveTime</tt>.
     * 
     * @param arriveTime value to be assigned to property arriveTime
     */
    public void setArriveTime(long arriveTime) {
        this.arriveTime = arriveTime;
    }
}

