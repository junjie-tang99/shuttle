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


/**
 * Response command for Rpc.
 * 
 * @author jiangping
 * @version $Id: RpcResponseCommand.java, v 0.1 2015-9-25 PM2:15:41 tao Exp $
 */
public class RpcResponseCommand extends ResponseCommand {
    /** For serialization  */
    private static final long serialVersionUID = 5667111367880018776L;
    //返回对象
    private Object            responseObject;
    //返回对象的Class名称
    private String            responseClass;

    private CustomSerializer  customSerializer;
    private Object            responseHeader;

    private String            errorMsg;

    public RpcResponseCommand() {
        super(CommandCode.RPC_RESPONSE);
    }

    public RpcResponseCommand(Object response) {
        super(CommandCode.RPC_RESPONSE);
        this.responseObject = response;
    }

    public RpcResponseCommand(int id, Object response) {
        super(CommandCode.RPC_RESPONSE, id);
        this.responseObject = response;
    }

    /**
     * Getter method for property <tt>responseObject</tt>.
     * 
     * @return property value of responseObject
     */
    public Object getResponseObject() {
        return responseObject;
    }

    /**
     * Setter method for property <tt>responseObject</tt>.
     * 
     * @param response value to be assigned to property responseObject
     */
    public void setResponseObject(Object response) {
        this.responseObject = response;
    }

    @Override
    public void serializeClazz() throws SerializationException {
        if (this.getResponseClass() != null) {
            try {
                byte[] clz = this.getResponseClass().getBytes(Configs.DEFAULT_CHARSET);
                this.setClazz(clz);
            } catch (UnsupportedEncodingException e) {
                throw new SerializationException("Unsupported charset: " + Configs.DEFAULT_CHARSET,
                    e);
            }
        }
    }

    @Override
    public void deserializeClazz() throws DeserializationException {
        if (this.getClazz() != null && this.getResponseClass() == null) {
            try {
                this.setResponseClass(new String(this.getClazz(), Configs.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                throw new DeserializationException("Unsupported charset: "
                                                   + Configs.DEFAULT_CHARSET, e);
            }
        }
    }

    @Override
    public void serializeContent(InvokeContext invokeContext) throws SerializationException {
        if (this.getResponseObject() != null) {
            try {
                if (this.getCustomSerializer() != null
                    && this.getCustomSerializer().serializeContent(this)) {
                    return;
                }

                this.setContent(SerializerManager.getSerializer(this.getSerializer()).serialize(
                    this.responseObject));
            } catch (SerializationException e) {
                throw e;
            } catch (Exception e) {
                throw new SerializationException(
                    "Exception caught when serialize content of rpc response command!", e);
            }
        }
    }

    @Override
    public void deserializeContent(InvokeContext invokeContext) throws DeserializationException {
        if (this.getResponseObject() == null) {
            try {
                if (this.getCustomSerializer() != null
                    && this.getCustomSerializer().deserializeContent(this, invokeContext)) {
                    return;
                }
                if (this.getContent() != null) {
                    this.setResponseObject(SerializerManager.getSerializer(this.getSerializer())
                        .deserialize(this.getContent(), this.responseClass));
                }
            } catch (DeserializationException e) {
                throw e;
            } catch (Exception e) {
                throw new DeserializationException(
                    "Exception caught when deserialize content of rpc response command!", e);
            }
        }

    }

    @Override
    public void serializeHeader(InvokeContext invokeContext) throws SerializationException {
        if (this.getCustomSerializer() != null) {
            try {
                this.getCustomSerializer().serializeHeader(this);
            } catch (SerializationException e) {
                throw e;
            } catch (Exception e) {
                throw new SerializationException(
                    "Exception caught when serialize header of rpc response command!", e);
            }
        }
    }

    @Override
    public void deserializeHeader(InvokeContext invokeContext) throws DeserializationException {
        if (this.getHeader() != null && this.getResponseHeader() == null) {
            if (this.getCustomSerializer() != null) {
                try {
                    this.getCustomSerializer().deserializeHeader(this, invokeContext);
                } catch (DeserializationException e) {
                    throw e;
                } catch (Exception e) {
                    throw new DeserializationException(
                        "Exception caught when deserialize header of rpc response command!", e);
                }
            }
        }
    }

    /**
     * Getter method for property <tt>responseClass</tt>.
     * 
     * @return property value of responseClass
     */
    public String getResponseClass() {
        return responseClass;
    }

    /**
     * Setter method for property <tt>responseClass</tt>.
     * 
     * @param responseClass value to be assigned to property responseClass
     */
    public void setResponseClass(String responseClass) {
        this.responseClass = responseClass;
    }

    /**
     * Getter method for property <tt>responseHeader</tt>.
     * 
     * @return property value of responseHeader
     */
    public Object getResponseHeader() {
        return responseHeader;
    }

    /**
     * Setter method for property <tt>responseHeader</tt>.
     * 
     * @param responseHeader value to be assigned to property responseHeader
     */
    public void setResponseHeader(Object responseHeader) {
        this.responseHeader = responseHeader;
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
        if (this.responseClass != null) {
            this.customSerializer = CustomSerializerManager.getCustomSerializer(this.responseClass);
        }
        if (this.customSerializer == null) {
            this.customSerializer = CustomSerializerManager.getCustomSerializer(this.getCmdCode());
        }
        return this.customSerializer;
    }

    /**
     * Getter method for property <tt>errorMsg</tt>.
     * 
     * @return property value of errorMsg
     */
    public String getErrorMsg() {
        return errorMsg;
    }

    /**
     * Setter method for property <tt>errorMsg</tt>.
     * 
     * @param errorMsg value to be assigned to property errorMsg
     */
    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }
}
