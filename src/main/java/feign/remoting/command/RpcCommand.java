package feign.remoting.command;

import java.io.Serializable;

import feign.remoting.config.ConfigManager;
import feign.remoting.enumerate.CommandCode;
import feign.remoting.enumerate.RpcDeserializeLevel;
import feign.remoting.exception.DeserializationException;
import feign.remoting.exception.SerializationException;
import feign.remoting.invoke.InvokeContext;
import feign.remoting.protocol.ProtocolCode;
import feign.remoting.switches.ProtocolSwitch;


/**
 * Remoting command.
 * 
 */
public abstract class RpcCommand implements RemotingCommand {

	 /** For serialization  */
    private static final long serialVersionUID = -3570261012462596503L;

    //Comand的种类标识，例如：Heartbeat，Request，Response等
    private CommandCode       cmdCode;
    //Command的版本号
    private byte              version          = 0x1;
    //Comand的类型，例如：REQUEST，REQUEST_ONEWAY，RESPONSE等
    private byte              type;
    //序列化器，默认是：1，使用Hessian进行序列化
    //Notice: this can not be changed after initialized at runtime.
    private byte serializer = ConfigManager.serializer;
    //协议开关
    private ProtocolSwitch    protocolSwitch   = new ProtocolSwitch();
    //请求ID
    private int               id;
    
    /** The length of clazz */
    private short             clazzLength      = 0;
    private short             headerLength     = 0;
    private int               contentLength    = 0;
    
    //Command指定的业务Class
    private byte[]            clazz;
    //Command的header
    private byte[]            header;
    //Command的内容
    private byte[]            content;
    
    /** invoke context of each rpc command. */
    private InvokeContext     invokeContext;

    public RpcCommand() {
    }

    public RpcCommand(byte type) {
        this();
        this.type = type;
    }

    public RpcCommand(CommandCode cmdCode) {
        this();
        this.cmdCode = cmdCode;
    }

    public RpcCommand(byte type, CommandCode cmdCode) {
        this(cmdCode);
        this.type = type;
    }

    public RpcCommand(byte version, byte type, CommandCode cmdCode) {
        this(type, cmdCode);
        this.version = version;
    }

    //序列化类名、头部以及内容
    @Override
    public void serialize() throws SerializationException {
        this.serializeClazz();
        this.serializeHeader(this.invokeContext);
        this.serializeContent(this.invokeContext);
    }

    //反序列化类名、头部以及内容
    @Override
    public void deserialize() throws DeserializationException {
        this.deserializeClazz();
        this.deserializeHeader(this.invokeContext);
        this.deserializeContent(this.invokeContext);
    }

    //如果mask <= RpcDeserializeLevel.DESERIALIZE_CLAZZ，那么只反序列化clazz
    //如果mask <= RpcDeserializeLevel.DESERIALIZE_HEADER，那么只反序列化clazz + header
    //如果mask <= RpcDeserializeLevel.DESERIALIZE_ALL，那么反序列化clazz + header + content
    public void deserialize(long mask) throws DeserializationException {
        if (mask <= RpcDeserializeLevel.DESERIALIZE_CLAZZ.value()) {
            this.deserializeClazz();
        } else if (mask <= RpcDeserializeLevel.DESERIALIZE_HEADER.value()) {
            this.deserializeClazz();
            this.deserializeHeader(this.getInvokeContext());
        } else if (mask <= RpcDeserializeLevel.DESERIALIZE_ALL.value()) {
            this.deserialize();
        }
    }

    //=================================================
    //提供对序列化及反序列化的默认实现，子类要覆盖相关方法
	//=================================================
    public void serializeClazz() throws SerializationException {
    }
    public void deserializeClazz() throws DeserializationException {
    }
    public void serializeHeader(InvokeContext invokeContext) throws SerializationException {
    }
    public void deserializeHeader(InvokeContext invokeContext) throws DeserializationException {
    }
    @Override
    public void serializeContent(InvokeContext invokeContext) throws SerializationException {
    }
    @Override
    public void deserializeContent(InvokeContext invokeContext) throws DeserializationException {
    }

    @Override
    public ProtocolCode getProtocolCode() {
        return ProtocolCode.fromBytes((byte)2);
    }

    @Override
    public CommandCode getCmdCode() {
        return cmdCode;
    }

    @Override
    public InvokeContext getInvokeContext() {
        return invokeContext;
    }

    @Override
    public byte getSerializer() {
        return serializer;
    }

    @Override
    public ProtocolSwitch getProtocolSwitch() {
        return protocolSwitch;
    }

    public void setCmdCode(CommandCode cmdCode) {
        this.cmdCode = cmdCode;
    }

    public byte getVersion() {
        return version;
    }

    public void setVersion(byte version) {
        this.version = version;
    }

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public void setSerializer(byte serializer) {
        this.serializer = serializer;
    }

    public void setProtocolSwitch(ProtocolSwitch protocolSwitch) {
        this.protocolSwitch = protocolSwitch;
    }

    @Override
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public byte[] getHeader() {
        return header;
    }

    public void setHeader(byte[] header) {
        if (header != null) {
            this.header = header;
            this.headerLength = (short) header.length;
        }
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        if (content != null) {
            this.content = content;
            this.contentLength = content.length;
        }
    }

    public short getHeaderLength() {
        return headerLength;
    }

    public int getContentLength() {
        return contentLength;
    }

    public short getClazzLength() {
        return clazzLength;
    }

    public byte[] getClazz() {
        return clazz;
    }

    public void setClazz(byte[] clazz) {
        if (clazz != null) {
            this.clazz = clazz;
            this.clazzLength = (short) clazz.length;
        }
    }

    public void setInvokeContext(InvokeContext invokeContext) {
        this.invokeContext = invokeContext;
    }
	
}