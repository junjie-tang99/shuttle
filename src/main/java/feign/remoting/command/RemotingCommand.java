package feign.remoting.command;

import java.io.Serializable;

import feign.remoting.enumerate.CommandCode;
import feign.remoting.exception.DeserializationException;
import feign.remoting.exception.SerializationException;
import feign.remoting.invoke.InvokeContext;
import feign.remoting.protocol.ProtocolCode;
import feign.remoting.switches.ProtocolSwitch;


public interface RemotingCommand extends Serializable {
    /**
     * Get the code of the protocol that this command belongs to
     *
     * @return protocol code
     */
    ProtocolCode getProtocolCode();

    /**
     * Get the command code for this command
     *
     * @return command code
     */
    CommandCode getCmdCode();

    /**
     * Get the id of the command
     *
     * @return an int value represent the command id
     */
    int getId();

    /**
     * Get invoke context for this command
     *
     * @return context
     */
    InvokeContext getInvokeContext();

    /**
     * Get serializer type for this command
     *
     * @return
     */
    byte getSerializer();

    /**
     * Get the protocol switch status for this command
     *
     * @return
     */
    ProtocolSwitch getProtocolSwitch();

    /**
     * Serialize all parts of remoting command
     *
     * @throws SerializationException
     */
    void serialize() throws SerializationException;

    /**
     * Deserialize all parts of remoting command
     *
     * @throws DeserializationException
     */
    void deserialize() throws DeserializationException;

    /**
     * Serialize content of remoting command
     *
     * @param invokeContext
     * @throws SerializationException
     */
    void serializeContent(InvokeContext invokeContext) throws SerializationException;

    /**
     * Deserialize content of remoting command
     *
     * @param invokeContext
     * @throws DeserializationException
     */
    void deserializeContent(InvokeContext invokeContext) throws DeserializationException;
}
