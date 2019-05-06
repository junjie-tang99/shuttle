package feign.remoting.command;

import feign.remoting.enumerate.CommandCode;
import feign.remoting.protocol.ProtocolCode;
import feign.remoting.util.IDGenerator;

/**
 * Heart beat.
 * 
 * @author jiangping
 * @version $Id: HeartbeatCommand.java, v 0.1 2015-9-10 AM9:46:36 tao Exp $
 */
public class HeartbeatCommand extends RequestCommand {

    /** For serialization  */
    private static final long serialVersionUID = 4949981019109517725L;

    /**
     * Construction.
     */
    public HeartbeatCommand() {
        super(CommandCode.HEARTBEAT);
        this.setId(IDGenerator.nextId());
    }



}