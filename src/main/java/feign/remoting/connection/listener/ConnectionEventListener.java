package feign.remoting.connection.listener;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import feign.remoting.connection.Connection;
import feign.remoting.connection.processor.ConnectionEventProcessor;
import feign.remoting.enumerate.ConnectionEventType;



//Listen and dispatch connection events.
public class ConnectionEventListener {

    private ConcurrentHashMap<ConnectionEventType, List<ConnectionEventProcessor>> processors = new ConcurrentHashMap<ConnectionEventType, List<ConnectionEventProcessor>>(
                                                                                                  3);

    /**
     * Dispatch events.
     * 
     * @param type
     * @param remoteAddr
     * @param conn
     */
    public void onEvent(ConnectionEventType type, String remoteAddr, Connection conn) {
        List<ConnectionEventProcessor> processorList = this.processors.get(type);
        if (processorList != null) {
            for (ConnectionEventProcessor processor : processorList) {
                processor.onEvent(remoteAddr, conn);
            }
        }
    }

    /**
     * Add event processor.
     * 
     * @param type
     * @param processor
     */
    public void addConnectionEventProcessor(ConnectionEventType type,
                                            ConnectionEventProcessor processor) {
        List<ConnectionEventProcessor> processorList = this.processors.get(type);
        if (processorList == null) {
            this.processors.putIfAbsent(type, new ArrayList<ConnectionEventProcessor>(1));
            processorList = this.processors.get(type);
        }
        processorList.add(processor);
    }

}
