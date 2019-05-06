package feign.remoting.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * IDGenerator is used for generating request id in integer form.
 * 
 * @author jiangping
 * @version $Id: IDGenerator.java, v 0.1 2015-9-23 PM5:28:58 tao Exp $
 */
public class IDGenerator {
    private static final AtomicInteger id = new AtomicInteger(0);

    /**
     * generate the next id
     * 
     * @return
     */
    public static int nextId() {
        return id.incrementAndGet();
    }

    public static void resetId() {
        id.set(0);
    }
}
