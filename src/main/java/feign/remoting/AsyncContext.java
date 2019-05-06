package feign.remoting;

/**
 * Async context for biz.
 * 
 * @author xiaomin.cxm
 * @version $Id: AsyncContext.java, v 0.1 May 19, 2016 2:19:05 PM xiaomin.cxm Exp $
 */
public interface AsyncContext {
    /**
     * send response back
     * 
     * @param responseObject
     */
    void sendResponse(Object responseObject);
}
