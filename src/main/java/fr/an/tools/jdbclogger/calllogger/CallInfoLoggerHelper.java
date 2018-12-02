package fr.an.tools.jdbclogger.calllogger;



/**
 * helper class for filling callInfo with pre/post info, and logging with CallLogger
 */
public class CallInfoLoggerHelper {

    /** current callInfo to fill and log */
    private CallMsgInfo callInfo;

    /** logger for pre/post */
    private final CallMsgInfoLogger callInfoPrePostLogger;

    // constructor
    // --------------------------------------------------------------------------------------------

    /** full ctor */
    public CallInfoLoggerHelper(CallMsgInfo callInfo, CallMsgInfoLogger logger) {
        this.callInfo = callInfo;
        this.callInfoPrePostLogger = logger;
    }

    // public
    // --------------------------------------------------------------------------------------------

    /** fill + call logger.logPre() */
    public void pre(String meth, String arg) {
        callInfo.setPre("", meth, arg);
        if (callInfoPrePostLogger != null) {
            try {
                callInfoPrePostLogger.logPre(callInfo); // (meth, sql, timePre);
            } catch (Throwable ex) {
            	logIgnoreInternalEx("db logger logPost failed", ex);
            }
        }
    }

    /** fill + call logger.logPost() */
    public void postVoid() {
        callInfo.setPostVoid();
        if (callInfoPrePostLogger != null) {
            try {
                callInfoPrePostLogger.logPost(callInfo);
            } catch (Exception ex) {
            	logIgnoreInternalEx("db logger logPost failed", ex);
            }
        }
    }

    /** fill + call logger.logPost() */
    public void postRes(Object res) {
        callInfo.setPostRes(res);
        if (callInfoPrePostLogger != null) {
            try {
                callInfoPrePostLogger.logPost(callInfo);
            } catch (Exception ex) {
            	logIgnoreInternalEx("db logger logPost failed", ex);
            }
        }
    }

    /** fill + call logger.logPost() */
    public void postDefaultRes(Object res) {
        callInfo.setPostDefaultRes(res);
        if (callInfoPrePostLogger != null) {
            try {
                callInfoPrePostLogger.logPost(callInfo);
            } catch (Exception ex) {
            	logIgnoreInternalEx("db logger logPost failed", ex);
            }
        }
    }

    /** fill + call logger.logPost() */
    public void postDefaultRes() {
        postDefaultRes(null);
    }

    /** fill + call logger.logPostEx() */
    public void postEx(Throwable ex) {
        callInfo.setPostEx(ex);
        if (callInfoPrePostLogger != null) {
            try {
                callInfoPrePostLogger.logPostEx(callInfo);
            } catch (Exception ex2) {
            	logIgnoreInternalEx("db logger logPost failed", ex);
            }
        }
    }

    /** fill + call logger.logPre() */
    public void preIgnore(String meth, String arg) {
        callInfo.setPre("", meth, arg);
        if (callInfoPrePostLogger != null) {
            try {
                callInfoPrePostLogger.preIgnoreLog(callInfo); // (meth, sql, timePre);
            } catch (Exception ex) {
            	logIgnoreInternalEx("db logger preIgnoreLog failed", ex);
            }
        }
    }

    /** fill + call logger.postIgnoreLog() */
    public void postIgnoreVoid() {
        callInfo.setIgnoreMsg(true);
        if (callInfoPrePostLogger != null) {
            callInfoPrePostLogger.postIgnoreLog(callInfo);
        }
    }

    /** fill + call logger.postIgnoreLog() */
    public void postIgnoreRes(Object res) {
        callInfo.setIgnoreMsg(true);
        if (callInfoPrePostLogger != null) {
            callInfoPrePostLogger.postIgnoreLog(callInfo);
        }
    }

    private void logIgnoreInternalEx(String msg, Throwable ex) {
    	System.err.println(msg);
    	ex.printStackTrace(System.err);
    }
    
    public void logPostFetch(CallMsgInfo msgInfo) {
    	if (callInfoPrePostLogger != null) {
            callInfoPrePostLogger.logPostFetch(msgInfo);
        }
    }
}
