package fr.an.tools.jdbclogger.calllogger;

import java.util.ArrayList;
import java.util.List;

/**
 * broadcaster, composite design pattern for CallMsgInfoLogger
 */
public class CallMsgInfoListLogger implements CallMsgInfoLogger {

    private List<CallMsgInfoLogger> appenders = new ArrayList<>();

    //-------------------------------------------------------------------------

    public CallMsgInfoListLogger() {
    }

    // ------------------------------------------------------------------------
    
    public void addAppender(CallMsgInfoLogger p) {
        appenders.add(p);
    }

    public void removeAppender(CallMsgInfoLogger p) {
        appenders.remove(p);
    }
    
    public List<CallMsgInfoLogger> getAppenders() {
    	return appenders;
    }
    
    // -------------------------------------------------------------------------

    @Override
    public void log(String msg) {
        for (CallMsgInfoLogger elt : appenders) {
            elt.log(msg);
        }
    }

    @Override
    public void logPendingCall(long minWaitingMillis) {
        for (CallMsgInfoLogger elt : appenders) {
            elt.logPendingCall(minWaitingMillis);
        }
    }

    @Override
    public void logPost(CallMsgInfo stmt) {
        for (CallMsgInfoLogger elt : appenders) {
            elt.logPost(stmt);
        }
    }

    @Override
    public void logPostEx(CallMsgInfo stmt) {
        for (CallMsgInfoLogger elt : appenders) {
            elt.logPostEx(stmt);
        }
    }

    @Override
    public void logPre(CallMsgInfo stmt) {
        for (CallMsgInfoLogger elt : appenders) {
            elt.logPre(stmt);
        }
    }

    @Override
    public void postIgnoreLog(CallMsgInfo msgInfo) {
        for (CallMsgInfoLogger elt : appenders) {
            elt.postIgnoreLog(msgInfo);
        }
    }

    @Override
    public void preIgnoreLog(CallMsgInfo msgInfo) {
        for (CallMsgInfoLogger elt : appenders) {
            elt.preIgnoreLog(msgInfo);
        }
    }

    @Override
    public void logPostFetch(CallMsgInfo msgInfo) {
    	for (CallMsgInfoLogger elt : appenders) {
            elt.logPostFetch(msgInfo);
        }
    }

}
