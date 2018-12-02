package fr.an.tools.jdbclogger.calllogger;

/**
 * CallLogger
 * logger for call methods on Statement object
 */
public interface CallMsgInfoLogger {

    public void log(String msg);

    public void logPre(CallMsgInfo stmt);

    public void logPost(CallMsgInfo stmt);

    public void logPostEx(CallMsgInfo stmt);

    public void preIgnoreLog(CallMsgInfo msgInfo);

    public void postIgnoreLog(CallMsgInfo msgInfo);

    /** used to force/flush logStatementPre while only 1 line would be emited by stmt Pre+Post */
    public void logPendingCall(long minWaitingMillis);

    public void logPostFetch(CallMsgInfo msgInfo);

}
