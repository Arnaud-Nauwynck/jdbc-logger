package fr.an.tools.jdbclogger.calllogger;

/**
 * filter interface for CallMsgInfo 
 */
public interface CallMsgInfoFilter {

    public boolean acceptMsg(CallMsgInfo callMsgInfo);

}
