package fr.an.tools.jdbclogger.calllogger;

import java.util.Collection;
import java.util.Set;

/**
 * DefaultCallMsgInfoFilter
 * filter supporting minDuration, prefix of message, exact message  
 */
public class DefaultCallMsgInfoFilter implements CallMsgInfoFilter {

    protected int minDurationMillis;

    protected Set<String> setDiscardMsg; 

    protected Collection<String> lsDiscardMsgStartWith; 

    // --------------------------------------------------------------------------------------------

    public DefaultCallMsgInfoFilter(int minDurationMillis, Set<String> setDiscardMsg, Collection<String> lsDiscardMsgStartWith) {
        this.minDurationMillis = minDurationMillis;
        this.setDiscardMsg = setDiscardMsg;
        this.lsDiscardMsgStartWith = lsDiscardMsgStartWith;
    }

    // --------------------------------------------------------------------------------------------

    /**
     * test if msg is accepted/skipped for logs
     */
    public boolean acceptMsg(CallMsgInfo callMsgInfo) {
        final String msg = callMsgInfo.getMsgPre();
        if (callMsgInfo.isIgnoreMsg()) {
            return false;
        }
        if (callMsgInfo.getEndTimeNanos() != 0 && callMsgInfo.getMillis() < minDurationMillis) {
            return false;
        }
        if (setDiscardMsg != null) {
            if (setDiscardMsg.contains(msg)) {
                return false;
            }
        }
        if (lsDiscardMsgStartWith != null) {
            for (String item : lsDiscardMsgStartWith) {
                if (msg.startsWith(item)) {
                    return false;
                }
            }
        }
        return true;
    }

    public int getMinDurationMillis() {
        return minDurationMillis;
    }

    public void setMinDurationMillis(int p) {
        this.minDurationMillis = p;
    }


}
