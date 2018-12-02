package fr.an.tools.jdbclogger.calllogger;

import java.io.PrintWriter;

public interface CallMsgInfoFormat {

    public abstract void format(PrintWriter out, CallMsgInfo p);

}
