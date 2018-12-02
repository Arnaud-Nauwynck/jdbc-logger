package fr.an.tools.jdbclogger.calllogger.templaterecorder;

/**
 * interface to simplify java stack (StackTraceElement[]) to print / record info.
 */
public interface StackTraceSimplifier {

    public StackTraceElement[] simplifyStackTrace(StackTraceElement[] stack);

}
