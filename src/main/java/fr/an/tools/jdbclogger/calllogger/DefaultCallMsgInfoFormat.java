package fr.an.tools.jdbclogger.calllogger;

import java.io.PrintWriter;

/**
 * Default Text Formatter class for CallInfoMsg
 */
public class DefaultCallMsgInfoFormat implements CallMsgInfoFormat {

    public static final CallMsgInfoFormat fmtNone = new DefaultCallMsgInfoFormat(false,
                                                                                 Integer.MAX_VALUE,
                                                                                 false,
                                                                                 false,
                                                                                 false);
    public static final CallMsgInfoFormat fmtAll = new DefaultCallMsgInfoFormat(true, -1, true, true, true);
    public static final CallMsgInfoFormat fmtDefault = new DefaultCallMsgInfoFormat(true, 10, true, false, true);

    public static final CallMsgInfoFormat fmtErrorShort = new DefaultCallMsgInfoFormat(false,
                                                                                       Integer.MAX_VALUE,
                                                                                       false,
                                                                                       false,
                                                                                       false);
    public static final CallMsgInfoFormat fmtErrorLong = new DefaultCallMsgInfoFormat(false,
                                                                                      Integer.MAX_VALUE,
                                                                                      false,
                                                                                      false,
                                                                                      true);

    public static CallMsgInfoFormat fmtToString = fmtDefault;

    // --------------------------------------------------------------------------------------------

    boolean printStatus;
    int minMillisForPrint;
    boolean printTimeWithMillis;
    boolean forcePrintTime;
    boolean dumpExceptionStack;

    // --------------------------------------------------------------------------------------------

    public DefaultCallMsgInfoFormat(boolean printStatus,
                                    int minMillisForPrint,
                                    boolean printTimeWithMillis,
                                    boolean forcePrintTime,
                                    boolean dumpExceptionStack) {
        this.printStatus = printStatus;
        this.minMillisForPrint = minMillisForPrint;
        this.printTimeWithMillis = printTimeWithMillis;
        this.forcePrintTime = forcePrintTime;
        this.dumpExceptionStack = dumpExceptionStack;
    }

    /** implements CallMsgInfoFormat */
    @Override
    public void format(PrintWriter out, CallMsgInfo p) {
        long startTimeNanos = p.getStartTimeNanos();
        long endTimeNanos = p.getEndTimeNanos();

        if (p.getCallerName() != null) {
            out.print(p.getCallerName());
            out.print(" ");
        }

        String msg;
        if (endTimeNanos != 0) {
            // finished
        	msg = p.getMsgPost();
        } else if (startTimeNanos != 0) {
            // started, still pending
            msg = p.getMsgPre();
        } else { // startTime == 0 && endTime == 0 
            // not started?
            msg = p.getMsg();
        }

        if (msg != null) {
            out.print(msg);
            out.print(" ");
        }

        Throwable ex = p.getException();
        if (ex != null) {
            out.print("ERROR ");
            if (dumpExceptionStack) {
                ex.printStackTrace(out);
            } else {
                out.print(ex.getMessage() + " ");
            }
        }
    }

}
