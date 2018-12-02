package fr.an.tools.jdbclogger.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;

/**
 * utility class for Exception
 */
public final class ExUtil {

    /** private for all static */
    private ExUtil() {
    }

    /**
     * 
     */
    public static RuntimeException wrapRuntime(Throwable ex) {
        RuntimeException res;
        if (ex instanceof RuntimeException) {
            res = (RuntimeException) ex;
        } else {
            res = new RuntimeException(ex);
        }
        return res;
    }

    /**
     * utility for Exception Stack Trace toString
     */
    public static String stackTraceToString(Throwable ex) {
        ByteArrayOutputStream sb = new ByteArrayOutputStream();
        PrintStream stream = new PrintStream(sb);
        ex.printStackTrace(stream);
        return sb.toString();
    }

    /**
     * utility for Exception Stack Trace toString
     * does not format as Exception, but as single line text
     */
    public static String stackTraceToShortPath(Throwable stackTrace) {
        ByteArrayOutputStream sb = new ByteArrayOutputStream();
        PrintStream stream = new PrintStream(sb);

        doStackTraceToShortPath(stream, stackTrace);

        Throwable cause = stackTrace.getCause();
        if (cause != null) {
            StackTraceElement[] parentStack = stackTrace.getStackTrace();
            printShortTraceAsCause(stream, cause, parentStack);
        }

        return sb.toString();
    }

    /**
     * utility for Exception Stack Trace toString
     * does not format as Exception, but as single line text
     */
    public static String stackTraceToShortPath(StackTraceElement[] traceArray) {
        ByteArrayOutputStream sb = new ByteArrayOutputStream();
        PrintStream stream = new PrintStream(sb);

        doStackTraceToShortPath(stream, traceArray);

        return sb.toString();
    }

    /**
     * utility for Exception Stack Trace toString
     * does not format as Exception, but as single line text
     */
    public static void doStackTraceToShortPath(PrintStream stream, Throwable stackTrace) {
        StackTraceElement[] traceArray = stackTrace.getStackTrace();
        doStackTraceToShortPath(stream, traceArray, 0, traceArray.length);
    }

    /**
     * utility for Exception Stack Trace toString
     * does not format as Exception, but as single line text
     */
    public static void doStackTraceToShortPath(PrintStream stream, StackTraceElement[] traceArray) {
        doStackTraceToShortPath(stream, traceArray, 0, traceArray.length);
    }

    /**
     * utility for Exception Stack Trace toString
     * does not format as Exception, but as single line text
     */
    public static void doStackTraceToShortPath(PrintStream stream,
                                               StackTraceElement[] traceArray,
                                               int fromIndex,
                                               int toIndex) {
        String prevFileName = "";
        for (int i = fromIndex; i < toIndex; i++) {
            try {
                StackTraceElement element = traceArray[i];

                String className = element.getClassName();
                // filter internal stack frames..
                if (className.startsWith("sun.ref") || className.startsWith("java.lang.")
                    || className.startsWith("java.io.") || className.indexOf("$Proxy") != -1
                    || className.startsWith("weblogic")) {
                    continue;
                }

                String fileName = element.getFileName();
                // stream.print(element.getClassName() + "." + element.getMethodName());
                if (fileName != null) {
                    if (fileName.endsWith(".java"))
                        fileName = fileName.substring(0, fileName.length() - ".java".length());
                    if (!prevFileName.equals(fileName)) {
                        stream.print(fileName);
                    } else {
                        // no repeat same file..
                    }
                    prevFileName = fileName;
                    stream.print(":");
                    stream.print(element.getLineNumber());
                } else {
                    stream.print(element.getClassName() + "." + element.getMethodName() + " (Unknown Source)");
                }
            } catch (Exception toStringEx) {
                // ignore
            }
            if (i + 1 < traceArray.length)
                stream.print("/");
        }
    }

    /**
     * utility for current Exception Stack Trace toString
     * see <code>stackTraceToShortPath(new Exception());</code> with 1 additional stack trace element for this method
     */
    public static String currentStackTraceShortPath() {
        return stackTraceToShortPath(new Exception());
    }

    /**
     * utility for showing list of nested message, instead of full StackTraceElement
     * does not format as Exception, but as single line text
     */
    public static String stackCausedMessageToString(Throwable stackTrace) {
        StringBuilder sb = new StringBuilder();
        appendStackCausedMessage(sb, stackTrace);
        return sb.toString();
    }

    /**
     * utility for showing list of nested message, instead of full StackTraceElement
     * does not format as Exception, but as single line text
     */
    public static void appendStackCausedMessage(StringBuilder output, Throwable stackTrace) {
        String msg = stackTrace.getLocalizedMessage();
        output.append(msg);
        if (stackTrace.getCause() != null) {
            output.append(" Caused By: ");
            int nestedDepth = 1;
            for (Throwable causedBy = stackTrace.getCause(); causedBy != null; causedBy = causedBy.getCause()) {
                if (nestedDepth > 1) {
                    output.append(" / Cause By[" + nestedDepth + "]: ");
                }
                String causedMsg = causedBy.getLocalizedMessage();
                output.append(causedMsg);
                nestedDepth++;
            }
        }
    }

    /**
     * Compute number of frames in common between exceptions
     * use mainly for causedBy / parent Exception
     */
    public static int stackTraceDepthInCommon(StackTraceElement[] trace, StackTraceElement[] parentTrace) {
        int m = trace.length - 1;
        int n = parentTrace.length - 1;
        while (m >= 0 && n >= 0 && trace[m].equals(parentTrace[n])) {
            m--;
            n--;
        }
        int framesInCommon = trace.length - 1 - m;
        return framesInCommon;
    }

    /**
     * code similar to Exception.printStackTraceAsCause()
     * utility for PrintStream / PrintWriter adapter
     * @param s output for printing result
     * @param causedByException exception to print, but skip common lowest trace elements comparing to parentStackTrace
     * @param parentStackTrace for comparing common stack trace elements
     */
    public static void printStackTraceAsCause(PrintStream s,
                                              Throwable causedByException,
                                              StackTraceElement[] parentTrace) {
        //        synchronized(s) {
        //          PrintWriter writer = new PrintWriter(s);
        //          printStackTraceAsCause(writer, causedByException, parentStackTrace);
        //          writer.flush();
        //        }
        // COPY&PASTE with PrintWriter?!
        synchronized (s) {
            StackTraceElement[] trace = causedByException.getStackTrace();
            int framesInCommon = stackTraceDepthInCommon(trace, parentTrace);

            s.println("Caused By:" + causedByException);
            int max = trace.length - framesInCommon;
            for (int i = 0; i < max; i++) {
                s.println("\tat " + trace[i]);
            }
            if (framesInCommon != 0) {
                s.println("\t... " + framesInCommon + " more");
            }

            // Recurse if we have a cause
            Throwable recursiveCausedBy = causedByException.getCause();
            if (recursiveCausedBy != null) {
                printStackTraceAsCause(s, recursiveCausedBy, trace);
            }
        }

    }

    /**
     * code similar to Exception.printStackTraceAsCause()
     * @param s output for printing result
     * @param causedByException exception to print, but skip common lowest trace elements comparing to parentStackTrace
     * @param parentStackTrace for comparing common stack trace elements
     *
     * COPY&PASTE with PrintStream?!
     */
    public static void printStackTraceAsCause(PrintWriter s,
                                              Throwable causedByException,
                                              StackTraceElement[] parentTrace) {
        synchronized (s) {
            StackTraceElement[] trace = causedByException.getStackTrace();
            int framesInCommon = stackTraceDepthInCommon(trace, parentTrace);

            s.println("Caused By:" + causedByException);
            int max = trace.length - framesInCommon;
            for (int i = 0; i < max; i++) {
                s.println("\tat " + trace[i]);
            }
            if (framesInCommon != 0) {
                s.println("\t... " + framesInCommon + " more");
            }

            // Recurse if we have a cause
            Throwable recursiveCausedBy = causedByException.getCause();
            if (recursiveCausedBy != null) {
                printStackTraceAsCause(s, recursiveCausedBy, trace);
            }
        }
    }

    /**
     * code similar to printStackTraceAsCause, but for using Short string
     * @see stackTraceToShortPath
     */
    public static void printShortTraceAsCause(PrintStream stream,
                                              Throwable causedByException,
                                              StackTraceElement[] parentTrace) {
        synchronized (stream) {
            StackTraceElement[] trace = causedByException.getStackTrace();
            int framesInCommon = stackTraceDepthInCommon(trace, parentTrace);

            stream.print(" Caused By:" + causedByException);
            int max = trace.length - framesInCommon;
            stream.print(" ");
            doStackTraceToShortPath(stream, trace, 0, max);
            if (framesInCommon != 0) {
                stream.print("... " + framesInCommon + " more");
            }

            // Recurse if we have a cause
            Throwable recursiveCausedBy = causedByException.getCause();
            if (recursiveCausedBy != null) {
                printShortTraceAsCause(stream, recursiveCausedBy, trace);
            }
        }
    }

}
