package fr.an.tools.jdbclogger.calllogger;

import fr.an.tools.jdbclogger.calllogger.templaterecorder.TemplateSqlEntry;

/**
 * Info for a method call (time pre/post + status) + params + return result
 *   String messages pre/post, args, etc..
 *   CallInfo(time,duration,Exception)
 *
 */
public class CallMsgInfo {

    protected CallMsgInfo parent;

    /** when the call was started */
    protected long startTimeNanos;

    /** when the call was finished*/
    protected long endTimeNanos;

    protected String callerName;

    /** */
    protected String msg;

    /** flag for skipping this call in logs */
    public boolean ignoreMsg;


    protected String logMsgPrefix = "";

    /** optional category for method (unused yet in logging) */
    protected String methCategory;


    /** name of method called */
    protected String meth;


    private Object methodFormatKey;

    
    /** argument of method call... 
     * TODO: rewrite as Object[] + use java.text formatter 
     */
    protected Object[] args;

    
    /** argument of method call... 
     * TODO: rewrite as Object[] + use java.text formatter 
     */
    protected String arg;


    /** has result return by call */
    boolean hasReturnType;

    /** */
    boolean printResReturn;


    /** */
    protected Object resReturn;

    /** exception if failed */
    protected Throwable exception;


    /**
     *  set by meth setPre from arg + input param infos..
     *  example: sql.Statement =>
     *    CallableStatement stmt = con.createCallableStatement("...);
     *    stmt.setInt (3, ..); // <= fill input param
     *    stmt.registerOutParam(2, ...)
     *    res = stmt.execute();  // call + get return code
     *    stmt.getInt(2, ..) // <= get out param
     *
     *  fields:
     *    qry = "{? = exec MySqlQuery ?, ?}"
     *    argWithValuePre = "{out = exec MySqlQuery 3245, out}"    TOCHECK??? return param of query..
     *    argWithValuePost = "out=>1234 exec MySqlQuery 3245, out=>2134
     *    resReturnMsg="true" / empty for default..
     */
    protected String argWithValuePre;
    protected String argWithValuePost; // <= set by meth setPost from arg + output param infos..
    protected String resReturnMsg; // <= set by meth setPost from result

    private int resultSetNextCount = 0;
    private long resultSetNextTotalMillis = 0;

    protected transient TemplateSqlEntry templateEntry;
    
    // ------------------------------------------------------------------------
    
	public CallMsgInfo() {
	}

    // ------------------------------------------------------------------------
    
    public CallMsgInfo getParent() {
        return parent;
    }

    /*package protected?*/
    public void setParent(CallMsgInfo p) {
        this.parent = p;
    }


    public TemplateSqlEntry getTemplateEntry() {
		return templateEntry;
	}

	public void setTemplateEntry(TemplateSqlEntry templateEntry) {
		this.templateEntry = templateEntry;
	}

	public long getStartTimeNanos() {
        return startTimeNanos;
    }

    public long getTimePreNanos() {
        return startTimeNanos;
    }


    public long getEndTimeNanos() {
        return endTimeNanos;
    }

    public long getNanos() {
        return (long) (endTimeNanos - startTimeNanos);
    }

    public int getMillis() {
        return (int) (getNanos() / 1000000);
    }

    public String getCallerName() {
        return callerName;
    }

    public void setCallerName(String p) {
        this.callerName = p;
    }

    public String getMsg() {
        return msg;
    }

    public void setIgnoreMsg(boolean p) {
        this.ignoreMsg = p;
    }

    public boolean isIgnoreMsg() {
        return ignoreMsg;
    }

    public String getLogMsgPrefix() {
        return logMsgPrefix;
    }

    public void setLogMsgPrefix(String p) {
        this.logMsgPrefix = p;
    }

    public String getMethCategory() {
        return methCategory;
    }

    protected void setMethCategory(String p) {
        this.methCategory = p;
    }

    public String getMeth() {
        return meth;
    }

    public Object getMethodFormatKey() {
        return methodFormatKey;
    }

    public void setMethodFormatKey(Object p) {
        this.methodFormatKey = p;
    }

    public Object[] getArgs() {
        return args;
    }

    public void setArgs(Object[] p) {
        this.args = p;
    }

    public String getArg() {
        return arg;
    }

    boolean isHasReturnType() {
        return hasReturnType;
    }

    public boolean isPrintResReturn() {
        return printResReturn;
    }

    public Object getResReturn() {
        return resReturn;
    }

    public Throwable getException() {
        return exception;
    }


    /** fill */
    public void setPre(String meth, Object[] args) {
        this.meth = meth;
        setArgs(args);
        this.arg = "";
        this.startTimeNanos = System.nanoTime();
        this.msg = null;
        this.exception = null;
        this.resReturn = null;
        this.ignoreMsg = false;

        try {
            this.argWithValuePre = fmtMsgParamPre();
            if (argWithValuePre == null) {
                argWithValuePre = "";
            }
        } catch (Exception ex) {
        	// should not occur, ignore!
        }
    }

    /** fill */
    public void setPre(String methCategory, String meth, String arg) {
        setPre(meth, new Object[] { arg });
        setMethCategory(methCategory);
        this.arg = arg;
    }

    /** fill */
    private void setPost(Object val) {
        this.endTimeNanos = System.nanoTime();
        this.resReturn = val;

        try {
            this.resReturnMsg = fmtMsgResReturn();

            this.argWithValuePost = fmtMsgParamOutReturn();
            if (argWithValuePost == null) {
                argWithValuePost = "";
            }
        } catch (Exception ex) {
        }
    }

    public void setPostVoid() {
        this.hasReturnType = false;
        this.printResReturn = false;
        setPost(null);
    }

    public void setPostRes(Object res) {
        this.hasReturnType = true;
        this.printResReturn = true;
        setPost(res);
    }

    public void setPostDefaultRes(Object res) { // res may be null (not printed!)
        this.hasReturnType = true;
        this.printResReturn = false;
        setPost(res);
    }

    public void setPostDefaultRes() {
        setPostDefaultRes(null);
    }

    public void setPostEx(Throwable ex) {
        this.endTimeNanos = System.nanoTime();

        this.exception = ex;
        this.resReturn = null;

        this.hasReturnType = false;
        this.printResReturn = false;

        this.argWithValuePost = argWithValuePre; // no param out on Error!!
        // => fmtEx ??
        if (argWithValuePost == null)
            argWithValuePost = "";
    }

    // formatter 
    // ------------------------------------------------------------------------

    public static String argsToString(Object[] args) {
        StringBuilder sb = new StringBuilder();
        final int size = args.length;
        for (int i = 0; i < size; i++) {
            sb.append(args[i]);
            if (i + 1 < size)
                sb.append(", ");
        }
        return sb.toString();
    }

    /**
     * fmt message 
     */
    public String fmtMsgParamTemplate() {
        String res = "";
        if (arg != null) {
            res += argsToString(args);
        }
        return res;
    }

    /**
     * fmt message with param before call
     */
    public String fmtMsgParamPre() {
        String res = "";
        if (args != null) {
            res += argsToString(args);
        }
        return res;
    }
    
    /**
     * fmt message with bindVar
     */
    public String fmtMsgBindVar() {
        String res = meth;
        return res;
    }
    

    /**
     * fmt message with param after call (may have out Param)
     */
    public String fmtMsgParamOutReturn() {
        String res = "";
        if (args != null) {
            res = argsToString(args);
        }
        return res;
    }

    /**
     * fmt message for result returned
     *  empty string or null allowed
     */
    public String fmtMsgResReturn() {
        String res = null;
        if (hasReturnType && printResReturn) {
            res = ((resReturn != null) ? resReturn.toString() : "null");
        } else
            res = "";
        return res;
    }

    public String getMsgPre() {
        StringBuilder sb = new StringBuilder();
        sb.append(logMsgPrefix);

        sb.append("begin.. ");

        sb.append(meth);
        sb.append(" ");
        if (argWithValuePre != null) {
            sb.append(argWithValuePre);
        }
        return sb.toString();
    }

    public String getMsgPost() {
        StringBuilder sb = new StringBuilder();
        sb.append(logMsgPrefix);

        sb.append(".. done ");
        sb.append(getMillis());
        sb.append("ms ");

        sb.append(meth);
        sb.append(" ");
        if (argWithValuePost != null) {
            sb.append(argWithValuePost);
        }
        if (resReturnMsg != null && resReturnMsg.length() != 0) {
            sb.append(", returned:");
            sb.append(resReturnMsg);
        }
        return sb.toString();
    }

    public String getArgWithValue() {
        return argWithValuePost;
    }


	public void incrResultSetNext(long time) {
		resultSetNextCount++;
	    resultSetNextTotalMillis += time;		
	}
	
    public int getResultSetNextCount() {
		return resultSetNextCount;
	}

	public long getResultSetNextTotalMillis() {
		return resultSetNextTotalMillis;
	}

	public void clearResultSetNextStats() {
		resultSetNextCount = 0;
		resultSetNextTotalMillis = 0;
	}
}
