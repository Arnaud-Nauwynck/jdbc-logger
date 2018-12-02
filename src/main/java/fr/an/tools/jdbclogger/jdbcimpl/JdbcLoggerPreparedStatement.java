package fr.an.tools.jdbclogger.jdbcimpl;

import fr.an.tools.jdbclogger.calllogger.CallMsgInfo;
import fr.an.tools.jdbclogger.calllogger.ParamInfo;
import fr.an.tools.jdbclogger.calllogger.templaterecorder.TemplateSqlUtil;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;

/**
 * 
 */
public class JdbcLoggerPreparedStatement extends StatementLog implements PreparedStatement {

    /**
     * inner class for overriding CallMsgInfo, specific for PreparedStatement
     */
    protected class PreparedStatementCallMsgInfo extends CallMsgInfo implements StatementCallMsgInfo {

        /** implements StatementCallMsgInfo */
        @Override
        public Statement getOwnerStatement() {
            return JdbcLoggerPreparedStatement.this;
        }

        /**
         * override to return sql bind var
         */
        public String fmtMsgBindVar() {
            return sqlQuery;
        }
        
        /**
         * override to return template sql query
         */
        public String fmtMsgParamTemplate() {
            String res = TemplateSqlUtil.templatizeSqlText(sqlQuery);
            return res;
        }

        /** @extends CallMsgInfo */
        public String fmtMsgParamPre() {
            return fmtMsgParam(0);
        }

        /** @extends CallMsgInfo */
        public String fmtMsgParamOutReturn() {
            return fmtMsgParam(1);
        }

        /** called from super.getArg() when super.isDirtyArg .. */
        public String fmtMsgParam(int prePost) {
            String res = sqlQuery;

            String prefix = "";
            if (res.startsWith("/*")) {
            	// do not replace "?" in prefixed comment ... cf hibernate option: hibernate.use_sql_comments
            	int posEndComment = res.indexOf("*/ ");
            	if (posEndComment != -1) {
            		posEndComment = posEndComment + 3;
            		prefix = res.substring(0, posEndComment);
            		res = res.substring(posEndComment, res.length());
            	}
            }
            
            // loop params and replace "?"
            final int size = params.size();
            for (int i = 0; i < size; i++) { // no 0 even for return code
                ParamInfo elt = (ParamInfo) params.get(i);
                if (elt != null) {
                    String paramStr = elt.toStringPrePost(prePost);
                    int idxMarker = res.indexOf("?");
                    if (idxMarker != -1) {
                        res = ParamInfo.replaceStringChar(res, idxMarker, paramStr);
                    } else {
                        System.err.println("Internal sql marker '?' not found in query for replacing param [" + i + "] "
                                  + "by text:" + paramStr);
                    }
                } else {
                	System.err.println("param " + i + " no set or registerOut");
                }
            }

            return prefix + res;
        }
        
    }

    // ------------------------------------------------------------------------
    
    /** redondant with ((PreparedStatement)super.to) */
    private PreparedStatement to;

    /** log info about indexed parameters **/
    private String sqlQuery;

    /**
     * ArrayList<ParamInfo> 
     * param by index 
     * TODO need also to handle param by name for jdbc2 
     */
    private ArrayList<ParamInfo> params = new ArrayList<ParamInfo>();


    // constructor
    // ------------------------------------------------------------------------

    public JdbcLoggerPreparedStatement(JdbcLoggerConnection owner, PreparedStatement to, String sql) {
        super(owner, to);
        this.to = to;
        this.sqlQuery = sql;
    }

    public JdbcLoggerPreparedStatement(JdbcLoggerConnection owner,
                                PreparedStatement to,
                                String sql,
                                int resultSetType,
                                int resultSetConcurrency) {
        super(owner, to, resultSetType, resultSetConcurrency);
        this.to = to;
        this.sqlQuery = sql;
    }

    // ------------------------------------------------------------------------

    /** override factory .. */
    protected CallMsgInfo createCallMsgInfo() {
    	return new PreparedStatementCallMsgInfo();
    }

    public final String getSqlQuery() {
        return sqlQuery;
    }
    
    public ParamInfo getParamInfo(int index) {
        return params.get(index);
    }

    public void setPre(String methCategory, String meth, String arg) {
        callInfo.setPre(methCategory, meth, arg);
    }


    // internal
    // ------------------------------------------------------------------------

    /** internal */
    private void setParamChanged() {
    }

    /** internal */
    protected void set(int indexParameter, ParamInfo paramInfo) {
        try {
            int index = indexParameter - 1;
            if (index >= 0) {
                setParamChanged();
                if (index >= params.size()) {
                    // params.ensureSize..
                    params.ensureCapacity(index + 1);
                    int nb = index - params.size() + 1;
                    for (int i = 0; i < nb; i++)
                        params.add(null);
                }
                params.set(index, paramInfo);
            } else {
                // ignore error here in log
            }
        } catch (Exception ex) {
        	System.err.println("set save paramInfo");
        	ex.printStackTrace(System.err);
        }
    }

    private void set(int index, Object value) {
        set(index, new ParamInfo(value));
    }

    /** internal */
    protected void resetParams() {
        params.clear();
        setParamChanged();
    }

    // implements java.sql.PreparedStatement
    // ------------------------------------------------------------------------

    public final ResultSet executeQuery() throws SQLException {
    	JdbcLoggerResultSet res;
        pre("executeQuery", "");
        try {
            ResultSet tmpres = to.executeQuery();
            if (tmpres != null) {
                res = wrapResultSet(tmpres);
                postDefaultRes(res);
            } else {
            	res = null;
                postRes("NULL ResultSet");
            }
        } catch (SQLException ex) {
            postEx(ex);
            throw ex;
        }
        return res;
    }

    public final int executeUpdate() throws SQLException {
        int res;
        pre("executeUpdate", "");
        try {
            res = to.executeUpdate();
            ownerConnectionLog.incrCountExecuteUpdate(sqlQuery); //unformatted prepared statment, without params
            postRes(new Integer(res));
        } catch (SQLException ex) {
            postEx(ex);
            throw ex;
        }
        return res;
    }

    public final boolean execute() throws SQLException {
        boolean res;
        pre("execute", "");
        try {
            res = to.execute();
            ownerConnectionLog.incrCountExecute(sqlQuery); //unformatted prepared statment, without params
            if (res)
                postDefaultRes((res) ? Boolean.TRUE : Boolean.FALSE);
            else
                postRes("false");
        } catch (SQLException ex) {
            postEx(ex);
            throw ex;
        }
        return res;
    }

    public final void addBatch() throws SQLException {
        pre("addBatch", "");
        try {
            to.addBatch();
            postVoid();
        } catch (SQLException ex) {
            postEx(ex);
            throw ex;
        }
    }

    public final ResultSetMetaData getMetaData() throws SQLException {
        ResultSetMetaData res;
        pre("getMetaData", "");
        try {
            res = to.getMetaData();
            postDefaultRes(res);
        } catch (SQLException ex) {
            postEx(ex);
            throw ex;
        }
        return res;
    }

    public final ParameterMetaData getParameterMetaData() throws SQLException {
        return to.getParameterMetaData();
    }

    // implements java.sql.PreparedStatement, save param value + delegate to underlying  
    // -------------------------------------------------------------------------

    public final void clearParameters() throws SQLException {
        params.clear();
        setParamChanged();
        to.clearParameters();
    }

    public final void setNull(int parameterIndex, int sqlType) throws SQLException {
        ParamInfo p = new ParamInfo(false, sqlType);
        set(parameterIndex, p);
        to.setNull(parameterIndex, sqlType);
    }

    public final void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        ParamInfo p = new ParamInfo(false, sqlType);
        p.setTypeName(typeName);
        to.setNull(parameterIndex, sqlType, typeName);
    }

    public final void setBoolean(int parameterIndex, boolean x) throws SQLException {
        set(parameterIndex, (x)? "'1'" : "'0'");
        to.setBoolean(parameterIndex, x);
    }

    public final void setByte(int parameterIndex, byte x) throws SQLException {
        set(parameterIndex, new Byte(x));
        to.setByte(parameterIndex, x);
    }

    public final void setShort(int parameterIndex, short x) throws SQLException {
        set(parameterIndex, new Short(x));
        to.setShort(parameterIndex, x);
    }

    public final void setInt(int parameterIndex, int x) throws SQLException {
        set(parameterIndex, new Integer(x));
        to.setInt(parameterIndex, x);
    }

    public final void setLong(int parameterIndex, long x) throws SQLException {
        set(parameterIndex, new Long(x));
        to.setLong(parameterIndex, x);
    }

    public final void setFloat(int parameterIndex, float x) throws SQLException {
        set(parameterIndex, new Float(x));
        to.setFloat(parameterIndex, x);
    }

    public final void setDouble(int parameterIndex, double x) throws SQLException {
        set(parameterIndex, new Double(x));
        to.setDouble(parameterIndex, x);
    }

    public final void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        set(parameterIndex, x);
        to.setBigDecimal(parameterIndex, x);
    }

    public final void setString(int parameterIndex, String x) throws SQLException {
        set(parameterIndex, "'" + x + "'");
        to.setString(parameterIndex, x);
    }

    public final void setBytes(int parameterIndex, byte[] x) throws SQLException {
        set(parameterIndex, x);
        to.setBytes(parameterIndex, x);
    }

    
    public final void setDate(int parameterIndex, Date x) throws SQLException {
        set(parameterIndex, ParamInfo.formatOracleToDate(x));
        to.setDate(parameterIndex, x);
    }

    public final void setTime(int parameterIndex, Time x) throws SQLException {
        set(parameterIndex, ParamInfo.formatOracleToTime(x));
        to.setTime(parameterIndex, x);
    }

    public final void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        set(parameterIndex, ParamInfo.formatOracleToTimestamp(x));
        to.setTimestamp(parameterIndex, x);
    }

    public final void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        set(parameterIndex, ParamInfo.formatOracleToDate(x, cal));
        //        set(parameterIndex, new ValueDateWithCal(x,cal));
        to.setDate(parameterIndex, x, cal);
    }

    public final void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        set(parameterIndex, ParamInfo.formatOracleToTime(x, cal));
        //        set(parameterIndex, new ValueDateWithCal(x,cal));
        to.setTime(parameterIndex, x, cal);
    }

    public final void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        set(parameterIndex, ParamInfo.formatOracleToTimestamp(x, cal));
        //        set(parameterIndex, new ValueDateWithCal(x,cal));
        to.setTimestamp(parameterIndex, x, cal);
    }

    public final void setArray(int parameterIndex, Array x) throws SQLException {
        set(parameterIndex, objToString(x.getArray()));
        to.setArray(parameterIndex, x);
    }

    public final void setRef(int parameterIndex, Ref x) throws SQLException {
        set(parameterIndex, x);
        to.setRef(parameterIndex, x);
    }

    public final void setObject(int parameterIndex, Object x, int targetSqlType, int scale) throws SQLException {
        ParamInfo p = new ParamInfo(x);
        p.setTargetSqlType(targetSqlType);
        p.setScale(scale);
        set(parameterIndex, p);
        to.setObject(parameterIndex, x, targetSqlType, scale);
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        ParamInfo p = new ParamInfo(x);
        p.setTargetSqlType(targetSqlType);
        set(parameterIndex, p);
        to.setObject(parameterIndex, x, targetSqlType);
    }

    public void setObject(int parameterIndex, Object x) throws SQLException {
        String text;
        if (x == null) {
            text = "'null'";
        } else {
            Class cl = x.getClass();
            if (cl.isPrimitive()) {
                text = x.toString();
            } else {
                text = objToString(x);
            }
        }
        set(parameterIndex, text);

        to.setObject(parameterIndex, x);
    }
    
    private static String objToString(Object x) {
        String textValue;
        if (x == null) {
            textValue = "null";
        } else if (x instanceof String) {
            textValue = "'" + x + "'";
        } else if (x instanceof Character) {
            textValue = "'" + x + "'";
        } else if (x instanceof Boolean) {
            boolean b = ((Boolean) x).booleanValue();
            textValue = (b)? "'1'" : "'0'";
        } else if (x instanceof Long) {
            textValue = ((Long) x).toString();
        } else if (x instanceof Integer) {
            textValue = ((Integer) x).toString();
        } else if (x instanceof BigDecimal) {
            textValue = ((BigDecimal) x).toString();
        } else if (x instanceof java.sql.Timestamp) {
            textValue = ParamInfo.formatOracleToTimestamp((java.sql.Timestamp) x);
        } else if (x instanceof java.sql.Time) {
            textValue = ParamInfo.formatOracleToTime((java.sql.Time) x);;
        } else if (x instanceof java.sql.Date) {
            textValue = ParamInfo.formatOracleToDate((java.sql.Date) x);
        } else if (x instanceof java.util.Date) {
            textValue = ParamInfo.formatOracleToDate((java.util.Date) x);
        } else if (x instanceof long[]) {
            long[] arr = (long[]) x;
            StringBuilder sb = new StringBuilder(arr.length * 7);
            sb.append("[ ");
            int len = arr.length;
            int displayLen = (len > 10)? 10 : len; 
            for (int i = 0; i < displayLen; i++) {
                sb.append(Long.toString(arr[i]));
                if (i + 1 < len)
                    sb.append(',');
            }
            if (displayLen != len) {
                sb.append(" /* ... truncated " + displayLen + "/" + len +  "... */ ");
                sb.append("\n");
            }
            sb.append(" ]");
            textValue = sb.toString();
        } else if (x instanceof BigDecimal[]) {
            BigDecimal[] arr = (BigDecimal[]) x;
            StringBuilder sb = new StringBuilder(arr.length * 7);
            sb.append("[ ");
            int len = arr.length;
            int displayLen = (len > 10)? 10 : len; 
            for (int i = 0; i < displayLen; i++) {
                sb.append(arr[i]);
                if (i + 1 < len)
                    sb.append(',');
            }
            if (displayLen != len) {
                sb.append(" /* ... truncated " + displayLen + "/" + len +  "... */ ");
                sb.append("\n");
            }
            sb.append(" ]");
            textValue = sb.toString();
        } else {
            textValue = x.toString();
        }
        return textValue;
    }

    public final void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        set(parameterIndex, new ParamInfo("'--AsciiStream.. (not  supported  in log) length:" + length + "--'"));
        to.setAsciiStream(parameterIndex, x, length);
    }

    public final void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        set(parameterIndex, new ParamInfo("'--UnicodeStream.. (not  supported  in log) length:" + length + "--'"));
        to.setUnicodeStream(parameterIndex, x, length); // deprecated
    }

    public final void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        set(parameterIndex, new ParamInfo("'--BinaryStream.. (not  supported  in log) length:" + length + "--'"));
        to.setBinaryStream(parameterIndex, x, length);
    }

    public final void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        set(parameterIndex, new ParamInfo("'--CharacterStream.. (not  supported  in log) length:" + length + "--'"));
        to.setCharacterStream(parameterIndex, reader, length);
    }

    public final void setBlob(int parameterIndex, Blob x) throws SQLException {
        set(parameterIndex, new ParamInfo("'--Blob.. (not  supported  in log)--'"));
        to.setBlob(parameterIndex, x);
    }

    public final void setClob(int parameterIndex, Clob x) throws SQLException {
        set(parameterIndex, new ParamInfo("'--Clob.. (not  supported  in log)--'"));
        to.setClob(parameterIndex, x);
    }

    public final void setURL(int parameterIndex, URL x) throws SQLException {
        set(parameterIndex, x);
        to.setURL(parameterIndex, x);
    }

    // ------------------------------------------------------------------------
    
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        to.setAsciiStream(parameterIndex, x);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        to.setAsciiStream(parameterIndex, x, length);
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        to.setBinaryStream(parameterIndex, x);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        to.setBinaryStream(parameterIndex, x, length);
    }

    public void setBlob(int parameterIndex, InputStream x) throws SQLException {
        to.setBlob(parameterIndex, x);
    }

    public void setBlob(int parameterIndex, InputStream x, long length) throws SQLException {
        to.setBlob(parameterIndex, x, length);
    }

    public void setCharacterStream(int parameterIndex, Reader x) throws SQLException {
        to.setCharacterStream(parameterIndex, x);
    }

    public void setCharacterStream(int parameterIndex, Reader x, long length) throws SQLException {
        to.setCharacterStream(parameterIndex, x, length);
    }

    public void setClob(int parameterIndex, Reader x) throws SQLException {
        to.setClob(parameterIndex, x);
    }

    public void setClob(int parameterIndex, Reader x, long length) throws SQLException {
        to.setClob(parameterIndex, x, length);
    }

    public void setNCharacterStream(int parameterIndex, Reader x) throws SQLException {
        to.setNCharacterStream(parameterIndex, x);
    }

    public void setNCharacterStream(int parameterIndex, Reader x, long length) throws SQLException {
        to.setNCharacterStream(parameterIndex, x, length);
    }

    public void setNClob(int parameterIndex, NClob x) throws SQLException {
        to.setNClob(parameterIndex, x);
    }

    public void setNClob(int parameterIndex, Reader x) throws SQLException {
        to.setNClob(parameterIndex, x);
    }

    public void setNClob(int parameterIndex, Reader x, long length) throws SQLException {
        to.setNClob(parameterIndex, x, length);
    }

    public void setNString(int parameterIndex, String x) throws SQLException {
        to.setNString(parameterIndex, x);
    }

    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        to.setRowId(parameterIndex, x);
    }

    public void setSQLXML(int parameterIndex, SQLXML x) throws SQLException {
        to.setSQLXML(parameterIndex, x);
    }
    
    // ------------------------------------------------------------------------

    @Override
	public String toString() {
		return "JdbcLoggerPStmt[" + sqlQuery + "]";
	}
    
    
} //PreparedStatementLog
