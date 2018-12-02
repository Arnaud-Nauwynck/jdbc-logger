package fr.an.tools.jdbclogger.jdbcimpl;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import fr.an.tools.jdbclogger.calllogger.CallInfoLoggerHelper;
import fr.an.tools.jdbclogger.calllogger.CallMsgInfo;
import fr.an.tools.jdbclogger.calllogger.CallMsgInfoLogger;
import fr.an.tools.jdbclogger.conf.ConnectionFactoryConfig;

/**
 * Proxy for java.sql.Connection + wrapp all calls with pre()/log.post() + set params 
 *
 * design pattern:
 *  - Bridge/Proxy to java.sql.Connection
 *  - factory of StatementLog / PreparedStatementLog / CallableStatementLog
 *      (replacement for sql.Statement / sql.PreparedStatement / sql.CallableStatement)
 *
 */
public class JdbcLoggerConnection implements Connection {

    /** underlying for proxy */
    private final Connection to;

	protected final ConnectionFactoryConfig owner;
	protected final CallMsgInfoLogger logger;
	protected final int connectionId;
	protected final Properties loggerProperties;

    private final CallInfoLoggerHelper callInfoLogger;

    private final CallMsgInfo callInfo = new CallMsgInfo() {
        public String fmtMsgParamOutReturn() {
            return arg;
        }
    };
    
    /** counter for executeUpdate since last commit 
     * reset on commit/rollback  
     */
    private int countExecute;
    private int countExecuteUpdate;
    private int countExecuteBatch;
    private int countExecuteMatchingSleepCommit;


    // utility for loggerProperties

    private String sleepInCommitWhenMatching;
    private int sleepMillisBeforeCommit = 0;
    private int sleepMillisAfterCommit = 0;

	private int countOpenStatements;
	private static final AtomicInteger countAllOpenStatements = new AtomicInteger();

	protected boolean logOpenClose;
	
    // constructor
    // ------------------------------------------------------------------------
        
    /** Ctor */
    public JdbcLoggerConnection(ConnectionFactoryConfig owner, CallMsgInfoLogger logger, 
    		Connection to, 
    		int connectionId, 
    		Properties loggerProperties,
    		boolean logOpenClose) {
    	this.owner = owner;
    	this.logger = logger;
    	this.connectionId = connectionId;
    	this.loggerProperties = loggerProperties;
    	this.to = to;
        callInfo.setLogMsgPrefix("[" + connectionId + "] ");
        callInfoLogger = new CallInfoLoggerHelper(callInfo, logger);
        this.logOpenClose = logOpenClose;
        
        this.sleepInCommitWhenMatching = loggerProperties.getProperty("sleep_commit_when_matching", null);
        this.sleepMillisBeforeCommit = Integer.parseInt(loggerProperties.getProperty("sleep_before_commit", "0"));
        this.sleepMillisAfterCommit = Integer.parseInt(loggerProperties.getProperty("sleep_after_commit", "0"));
    }

    // counter support for execute/executeUpdate/executeBatch since last commit/rollback
    // ------------------------------------------------------------------------
    
    public Connection getUnderlyingConnection() {
        return to;
    }
    
    public ConnectionFactoryConfig getOwner() {
        return owner;
    }

    public ConnectionFactoryConfig getConnectionFactoryConfig() {
        return owner;
    }

    public CallMsgInfoLogger getLogger() {
        return logger;
    }
    
    public final int getConnectionId() {
        return connectionId;
    }

    public final Properties getLoggerProperties() {
        return loggerProperties;
    }


    /*package protected*/void incrCountExecute(String match) {
        this.countExecute++;
        incrExecuteMatchingSleepCommit(match);
    }

    /*package protected*/void incrCountExecuteUpdate(String match) {
        this.countExecuteUpdate++;
        incrExecuteMatchingSleepCommit(match);
    }

    /*package protected*/void incrCountExecuteBatch(String match) {
        this.countExecuteBatch++;
        incrExecuteMatchingSleepCommit(match);
    }

    private void incrExecuteMatchingSleepCommit(String match) {
        if (match != null && sleepInCommitWhenMatching != null && match.indexOf(sleepInCommitWhenMatching) != -1) {
            countExecuteMatchingSleepCommit++;
        }
    }

    private boolean hasMatchingSleepCommit() {
        return (countExecuteMatchingSleepCommit > 0);
    }

    private void resetCount() {
        this.countExecute = 0;
        this.countExecuteUpdate = 0;
        this.countExecuteBatch = 0;
        this.countExecuteMatchingSleepCommit = 0;
    }

    protected boolean isLogCommits() {
        return owner.isCurrentActiveLogs()
        	&& owner.isCurrentActiveCommitLogs();
    }


	public void onOpenStatement(StatementLog statementLog) {
		countOpenStatements++;
		countAllOpenStatements.incrementAndGet();
		if (owner.isCurrentActiveStatementCloseLogs()) {
			logger.log("open stmt => " + countOpenStatements + "/" + countAllOpenStatements.get());
		}
	}

	public void onCloseStatement(StatementLog statementLog) {
		countOpenStatements--;
		countAllOpenStatements.decrementAndGet();
		if (owner.isCurrentActiveStatementCloseLogs()) {
			logger.log("close stmt => " + countOpenStatements + "/" + countAllOpenStatements.get());
		}
		
		CallMsgInfo stmtCallInfo = statementLog.getCallInfo();
		
		stmtCallInfo.clearResultSetNextStats();
	}

    public int getCountExecute() {
        return countExecute;
    }

    public int getCountExecuteUpdate() {
        return countExecuteUpdate;
    }

    public int getCountExecuteBatch() {
        return countExecuteBatch;
    }

    public int getCountExecuteMatchingSleepCommit() {
        return countExecuteMatchingSleepCommit;
    }

    public int getCountOpenStatements() {
        return countOpenStatements;
    }
	
    // override Object
    // ------------------------------------------------------------------------

    /** override Object */
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || !(obj instanceof JdbcLoggerConnection))
            return false;
        JdbcLoggerConnection o = (JdbcLoggerConnection) obj;
        return to.equals(o.to);
    }

    // implements java.sql.Connection
    // create sub Statement/PreparedStatment... => create real statement + wrap in logger   
    // ------------------------------------------------------------------------

    public Statement createStatement() throws SQLException {
        Statement p = to.createStatement();
        return new StatementLog(this, p);
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        PreparedStatement p = to.prepareStatement(sql);
        return new JdbcLoggerPreparedStatement(this, p, sql);
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        CallableStatement p = to.prepareCall(sql);
        return new JdbcLoggerCallableStatement(this, p, sql);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        Statement p = to.createStatement(resultSetType, resultSetConcurrency);
        return new StatementLog(this, p, resultSetType, resultSetConcurrency);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        PreparedStatement p = to.prepareStatement(sql, resultSetType, resultSetConcurrency);
        return new JdbcLoggerPreparedStatement(this, p, sql, resultSetType, resultSetConcurrency);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        CallableStatement p = to.prepareCall(sql, resultSetType, resultSetConcurrency);
        return new JdbcLoggerCallableStatement(this, p, sql, resultSetType, resultSetConcurrency);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        Statement p = to.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
        return new StatementLog(this, p, resultSetType, resultSetConcurrency); // TODO, resultSetHoldability;
    }

    public CallableStatement prepareCall(String sql,
                                         int resultSetType,
                                         int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        CallableStatement stmt = to.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        return new JdbcLoggerCallableStatement(this, stmt, sql, resultSetType, resultSetConcurrency);
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        PreparedStatement stmt = to.prepareStatement(sql, autoGeneratedKeys);
        return new JdbcLoggerPreparedStatement(this, stmt, sql);
    }

    public PreparedStatement prepareStatement(String sql,
                                              int resultSetType,
                                              int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        PreparedStatement stmt = to.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        return new JdbcLoggerPreparedStatement(this, stmt, sql, resultSetType, resultSetConcurrency); // TODO resultSetHoldability;
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        PreparedStatement stmt = to.prepareStatement(sql, columnIndexes);
        return new JdbcLoggerPreparedStatement(this, stmt, sql);
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        PreparedStatement stmt = to.prepareStatement(sql, columnNames);
        return new JdbcLoggerPreparedStatement(this, stmt, sql);
    }

    // implements java.sql.Connection : delegate to underlying connection + log pre/post 
    // ------------------------------------------------------------------------

    @Override
    public String nativeSQL(String sql) throws SQLException {
        callInfoLogger.pre("nativeSQL", sql);
        try {
            String res = to.nativeSQL(sql);
            callInfoLogger.postRes(res);
            return res;
        } catch (SQLException ex) {
            callInfoLogger.postEx(ex);
            throw ex;
        }
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        boolean isLog = isLogCommits();
        if (isLog) {
            callInfoLogger.pre("setAutoCommit", "setAutoCommit " + autoCommit);
        }
        try {
            to.setAutoCommit(autoCommit);
            if (isLog) {
                callInfoLogger.postVoid();
            }
        } catch (SQLException ex) {
            callInfoLogger.postEx(ex);
            throw ex;
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return to.getAutoCommit();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        boolean isLog = isLogCommits();
        if (isLog) {
            callInfoLogger.pre("setTransactionIsolation", "setTransactionIsolation " + level);
        }
        try {
            to.setTransactionIsolation(level);
            if (isLog) {
                callInfoLogger.postVoid();
            }
        } catch (SQLException ex) {
            callInfoLogger.postEx(ex);
            throw ex;
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return to.getTransactionIsolation();
    }

    @Override
    public void commit() throws SQLException {
        boolean isLog = isLogCommits();
        boolean doSleep = hasMatchingSleepCommit();
        if (sleepMillisBeforeCommit > 0 && doSleep) {
            try {
                Thread.sleep(sleepMillisBeforeCommit);
            } catch (Exception ex) {
            }
        }
        if (isLog) {
            String methodMsg = "commit";
            if (sleepMillisBeforeCommit > 0 && doSleep) {
                methodMsg = "sleep-before" + sleepMillisBeforeCommit + " + " + methodMsg;
            }
            if (sleepMillisAfterCommit > 0 && doSleep) {
                methodMsg = methodMsg + " + sleep-after " + sleepMillisAfterCommit;
            }
            callInfoLogger.pre(methodMsg, "commit");
        }

        try {
            to.commit();

            resetCount();
            if (isLog) {
                callInfoLogger.postVoid();
            }
        } catch (SQLException ex) {
            callInfoLogger.postEx(ex);
            throw ex;
        }

        if (sleepMillisAfterCommit > 0 && doSleep) {
            try {
                Thread.sleep(sleepMillisAfterCommit);
            } catch (Exception ex) {
            }
        }
    }

    @Override
    public void rollback() throws SQLException {
        boolean isLog = isLogCommits();
        if (isLog) {
            callInfoLogger.pre("rollback", "rollback");
        }
        try {
            to.rollback();

            resetCount();
            if (isLog) {
                callInfoLogger.postVoid();
            }
        } catch (SQLException ex) {
            callInfoLogger.postEx(ex);
            throw ex;
        }
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        boolean isLog = isLogCommits();
        if (isLog) {
            callInfoLogger.pre("releaseSavepoint", "" + savepoint);
        }
        try {
            to.releaseSavepoint(savepoint);
            if (isLog) {
                callInfoLogger.postVoid();
            }
        } catch (SQLException ex) {
            callInfoLogger.postEx(ex);
            throw ex;
        }
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        boolean isLog = isLogCommits();
        if (isLog) {
            callInfoLogger.pre("rollback", "" + savepoint);
        }
        try {
            to.rollback(savepoint);
            resetCount();
            if (isLog) {
                callInfoLogger.postVoid();
            }
        } catch (SQLException ex) {
            callInfoLogger.postEx(ex);
            throw ex;
        }
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        callInfoLogger.pre("abort", "" + executor);
        try {
            to.abort(executor);
            callInfoLogger.postVoid();
        } catch (SQLException ex) {
            callInfoLogger.postEx(ex);
            throw ex;
        }
    }
    
    @Override
    public Savepoint setSavepoint() throws SQLException {
        boolean isLog = isLogCommits();
        if (isLog) {
            callInfoLogger.pre("setSavepoint", "");
        }
        try {
            Savepoint res = to.setSavepoint();
            if (isLog) {
                callInfoLogger.postRes(res);
            }
            return res;
        } catch (SQLException ex) {
            callInfoLogger.postEx(ex);
            throw ex;
        }
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        boolean isLog = isLogCommits();
        if (isLog) {
            callInfoLogger.pre("setSavepoint", name);
        }
        try {
            Savepoint res = to.setSavepoint(name);
            if (isLog) {
                callInfoLogger.postRes(res);
            }
            return res;
        } catch (SQLException ex) {
            callInfoLogger.postEx(ex);
            throw ex;
        }
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
    	boolean isLog = isLogCommits();
        if (isLog) {
        	callInfoLogger.pre("setReadOnly", "" + readOnly);
        }
        try {
            to.setReadOnly(readOnly);
            if (isLog) {
            	callInfoLogger.postVoid();
            }
        } catch (SQLException ex) {
            if (isLog) {
            	callInfoLogger.postEx(ex);
            }
            throw ex;
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return to.isReadOnly();
    }

    public void close() throws SQLException {
        owner.onCloseConn(this, logOpenClose);
        to.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        boolean res = to.isClosed();
        return res;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return to.getMetaData();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        to.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        return to.getCatalog();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return to.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        to.clearWarnings();
    }

    @Override
    public Map<String,Class<?>> getTypeMap() throws SQLException {
        return to.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        to.setTypeMap(map);
    }

    @Override
    public int getHoldability() throws SQLException {
        return to.getHoldability();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        boolean isLog = isLogCommits();
        if (isLog) {
            callInfoLogger.pre("setHoldability", "" + holdability);
        }
        try {
            to.setHoldability(holdability);
            if (isLog) {
                callInfoLogger.postVoid();
            }
        } catch (SQLException ex) {
            callInfoLogger.postEx(ex);
            throw ex;
        }
    }


    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return to.createArrayOf(typeName, elements);
    }

    @Override
    public Blob createBlob() throws SQLException {
        return to.createBlob();
    }

    @Override
    public Clob createClob() throws SQLException {
        return to.createClob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return to.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return to.createSQLXML();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return to.createStruct(typeName, attributes);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return to.getClientInfo();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return to.getClientInfo(name);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        to.setClientInfo(properties);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        to.setClientInfo(name, value);        
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return to.isValid(timeout);
    }
    
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        if (iface.isAssignableFrom(to.getClass())) {
        	return true;
        }
    	return to.isWrapperFor(iface);
    }

    @Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		if (iface.isAssignableFrom(to.getClass())) {
    		@SuppressWarnings("unchecked")
    		T res = (T) to;
    		return res;
    	}
        return to.unwrap(iface);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        callInfoLogger.pre("setSchema", "setSchema " + schema);
        try {
            to.setSchema(schema);
            callInfoLogger.postVoid();
        } catch (SQLException ex) {
            callInfoLogger.postEx(ex);
            throw ex;
        }
    }

    @Override
    public String getSchema() throws SQLException {
        return to.getSchema();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        callInfoLogger.pre("setNetworkTimeout", " " + milliseconds);
        try {
            to.setNetworkTimeout(executor, milliseconds);
            callInfoLogger.postIgnoreVoid();
        } catch (SQLException ex) {
            callInfoLogger.postEx(ex);
            throw ex;
        }
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return to.getNetworkTimeout();
    }
    
}
