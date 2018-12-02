package fr.an.tools.jdbclogger.jdbcimpl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;

import fr.an.tools.jdbclogger.calllogger.CallInfoLoggerHelper;
import fr.an.tools.jdbclogger.calllogger.CallMsgInfo;
import fr.an.tools.jdbclogger.calllogger.CallMsgInfoLogger;
import fr.an.tools.jdbclogger.conf.ConnectionFactoryConfig;

/**
 * Proxy for java.sql.XAConnection + wrapp all calls with pre()/log.post() + set params 
 * 
 */
public class JdbcLoggerXAConnection implements XAConnection {

	/** underlying for proxy */
	protected XAConnection to;
	
	protected final ConnectionFactoryConfig owner;
	protected final CallMsgInfoLogger logger;
	protected final int connectionId;
	protected final Properties loggerProperties;
	
	private final CallInfoLoggerHelper callInfoLogger;

    private final CallMsgInfo callInfo = new CallMsgInfo() {
    	// override meths?
    };
    
    protected XAResource cachedWrapperXAResource;
    protected XAResource cachedTargetXAResource;
    
    protected boolean logXAConnection = true;
    
    protected Connection cachedUnderlyingUnwrappedConnection;
    protected Connection cachedUnderlyingWrappedConnection;
    
    protected boolean logOpenClose;
    
	// ------------------------------------------------------------------------
	
	public JdbcLoggerXAConnection(
			XAConnection to, 
			ConnectionFactoryConfig owner, 
			CallMsgInfoLogger logger, 
			int connectionId, 
			Properties loggerProperties,
			boolean logOpenClose) {
		this.to = to; 
		this.owner = owner;
		this.logger = logger;
		this.connectionId = connectionId;
		this.loggerProperties = loggerProperties;		
		this.callInfoLogger = new CallInfoLoggerHelper(callInfo, logger);
        this.logXAConnection = Boolean.parseBoolean(loggerProperties.getProperty("logXAConnection", "true"));
        this.logOpenClose = logOpenClose;
	}
	
	// ------------------------------------------------------------------------
	
    protected boolean isCurrLogXAConnection() {
        return logXAConnection 
        	&& owner.isCurrentActiveLogs()
        	&& owner.isCurrentActiveCommitLogs() // ??
        	;
    }

    
	/** internal accesser / downcast */
	public XAConnection getUnderlyingXAConnection() {
        return to;
    }

	public final int getConnectionId() {
        return connectionId;
    }
	
	// ------------------------------------------------------------------------

	public Connection getConnection() throws SQLException {
		Connection res; 
		boolean log = logOpenClose && isCurrLogXAConnection();
		if (log) {
			callInfoLogger.pre("XAConnection getConnection", null);
		}
		Connection underlyingConnection;
		try {
			underlyingConnection = to.getConnection();
			if (log) {
				callInfoLogger.postIgnoreVoid();
			}
		} catch(RuntimeException ex) {
			if (log) {
				callInfoLogger.postEx(ex);
			}
			throw ex;
		}
		
	    if (cachedUnderlyingUnwrappedConnection == underlyingConnection) { 
	    	res = cachedUnderlyingWrappedConnection;
	    } else {
	    	Connection res1 = owner.wrapOpenConn("getConnection", underlyingConnection, loggerProperties, logOpenClose);
			res = res1;
	    	cachedUnderlyingUnwrappedConnection = underlyingConnection;
	    	cachedUnderlyingWrappedConnection = res;
	    }
		return res;
	}

	@Override
	public void close() throws SQLException {
		boolean log = logOpenClose && isCurrLogXAConnection();
		if (log) {
			callInfoLogger.pre("XAConnection close", null);
		}
		try {
			to.close();
			if (log) {
				callInfoLogger.postIgnoreVoid();
			}
		} catch(RuntimeException ex) {
			if (log) {
				callInfoLogger.postEx(ex);
			}
			throw ex;
		}
		
		cachedUnderlyingUnwrappedConnection = null;
		cachedUnderlyingWrappedConnection = null;
	}


	public XAResource getXAResource() throws SQLException {
		XAResource res;
		XAResource xaResource = getUnderlyingXAConnection().getXAResource();
		if (xaResource == null) return null; // should not occur
		if (cachedTargetXAResource == xaResource && cachedWrapperXAResource != null) {
			// ** optim: use cache fro wrapper **
			res = cachedWrapperXAResource;
		} else {
			res = cachedWrapperXAResource = new JdbcLoggerXAResource(xaResource, 
					owner, logger, connectionId, loggerProperties);
			cachedTargetXAResource = xaResource;
		}
		return res;
	}
	
	
	public void addConnectionEventListener(ConnectionEventListener listener) {
		boolean log = isCurrLogXAConnection();
		if (log) {
			callInfoLogger.pre("XAConnection addConnectionEventListener", "" + listener);
		}
		try {
			getUnderlyingXAConnection().addConnectionEventListener(listener);
			if (log) {
				callInfoLogger.postIgnoreVoid();
			}
		} catch(RuntimeException ex) {
			if (log) {
				callInfoLogger.postEx(ex);
			}
			throw ex;
		}
	}

	public void removeConnectionEventListener(ConnectionEventListener listener) {
		boolean log = isCurrLogXAConnection();
		if (log) {
			callInfoLogger.pre("XAConnection removeConnectionEventListener", "" + listener);
		}
		try {
			getUnderlyingXAConnection().removeConnectionEventListener(listener);
			if (log) {
				callInfoLogger.postIgnoreVoid();
			}
		} catch(RuntimeException ex) {
			if (log) {
				callInfoLogger.postEx(ex);
			}
			throw ex;
		}
	}

	public void addStatementEventListener(StatementEventListener listener) {
		boolean log = isCurrLogXAConnection();
		if (log) {
			callInfoLogger.pre("XAConnection addStatementEventListener", "" + listener);
		}
		try {
			getUnderlyingXAConnection().addStatementEventListener(listener);
			if (log) {
				callInfoLogger.postIgnoreVoid();
			}
		} catch(RuntimeException ex) {
			if (log) {
				callInfoLogger.postEx(ex);
			}
			throw ex;
		}
	}

	public void removeStatementEventListener(StatementEventListener listener) {
		boolean log = isCurrLogXAConnection();
		if (log) {
			callInfoLogger.pre("removeStatementEventListener", "" + listener);
		}
		try {
			getUnderlyingXAConnection().removeStatementEventListener(listener);
			if (log) {
				callInfoLogger.postIgnoreVoid();
			}
		} catch(RuntimeException ex) {
			if (log) {
				callInfoLogger.postEx(ex);
			}
			throw ex;
		}
	}


	// ------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return "XAConnectionLog[" + super.toString() + "]";
	}
	
}
