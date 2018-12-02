package fr.an.tools.jdbclogger.jdbcimpl;

import java.util.Properties;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import fr.an.tools.jdbclogger.calllogger.CallInfoLoggerHelper;
import fr.an.tools.jdbclogger.calllogger.CallMsgInfo;
import fr.an.tools.jdbclogger.calllogger.CallMsgInfoLogger;
import fr.an.tools.jdbclogger.conf.ConnectionFactoryConfig;

/**
 * Proxy for java.sql.XAConnection + wrapp all calls with pre()/log.post() + set params 
 * 
 */
public class JdbcLoggerXAResource implements XAResource {

	 /** underlying for proxy */
    private final XAResource to;

    private final ConnectionFactoryConfig owner;
    private final int connectionId;
    private final Properties loggerProperties;
    
    private final CallInfoLoggerHelper callInfoLogger;

    private final CallMsgInfo callInfo = new CallMsgInfo() {
    	// override meths?
    };
    
    protected boolean logXAResource = true;
    
    // ------------------------------------------------------------------------

	public JdbcLoggerXAResource(XAResource to, 
			ConnectionFactoryConfig owner, CallMsgInfoLogger logger, int connectionId, Properties loggerProperties) {
		this.to = to;
		this.owner = owner;
		this.connectionId = connectionId;
		this.loggerProperties = loggerProperties;
		this.callInfoLogger = new CallInfoLoggerHelper(callInfo, logger);
        this.logXAResource = Boolean.parseBoolean(loggerProperties.getProperty("logXAResource", "true"));
	}

	// ------------------------------------------------------------------------

	public int getConnectionId() {
		return connectionId;
	}

	public Properties getLoggerProperties() {
		return loggerProperties;
	}

	protected boolean isCurrLogXAConnection() {
        return logXAResource 
        	&& owner.isCurrentActiveLogs()
        	&& owner.isCurrentActiveCommitLogs() // ??
        	;
    }
	
	@Override
	public void start(Xid arg0, int arg1) throws XAException {
		boolean log = isCurrLogXAConnection();
		if (log) {
			callInfoLogger.pre("XAResource start", "" + arg0 + " " + arg1);
		}
		try {
			to.start(arg0, arg1);
			if (log) {
				callInfoLogger.postVoid();
			}
		} catch(XAException ex) {
			if (log) {
				callInfoLogger.postEx(ex);
			}
			throw ex;
		} catch(RuntimeException ex) {
			if (log) {
				callInfoLogger.postEx(ex);
			}
			throw ex;
		}
	}
	

	@Override
	public int prepare(Xid arg0) throws XAException {
		int res;
		boolean log = isCurrLogXAConnection();
		if (log) {
			callInfoLogger.pre("XAResource prepare", "" + arg0);
		}
		try {
			res = to.prepare(arg0);
			if (log) {
				callInfoLogger.postRes(res);
			}
		} catch(XAException ex) {
			if (log) {
				callInfoLogger.postEx(ex);
			}
			throw ex;
		} catch(RuntimeException ex) {
			if (log) {
				callInfoLogger.postEx(ex);
			}
			throw ex;
		}
		return res;
	}

	@Override
	public void commit(Xid arg0, boolean arg1) throws XAException {
		boolean log = isCurrLogXAConnection();
		if (log) {
			callInfoLogger.pre("XAResource commit", "" + arg0 + " " + arg1);
		}
		try {
			to.commit(arg0, arg1);
			if (log) {
				callInfoLogger.postVoid();
			}
		} catch(XAException ex) {
			if (log) {
				callInfoLogger.postEx(ex);
			}
			throw ex;
		} catch(RuntimeException ex) {
			if (log) {
				callInfoLogger.postEx(ex);
			}
			throw ex;
		}
	}

	@Override
	public void rollback(Xid arg0) throws XAException {
		boolean log = isCurrLogXAConnection();
		if (log) {
			callInfoLogger.pre("XAResource rollback", "" + arg0);
		}
		try {
			to.rollback(arg0);
			if (log) {
				callInfoLogger.postVoid();
			}
		} catch(XAException ex) {
			if (log) {
				callInfoLogger.postEx(ex);
			}
			throw ex;
		} catch(RuntimeException ex) {
			if (log) {
				callInfoLogger.postEx(ex);
			}
			throw ex;
		}
	}
	
	@Override
	public void end(Xid arg0, int arg1) throws XAException {
		boolean log = isCurrLogXAConnection();
		if (log) {
			callInfoLogger.pre("XAResource end", "" + arg0 + " " + arg1);
		}
		try {
			to.end(arg0, arg1);
			if (log) {
				callInfoLogger.postVoid();
			}
		} catch(XAException ex) {
			if (log) {
				callInfoLogger.postEx(ex);
			}
			throw ex;
		} catch(RuntimeException ex) {
			if (log) {
				callInfoLogger.postEx(ex);
			}
			throw ex;
		}
	}

	@Override
	public void forget(Xid arg0) throws XAException {
		boolean log = isCurrLogXAConnection();
		if (log) {
			callInfoLogger.pre("XAResource forget", "" + arg0);
		}
		try {
			to.forget(arg0);
			if (log) {
				callInfoLogger.postVoid();
			}
		} catch(XAException ex) {
			if (log) {
				callInfoLogger.postEx(ex);
			}
			throw ex;
		} catch(RuntimeException ex) {
			if (log) {
				callInfoLogger.postEx(ex);
			}
			throw ex;
		}
	}

	@Override
	public Xid[] recover(int arg0) throws XAException {
		Xid[] res;
		boolean log = isCurrLogXAConnection();
		if (log) {
			callInfoLogger.pre("XAResource recover", "" + arg0);
		}
		try {
			res = to.recover(arg0);
			if (log) {
				callInfoLogger.postRes(res);
			}
		} catch(XAException ex) {
			if (log) {
				callInfoLogger.postEx(ex);
			}
			throw ex;
		} catch(RuntimeException ex) {
			if (log) {
				callInfoLogger.postEx(ex);
			}
			throw ex;
		}
		return res;
	}
	
	@Override
	public boolean isSameRM(XAResource arg0) throws XAException {
		return to.isSameRM(arg0);
	}

	@Override
	public int getTransactionTimeout() throws XAException {
		return to.getTransactionTimeout();
	}

	@Override
	public boolean setTransactionTimeout(int arg0) throws XAException {
		boolean res;
		boolean log = isCurrLogXAConnection();
		if (log) {
			callInfoLogger.pre("XAResource setTransactionTimeout", "" + arg0);
		}
		try {
			res = to.setTransactionTimeout(arg0);
			if (log) {
				callInfoLogger.postIgnoreRes(res);
			}
		} catch(XAException ex) {
			if (log) {
				callInfoLogger.postEx(ex);
			}
			throw ex;
		} catch(RuntimeException ex) {
			if (log) {
				callInfoLogger.postEx(ex);
			}
			throw ex;
		}
		return res;
	}
	
}
