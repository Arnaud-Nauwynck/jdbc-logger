package fr.an.tools.jdbclogger.helper.pool.dbcp;

import fr.an.tools.jdbclogger.conf.ConnectionFactoryConfig;
import fr.an.tools.jdbclogger.jdbcimpl.JdbcLoggerConnection;
import fr.an.tools.jdbclogger.util.logadapter.AbstractLoggerAdapter;

import org.apache.commons.pool.PoolableObjectFactory;

import java.sql.Connection;
import java.util.Properties;

public class JdbcLoggerPoolableObjectFactory implements PoolableObjectFactory/*<Connection>*/ {

	private PoolableObjectFactory delegate;

	private ConnectionFactoryConfig connectionFactoryConfig;
	
	protected AbstractLoggerAdapter logger;
	
	// ------------------------------------------------------------------------

	public JdbcLoggerPoolableObjectFactory(PoolableObjectFactory delegate, ConnectionFactoryConfig connectionFactoryConfig) {
		this.delegate = delegate;
		this.connectionFactoryConfig = connectionFactoryConfig;
		
		this.logger = connectionFactoryConfig.getLogger("db.dbcp.pool");
	}

	// ------------------------------------------------------------------------

	
	public Object/*<Connection>*/ makeObject() throws Exception {
		logger.info("dbcp.makeObject() => make Connection + wrap");
		Connection tmpres = (Connection) delegate.makeObject();
		Properties loggerProps = connectionFactoryConfig.getProperties(); // TOCHECK?
		boolean logOpenClose = false; // TOCHECK?
		JdbcLoggerConnection res = connectionFactoryConfig.wrapOpenConn("dbcp-wrap", tmpres, loggerProps, logOpenClose);
		return res;
	}

	public void destroyObject(Object/*<Connection>*/ obj) throws Exception {
		JdbcLoggerConnection conn = (JdbcLoggerConnection) obj;
		Connection delegateConnection = conn.getUnderlyingConnection();
		logger.info("dbcp.destroyObject() => unwrap + destroy Connection");
		delegate.destroyObject(delegateConnection);
	}

	public boolean validateObject(Object/*<Connection>*/ obj) {
		JdbcLoggerConnection conn = (JdbcLoggerConnection) obj;
		Connection delegateConnection = conn.getUnderlyingConnection();
		logger.info("dbcp.validateObject() => validateObject underlying Connection");
		return delegate.validateObject(delegateConnection);
	}

	public void activateObject(Object/*<Connection>*/ obj) throws Exception {
		JdbcLoggerConnection conn = (JdbcLoggerConnection) obj;
		Connection delegateConnection = conn.getUnderlyingConnection();
		logger.info("dbcp.activateObject() => activateObject underlying Connection");
		delegate.activateObject(delegateConnection);
	}

	public void passivateObject(Object/*<Connection>*/ obj) throws Exception {
		JdbcLoggerConnection conn = (JdbcLoggerConnection) obj;
		Connection delegateConnection = conn.getUnderlyingConnection();
		logger.info("dbcp.passivateObject() => passivateObject underlying Connection");
		delegate.passivateObject(delegateConnection);
	}
	
}
