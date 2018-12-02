package fr.an.tools.jdbclogger.helper.pool.dbcp;

import fr.an.tools.jdbclogger.conf.ConnectionFactoryConfig;
import fr.an.tools.jdbclogger.jdbcimpl.JdbcLoggerConnection;
import fr.an.tools.jdbclogger.util.logadapter.AbstractLoggerAdapter;

import org.apache.commons.dbcp.ConnectionFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class JdbcLoggerDbcpConnectionFactory implements ConnectionFactory {

	private ConnectionFactory delegate;

	private ConnectionFactoryConfig connectionFactoryConfig;
	
	protected AbstractLoggerAdapter logger;
	
	// ------------------------------------------------------------------------

	public JdbcLoggerDbcpConnectionFactory(ConnectionFactory delegate, ConnectionFactoryConfig connectionFactoryConfig) {
		this.delegate = delegate;
		this.connectionFactoryConfig = connectionFactoryConfig;
		
		this.logger = connectionFactoryConfig.getLogger("db.dbcp.connFactory");
	}

	// ------------------------------------------------------------------------

	
	public Connection createConnection() throws SQLException {
		logger.info("dbcp.createConnection() => create + wrap");
		Connection tmpres = delegate.createConnection();
		
		Properties loggerProps = connectionFactoryConfig.getProperties(); // TOCHECK?
		boolean logOpenClose = false; // TOCHECK?
		JdbcLoggerConnection res = connectionFactoryConfig.wrapOpenConn("dbcp-wrap", tmpres, loggerProps, logOpenClose);
		
		return res;
	}
	
}
