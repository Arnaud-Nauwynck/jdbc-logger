package fr.an.tools.jdbclogger.util.logadapter;

public abstract class LoggerAdapterFactory {

	public abstract AbstractLoggerAdapter getLogger(String name);
	
}
