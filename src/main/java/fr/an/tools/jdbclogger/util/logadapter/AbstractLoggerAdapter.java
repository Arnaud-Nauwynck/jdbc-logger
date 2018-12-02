package fr.an.tools.jdbclogger.util.logadapter;

public abstract class AbstractLoggerAdapter {
	
	public abstract boolean isInfoEnabled();
	public abstract void info(String msg);

	public abstract void warn(String msg);
	public abstract void warn(String msg, Throwable ex);

	public abstract void error(String msg);
	public abstract void error(String msg, Throwable ex);
	
}
