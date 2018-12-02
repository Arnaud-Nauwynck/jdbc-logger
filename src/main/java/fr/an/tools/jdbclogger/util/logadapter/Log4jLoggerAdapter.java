package fr.an.tools.jdbclogger.util.logadapter;

/**
 * Adtapter for Log4j
 */
public class Log4jLoggerAdapter extends AbstractLoggerAdapter {

	/**
	 * Factory for Log4jLoggerAdapter
	 */
	public static class Log4jLoggerAdapterFactory extends LoggerAdapterFactory {
		
		public Log4jLoggerAdapterFactory() {
		}
		
		@Override
		public AbstractLoggerAdapter getLogger(String name) {
			return new Log4jLoggerAdapter(name);
		}
	}
	
	private org.apache.log4j.Logger log;
	
	// ------------------------------------------------------------------------
	
	protected Log4jLoggerAdapter(String name) {
		log = org.apache.log4j.Logger.getLogger(name);
	}
	
	// ------------------------------------------------------------------------
	
	@Override
	public boolean isInfoEnabled() {
		return log.isInfoEnabled();
	}

	@Override
	public void info(String msg) {
		log.info(msg);
	}
	
	@Override
	public void warn(String msg) {
		log.warn(msg);
	}
	
	@Override
	public void warn(String msg, Throwable ex) {
		log.warn(msg, ex);
	}
	
	@Override
	public void error(String msg) {
		log.error(msg);
	}
	
	@Override
	public void error(String msg, Throwable ex) {
		log.error(msg, ex);
	}

}
