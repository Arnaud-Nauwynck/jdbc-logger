package fr.an.tools.jdbclogger.util.logadapter;

/**
 * Adtapter for Log4j
 */
public class Slf4jLoggerAdapter extends AbstractLoggerAdapter {

	/**
	 * Factory for Slf4jLoggerAdapter
	 */
	public static class Slf4jLoggerAdapterFactory extends LoggerAdapterFactory {
		
		public Slf4jLoggerAdapterFactory() {
		}
		
		@Override
		public AbstractLoggerAdapter getLogger(String name) {
			return new Slf4jLoggerAdapter(name);
		}
	}
	
	private org.slf4j.Logger log;
	
	// ------------------------------------------------------------------------
	
	protected Slf4jLoggerAdapter(String name) {
		log = org.slf4j.LoggerFactory.getLogger(name);
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
