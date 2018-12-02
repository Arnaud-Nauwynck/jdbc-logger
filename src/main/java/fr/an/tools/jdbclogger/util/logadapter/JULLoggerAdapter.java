package fr.an.tools.jdbclogger.util.logadapter;

import java.util.logging.Level;

/**
 * Adapter for JUL (java.util.logging)
 */
public class JULLoggerAdapter extends AbstractLoggerAdapter {

	/**
	 * Factory for JULLoggerAdapter
	 */
	public static class JULLoggerAdapterFactory extends LoggerAdapterFactory {
		
		public JULLoggerAdapterFactory() {
		}
		
		@Override
		public AbstractLoggerAdapter getLogger(String name) {
			return new JULLoggerAdapter(name);
		}
	}
	
	private java.util.logging.Logger log;
	
	// ------------------------------------------------------------------------
	
	protected JULLoggerAdapter(String name) {
		log = java.util.logging.Logger.getLogger(name);
	}
	
	// ------------------------------------------------------------------------
	
	@Override
	public boolean isInfoEnabled() {
		return log.isLoggable(Level.INFO);
	}

	@Override
	public void info(String msg) {
		log.info(msg);
	}

	@Override
	public void warn(String msg) {
		log.log(Level.WARNING, msg);
	}
	
	@Override
	public void warn(String msg, Throwable ex) {
		log.log(Level.WARNING, msg, ex);
	}

	@Override
	public void error(String msg) {
		log.log(Level.SEVERE, msg);
	}
	
	@Override
	public void error(String msg, Throwable ex) {
		log.log(Level.SEVERE, msg, ex);
	}

}
