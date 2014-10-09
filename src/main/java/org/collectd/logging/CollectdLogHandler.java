package org.collectd.logging;

import org.collectd.api.Collectd;

import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

/**
 * Delegate global java.util.logging to the Collectd.log method.
 */
public class CollectdLogHandler extends Handler {
	int forceLevel = 0;

	public void setForceLevel(int forceLevel) {
		this.forceLevel = forceLevel;
	}

	@Override
	public void publish(LogRecord record) {
		Formatter fmt = getFormatter();
		StringBuilder message = new StringBuilder("FastJMX Plugin: ");
		if (fmt != null) {
			message.append(fmt.formatMessage(record));
		} else {
			message.append(record.getMessage());
		}

		// SEVERE = ERROR
		// WARNING = WARNING
		// INFO = INFO
		// CONFIG = NOTICE
		// FINE || FINER || FINEST = DEBUG

		if (record.getLevel() == Level.SEVERE) {
			Collectd.logWarning(message.toString());
		} else if (record.getLevel() == Level.WARNING) {
			Collectd.logWarning(message.toString());
		} else if (record.getLevel() == Level.INFO) {
			Collectd.logInfo(message.toString());
		} else if (record.getLevel() == Level.CONFIG) {
			Collectd.logNotice(message.toString());
		} else {
			Collectd.logDebug(message.toString());
		}
	}

	@Override
	public void flush() {
	}

	@Override
	public void close() throws SecurityException {
	}
}
