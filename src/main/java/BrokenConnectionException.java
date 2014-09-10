import java.io.IOException;

/**
 * A BrokenConnectionException allows us to re-open failed connections when we need to.
 */
public class BrokenConnectionException extends IOException {
	private Connection brokenConnection;

	public BrokenConnectionException(Connection connection, String message, Throwable cause) {
		super(message, cause);
		this.brokenConnection = connection;
	}

	public BrokenConnectionException(Connection connection, String message) {
		super(message);
		this.brokenConnection = connection;
	}

	public Connection getBrokenConnection() {
		return brokenConnection;
	}
}
