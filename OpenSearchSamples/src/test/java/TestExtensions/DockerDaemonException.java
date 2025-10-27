package TestExtensions;

/**
 * Custom unchecked exception thrown when Docker daemon connectivity issues are detected.
 * <p>
 * This exception is used to indicate that Docker is not running, not responding,
 * or otherwise inaccessible when attempting to run Testcontainers-based tests.
 * </p>
 * <p>
 * This is a RuntimeException because Docker availability is an environmental
 * precondition that cannot be programmatically recovered from - the user must
 * start Docker Desktop manually.
 * </p>
 */
public class DockerDaemonException extends RuntimeException {
    
    /**
     * Constructs a new DockerDaemonException with the specified detail message.
     *
     * @param message the detail message explaining the Docker connectivity issue
     */
    public DockerDaemonException(String message) {
        super(message);
    }
    
    /**
     * Constructs a new DockerDaemonException with the specified detail message and cause.
     *
     * @param message the detail message explaining the Docker connectivity issue
     * @param cause the underlying cause of the exception
     */
    public DockerDaemonException(String message, Throwable cause) {
        super(message, cause);
    }
}

