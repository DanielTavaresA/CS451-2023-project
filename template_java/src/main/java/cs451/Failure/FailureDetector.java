package cs451.Failure;

/**
 * FailureDetector interface provides a way to detect process crashes.
 * It provides a way to start and stop the failure detector.
 * It also provides a way to crash a process.
 */
public interface FailureDetector {
    public void start();

    public void stop();

    public void crash();
}
