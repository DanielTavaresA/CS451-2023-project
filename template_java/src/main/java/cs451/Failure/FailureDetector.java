package cs451.Failure;

public interface FailureDetector {
    public void start();

    public void stop();

    public void crash();
}
