package cs451.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class Log {

    public static Path logPath;

    /**
     * Appends the given log message to the log file.
     * 
     * @param log the log message to be appended to the log file
     */
    public static void logFile(String log) {
        try {
            Files.write(logPath, log.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
