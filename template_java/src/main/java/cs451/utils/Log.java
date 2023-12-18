package cs451.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Log {

    public static Path logPath;
    public static Map<Integer, String> logs = new ConcurrentHashMap<>();

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

    public static void addLog(Integer index, String log) {
        logs.put(index, log);
    }

    public static void flush() {
        List<Integer> keys = new ArrayList<Integer>(logs.keySet());
        keys.sort((a, b) -> a.compareTo(b));
        for (Integer key : keys) {
            logFile(logs.get(key));
        }
    }
}
