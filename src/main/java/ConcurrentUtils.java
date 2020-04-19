import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ConcurrentUtils {
    public static void stop(ExecutorService executor) {
        try {
            executor.shutdown();
            executor.awaitTermination(30L, TimeUnit.SECONDS);
        } catch (InterruptedException var5) {
            log.error("Termination interrupted when shutting down an executor", var5);
        } finally {
            if (!executor.isTerminated()) {
                log.error("Force killing non-finished executor tasks.");
            }
            executor.shutdownNow();
        }

    }
}
