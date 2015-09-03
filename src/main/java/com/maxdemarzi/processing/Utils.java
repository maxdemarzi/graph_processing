package com.maxdemarzi.processing;

import java.util.List;
import java.util.concurrent.*;

/**
 * @author mh
 * @since 28.03.15
 */
public class Utils {
    static ExecutorService createPool(int threads, int queueSize) {
        return new ThreadPoolExecutor(1, threads, 30, TimeUnit.SECONDS, new LinkedBlockingDeque<>(queueSize),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public static int waitForTasks(List<Future> futures) {
        int total = 0;
        for (Future future : futures) {
            try {
                future.get();
                total ++;
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        futures.clear();
        return total;
    }
}
