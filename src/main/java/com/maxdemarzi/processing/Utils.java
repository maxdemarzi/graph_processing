package com.maxdemarzi.processing;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.ReadOperations;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author mh
 * @since 28.03.15
 */
public class Utils {
    static final int BATCH_SIZE  = 100_000;

    public static int toInt(double value) {
        return (int) (100_000*value);
    }
    public static double toFloat(int value) {
        return value / 100_000.0;
    }

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

    public static void runOperations(ExecutorService pool, final PrimitiveLongIterator it, int totalCount, ReadOperations ops, OpsRunner runner) {
        List<Future> futures = new ArrayList<>((int)(totalCount / BATCH_SIZE));
        while (it.hasNext()) {
            futures.add(pool.submit(new BatchRunnable(ops, it, BATCH_SIZE,runner)));
        }
        Utils.waitForTasks(futures);
    }
}
