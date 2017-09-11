package com.puresteam;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by steam on 11.09.17.
 */
public class Main {

    private static final int COUNT_THREAD = 8;

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        ExecutorService executor = Executors.newFixedThreadPool(COUNT_THREAD);
        // размер списка будет совпадать с количеством потоков. инфа 100%
        List<Callable<Boolean>> tasks = new ArrayList<>(COUNT_THREAD);
        for (int i = 0; i <= COUNT_THREAD; i++) {
            tasks.add(new Worker());
        }

        try {
            List<Future<Boolean>> results = executor.invokeAll(tasks);
            while (!isAllTaskDone(results)) {
                System.out.println("loading");
                Thread.sleep(TimeUnit.SECONDS.toMillis(5));
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("time=" + (System.currentTimeMillis() - startTime));
    }

    static boolean isAllTaskDone(List<Future<Boolean>> taskResults) {
        return taskResults.stream()
                .allMatch(Future::isDone);
    }
}
