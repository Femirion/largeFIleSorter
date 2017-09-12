package com.puresteam;

import com.puresteam.worker.FileProducer;
import com.puresteam.worker.Merger;
import com.puresteam.worker.entity.PairFile;
import com.puresteam.worker.Splitter;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Основной класс программы
 */
public class Main {

    private static final int COUNT_THREAD = 8;

    public static void main(String[] args) {
        String tmpPath = "/media/steam/E4DE4FB4DE4F7DB4/tmp/largesorttmp";

        // создадим папку для временных файлов. чтобы не засорять мусором исходный каталог
        File tmpDir = new File(tmpPath);
        tmpDir.mkdir();

/*        // эти два семафора накладывают ограничения на кол-во одновремнно читающих/записывающих потоков
        Semaphore read = new Semaphore(COUNT_THREAD == 1 ? 1 : COUNT_THREAD / 2);
        Semaphore write = new Semaphore(COUNT_THREAD == 1 ? 1 : COUNT_THREAD / 2);

        long startTime = System.currentTimeMillis();
        ExecutorService executor = Executors.newFixedThreadPool(COUNT_THREAD);
        // размер списка будет совпадать с количеством потоков. инфа 100%
        List<Callable<Boolean>> tasks = new ArrayList<>(COUNT_THREAD);
        for (int i = 0; i <= COUNT_THREAD; i++) {
            tasks.add(new Splitter(read, write, "", 10000));
        }

        try {
            executor.invokeAll(tasks);
        } catch (InterruptedException ex) {
            System.out.println("EXECUTOR ERORR=" + ex.getMessage());
        }

        long sortTime = System.currentTimeMillis() - startTime;
        System.out.println("sort=" + sortTime);*/

        final ArrayBlockingQueue<PairFile> queue = new ArrayBlockingQueue<>(10);


        Thread producer = new Thread(new FileProducer(tmpPath, queue));
        producer.start();

        Thread merger = new Thread(new Merger(10000, queue));
        merger.start();

//        executor.shutdown();
    }


}
