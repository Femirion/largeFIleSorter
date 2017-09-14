package com.puresteam;

import com.puresteam.worker.FileProducer;
import com.puresteam.worker.Merger;
import com.puresteam.worker.entity.PairFile;
import com.puresteam.worker.Splitter;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Основной класс программы
 */
public class Main {

    private static final int COUNT_THREAD = 8;

    public static void main(String[] args) {
        Path largeFile = Paths.get("/media/steam/E4DE4FB4DE4F7DB4/tmp/large_file.txt");
        String tmpPath = "/media/steam/E4DE4FB4DE4F7DB4/tmp/largesorttmp/";
        long count = 500000;

        // создадим папку для временных файлов. чтобы не засорять мусором исходный каталог
        File tmpDir = new File(tmpPath);
        tmpDir.mkdir();

       // эти два семафора накладывают ограничения на кол-во одновремнно читающих/записывающих потоков
        Semaphore read = new Semaphore(COUNT_THREAD == 1 ? 1 : COUNT_THREAD / 2);
        Semaphore write = new Semaphore(COUNT_THREAD == 1 ? 1 : COUNT_THREAD / 2);
/*
        long startTime = System.currentTimeMillis();
        ExecutorService executor = Executors.newFixedThreadPool(COUNT_THREAD);
        // размер списка будет совпадать с количеством потоков. инфа 100%
        List<Callable<Boolean>> tasks = new ArrayList<>(COUNT_THREAD);
        for (int i = 0; i <= COUNT_THREAD; i++) {
            tasks.add(new Splitter(read, write, largeFile, tmpPath, count));
        }

        try {
            executor.invokeAll(tasks);
        } catch (InterruptedException ex) {
            System.out.println("EXECUTOR ERORR=" + ex.getMessage());
        }

        long sortTime = System.currentTimeMillis() - startTime;
        System.out.println("sort=" + sortTime);*/


        final SynchronousQueue<PairFile> queue = new SynchronousQueue<>();


        List<Thread> mergers = new ArrayList<>();
        for (int i = 1; i <= 1; i++) {
            Thread merger = new Thread(new Merger(count / 2, queue));
            mergers.add(merger);
        }

        Thread producer = new Thread(new FileProducer(tmpPath, queue, mergers));
        producer.start();
        mergers.forEach(Thread::start);


//        executor.shutdown();
    }


}
