package com.puresteam;

import com.puresteam.worker.Merger;
import com.puresteam.worker.Splitter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Основной класс программы
 */
public class Main {

    private static int COUNT_THREAD;

    public static void main(String[] args) {
        Path largeFile = null;
        Path sortedFile = null;
        if (args.length == 3) {
            largeFile = Paths.get(args[0]);
            sortedFile = Paths.get(args[1]);
            COUNT_THREAD = Integer.valueOf(args[2]);
        } else {
            System.out.println("not enough parameters");
            System.exit(0);
        }

        Path tmpPath = Paths.get(largeFile.getParent().toString(), "largeFileTmpDirectory" );
        long count = 50000;

        // замерим время старта
        long startTime = System.currentTimeMillis();

        // создадим папку для временных файлов. чтобы не засорять мусором исходный каталог
        File tmpDir = tmpPath.toFile();
        tmpDir.mkdir();

       // эти два семафора накладывают ограничения на кол-во одновремнно читающих/записывающих потоков
        Semaphore read = new Semaphore(COUNT_THREAD == 1 ? 1 : COUNT_THREAD / 2);
        Semaphore write = new Semaphore(COUNT_THREAD == 1 ? 1 : COUNT_THREAD / 2);

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

        executor.shutdown();
        System.out.println("split ended, start merge");

        CyclicBarrier barrier = new CyclicBarrier(COUNT_THREAD);
        List<Thread> mergers = new ArrayList<>();
        for (int i = 1; i <= COUNT_THREAD; i++) {
            Thread merger = new Thread(new Merger(barrier, read, write, count / 2, tmpPath));
            mergers.add(merger);
        }

        mergers.forEach(Thread::start);

        // дождемся выполнения всех потоков
        mergers.forEach(m -> {
            try {
                m.join();
            } catch (InterruptedException e) {
                System.out.println("InterruptedException ex=" + e);
            }
        });

        try {
            // теперь во временной папке возьмем файл (он должен быть единственный)
            // переименуем его и положим из временной папки в текущую
            // временную папку дропнем
            Path resultFile = Files.list(tmpPath)
                    .limit(1)
                    .findAny()
                    .orElseThrow(FileNotFoundException::new);

            resultFile.toFile().renameTo(sortedFile.toFile());
            tmpDir.delete();
        } catch (FileNotFoundException ex) {
            System.out.println("Sorted file not founded=" + ex);
        } catch (IOException ex) {
            System.out.println("IOException=" + ex);
        }
        System.out.println("sort time=" + (System.currentTimeMillis() - startTime));
    }


}
