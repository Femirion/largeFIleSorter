package com.puresteam.worker;

import com.puresteam.worker.entity.PairFile;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Пишет в очередь пару файлов, которые нужно склеить вместе
 * их вычитывает Merger, и сливает в один
 */
public class FileProducer implements Runnable {

    private final String path;
    private final BlockingQueue<PairFile> queue;
    private final List<Thread> mergers;

    public FileProducer(String path, BlockingQueue<PairFile> queue, List<Thread> mergers) {
        this.path = path;
        this.queue = queue;
        this.mergers = mergers;
    }

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        System.out.println("I'm starting");
        while (true) {
            try {
                // сначала посчитает количество файлов в директории, если всего 1, значит это результат сортировки
                long count = Files.list(Paths.get(path)).count();
                if (count == 1) {
                    break;
                }
            } catch (IOException ex) {
                System.out.println("CALCULATE FILE ERROR=" + ex.getMessage());
            }


            try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(path), "*.txt")) {
                int i = 0;
                Path firstFile = null;
                for (Path path : directoryStream) {
                    if (i % 2 == 0) {
                        firstFile = path;
                    } else {
                        PairFile pair = new PairFile(firstFile, path, this.path);
                        queue.put(pair);
                    }
                    i++;
                }
            } catch (Exception ex) {
                System.out.println("TMP-FILE READ ERROR=" + ex.getMessage());
            }
        }

        mergers.forEach(Thread::interrupt);
        System.out.println("Finished merge, time=" + (System.currentTimeMillis() - startTime));
    }

}
