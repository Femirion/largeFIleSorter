package com.puresteam.worker;

import com.puresteam.worker.entity.PairFile;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.SynchronousQueue;

/**
 * Created by steam on 12.09.17.
 */
public class FileProducer implements Runnable {

    private final String path;
    private final ArrayBlockingQueue<PairFile> queue;

    public FileProducer(String path, ArrayBlockingQueue<PairFile> queue) {
        this.path = path;
        this.queue = queue;
    }

    @Override
    public void run() {
        System.out.println("I'm starting");

        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(path), "*.txt")) {
            int i = 0;
            String firstFile = "";
            for (Path path : directoryStream) {
                if (i % 2 == 0) {
                    firstFile = path.toString();
                } else {
                    PairFile pair = new PairFile(firstFile, path.toString(), this.path + "/tmp/");
                    queue.put(pair);
                }
                i++;
            }
        } catch (Exception ex) {
            System.out.println("TMP-FILE READ ERROR" + ex.getMessage());
        }
    }



}
