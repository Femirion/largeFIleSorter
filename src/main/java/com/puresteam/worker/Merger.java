package com.puresteam.worker;

import com.puresteam.utils.FilesUtils;
import com.puresteam.worker.entity.PairFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

/**
 * Класс для сливания файлов, полученых при работе Splitter-а
 */
public class Merger implements Runnable {

    /**
     * Количество считываемых в память строк
     */
    private final long COUNT;
    /**
     * Текущая позиция в файле
     */
    private long currentPosition = 0;
    private final BlockingQueue<PairFile> queue;

    public Merger(long count, BlockingQueue<PairFile> queue) {
        this.COUNT = count;
        this.queue = queue;
    }

    @Override
    public void run() {
        long currentId = Thread.currentThread().getId();
        List<String> firstLines;
        List<String> secondLines;
        try {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    PairFile pairFile = queue.take();
                    String mergingFile = FilesUtils.generateFileName(currentId);
                    List<String> resultList;

                    // возможна ситуация, когда продюссер обогнал консюмера
                    // тогда в очереди лежат файлы, удаленные на предыдущей итерации
                    // проверим что файлы для склейки действительно существуют
                    if (!Files.exists(pairFile.getFirstFile()) || !Files.exists(pairFile.getSecondFile())) {
                        continue;
                    }

                    do {
                        try (Stream<String> firstLinesStream = Files.lines(pairFile.getFirstFile());
                             Stream<String> secondLinesStream = Files.lines(pairFile.getSecondFile())) {

                            firstLines = firstLinesStream
                                    .skip(currentPosition)
                                    .limit(COUNT)
                                    .collect(Collectors.toList());

                            secondLines = secondLinesStream
                                    .skip(currentPosition)
                                    .limit(COUNT)
                                    .collect(Collectors.toList());

                            currentPosition += COUNT;

                            int firstSize = firstLines.size();
                            int secondSize = secondLines.size();
                            boolean firstBigger = firstSize > secondSize;
                            int max = firstBigger ? firstSize : secondSize;
                            int currentPositionFirst = 0;
                            int currentPositionSecond = 0;

                            resultList = new ArrayList<>(firstSize + secondSize);

                            for (int i = 0; i < max * 2; i++) {
                                if (firstSize == currentPositionFirst) {
                                    resultList.addAll(secondLines.subList(currentPositionSecond, secondSize));
                                    break;
                                }
                                if (secondSize == currentPositionSecond) {
                                    resultList.addAll(firstLines.subList(currentPositionFirst, firstSize));
                                    break;
                                }

                                String str1 = firstLines.get(currentPositionFirst);
                                String str2 = secondLines.get(currentPositionSecond);

                                int result = str1.compareTo(str2);
                                if (result == -1) {
                                    resultList.add(str1);
                                    currentPositionFirst++;
                                } else {
                                    resultList.add(str2);
                                    currentPositionSecond++;
                                }
                            }

                        }

                        if (!resultList.isEmpty()) {
                            Files.write(Paths.get(pairFile.getPath() + mergingFile), resultList, APPEND, CREATE);
                        }

                    }
                    while (!firstLines.isEmpty() && !secondLines.isEmpty());

                    System.out.println(
                            "id=" + currentId
                                    + " finished merge "
                                    + pairFile.getFirstFile()
                                    + "   " + pairFile.getSecondFile()
                                    + "   resultFile=" + mergingFile
                    );

                    Files.delete(pairFile.getFirstFile());
                    Files.delete(pairFile.getSecondFile());
                    // тк перешли к новому файлу, значит нужно обнулить позицию
                    currentPosition = 0;
                } catch (IOException ex) {
                    System.out.println("ERROR IN MERGE=" + ex.getMessage());
                }
            }
        } catch (InterruptedException ex) {
            System.out.println("InterruptedException=" + ex.getMessage());
        }
    }
}
