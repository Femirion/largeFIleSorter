package com.puresteam.worker;

import com.puresteam.utils.FilesUtils;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

/**
 * Класс для сливания файлов, полученых при работе Splitter-а
 */
public class Merger implements Runnable {

    private static final Object syncObj = new Object();
    private static Set<Path> workedFile = new HashSet<>();
    /**
     * Количество считываемых в память строк
     */
    private final long COUNT;
    /**
     * Текущая позиция в файле
     */
    private long currentPosition = 0;
    private Semaphore read;
    private Semaphore write;
    private final Path tmpPath;

    /**
     * @param read    семафор для чтения
     * @param write   семафор для записи
     * @param count   размер читаемой порции
     * @param tmpPath   путь до директории с временными файлами
     */
    public Merger(Semaphore read, Semaphore write, long count, Path tmpPath) {
        this.COUNT = count;
        this.read = read;
        this.write = write;
        this.tmpPath = tmpPath;
    }



    @Override
    public void run() {
        long currentId = Thread.currentThread().getId();
        List<String> firstLines;
        List<String> secondLines;
        while (true) {
            try {
                try {
                    // сначала подсчитает количество файлов в директории, если всего 1, значит это результат сортировки
                    long count = FilesUtils.fileCount(tmpPath);
                    if (count == 1) {
                        break;
                    }
                } catch (IOException ex) {
                    System.out.println("CALCULATE FILE COUNT ERROR=" + ex.getMessage());
                }


                Path firstFile = null;
                Path secondFile = null;
                // синхронизируем поиск файлов для слияния, чтобы 2 разных потока не взяли ли бы один и тот же файл
                synchronized (syncObj) {
                    try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(tmpPath, "*.txt")) {
                        int countFile = 0;
                        for (Path path : directoryStream) {
                            // если такой файл уже взяли, то переходим к следующему
                            if (workedFile.contains(path)) {
                                continue;
                            }
                            if (countFile % 2 == 0) {
                                firstFile = path;
                            } else {
                                secondFile = path;
                                break;
                            }
                            countFile++;
                        }
                    }
                }

                String mergingFile = FilesUtils.generateFileName(currentId);
                List<String> resultList;


                do {
                    try (Stream<String> firstLinesStream = Files.lines(firstFile);
                         Stream<String> secondLinesStream = Files.lines(secondFile)) {

                        read.acquire();
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

                    } finally {
                        read.release();
                    }

                    if (!resultList.isEmpty()) {
                        try {
                            write.acquire();
                            Files.write(Paths.get(tmpPath.toString() + mergingFile), resultList, APPEND, CREATE);
                        } finally {
                            write.release();
                        }

                    }

                }
                while (!firstLines.isEmpty() && !secondLines.isEmpty());

                System.out.println(
                        "id=" + currentId
                                + " finished merge "
                                + firstFile.toString()
                                + "   " + secondFile.toString()
                                + "   resultFile=" + mergingFile
                );

                Files.delete(firstFile);
                Files.delete(secondFile);
                // тк перешли к новому файлу, значит нужно обнулить позицию
                currentPosition = 0;
                System.out.println("consumer test1");
                System.out.println("consumer test2");
            } catch (IOException ex) {
                System.out.println("ERROR IN MERGE=" + ex.getMessage());
            } catch (InterruptedException ex) {
                System.out.println("InterruptedException=" + ex.getMessage());
            }
        }
    }

}
