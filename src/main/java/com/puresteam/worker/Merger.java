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
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

/**
 * Класс для сливания файлов, полученых при работе Splitter-а
 */
public class Merger implements Runnable {

    private static volatile Object syncObj = new Object();
    private static Set<Path> processedFile = new ConcurrentSkipListSet<>();
    private static Set<Long> mergers = new HashSet<>();
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
    private final CyclicBarrier barrier;

    /**
     * @param read    семафор для чтения
     * @param write   семафор для записи
     * @param count   размер читаемой порции
     * @param tmpPath путь до директории с временными файлами
     */
    public Merger(CyclicBarrier barrier, Semaphore read, Semaphore write, long count, Path tmpPath) {
        this.barrier = barrier;
        this.read = read;
        this.write = write;
        this.COUNT = count;
        this.tmpPath = tmpPath;
    }


    @Override
    public void run() {
        long currentId = Thread.currentThread().getId();
        mergers.add(currentId);
        List<String> firstLines;
        List<String> secondLines;
        while (true) {
            try {
                Path firstFile = null;
                Path secondFile = null;
                Path mergingFile = null;

                // синхронизируем поиск файлов для слияния, чтобы 2 разных потока не взяли ли бы один и тот же файл
                synchronized (syncObj) {
                    try {
                        // сначала подсчитает количество файлов в директории, если всего 1, значит это результат сортировки
                        long count = FilesUtils.fileCount(tmpPath);
                        System.out.println("currId=" + currentId + "  count=" + count);
                        if (count == 1) {
                            break;
                        }
                    } catch (IOException ex) {
                        System.out.println("CALCULATE FILE COUNT ERROR=" + ex.getMessage());
                    }

                    try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(tmpPath, "*.txt")) {
                        int countFile = 0;
                        for (Path path : directoryStream) {
                            // если такой файл уже взяли, то переходим к следующему
                            if (processedFile.contains(path)) {
                                continue;
                            }
                            if (countFile % 2 == 0) {
                                firstFile = path;
                            } else {
                                secondFile = path;
                                processedFile.add(secondFile);
                                processedFile.add(firstFile);
                                mergingFile = Paths.get(tmpPath.toString(), FilesUtils.generateFileName(currentId));
                                // добавим файл в множество обрабатываемых, чтобы
                                // другие потоки не взяли его, до того как запись в него завершена
                                processedFile.add(mergingFile);
//                                System.out.println("currId=" + currentId + "  firstFile=" + firstFile +"  secondFile=" + secondFile);
                                break;
                            }
                            countFile++;
                        }
                    }
                }

                if (firstFile == null && secondFile == null) {
//                    System.out.println("currId=" + currentId + "  await2  both file null");
                    barrier.await();
                    continue;
                }

                if (firstFile == null || secondFile == null) {
                    String result = firstFile == null ? "first" : "second";
//                    System.out.println("currId=" + currentId +" await3  " + result + " file null");
                    barrier.await();
                    continue;
                }



                List<String> resultList;

                do {
                    try (Stream<String> firstLinesStream = Files.lines(firstFile);
                         Stream<String> secondLinesStream = Files.lines(secondFile)) {

                        try {
                            read.acquire();
                            firstLines = firstLinesStream
                                    .skip(currentPosition)
                                    .limit(COUNT)
                                    .collect(Collectors.toList());

                            secondLines = secondLinesStream
                                    .skip(currentPosition)
                                    .limit(COUNT)
                                    .collect(Collectors.toList());
                        } finally {
                            read.release();
                        }

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
                        try {
                            write.acquire();
                            Files.write(mergingFile, resultList, APPEND, CREATE);
                        } finally {
                            write.release();
                        }

                    }

                }
                while (!firstLines.isEmpty() && !secondLines.isEmpty());

//                System.out.println(
//                        "id=" + currentId
//                                + " finished merge "
//                                + "   resultFile=" + mergingFile
//                );

                System.out.println("currId" + currentId + "  size=" + Files.size(mergingFile));

                Files.delete(firstFile);
                Files.delete(secondFile);
                processedFile.remove(mergingFile);
                // тк перешли к новому файлу, значит нужно обнулить позицию
                currentPosition = 0;

//                System.out.println("currId" + currentId + "  delete " + firstFile);
//                System.out.println("currId" + currentId + "  delete " + secondFile);
//                System.out.println("I'm watting await4 currId=" + currentId);
//

                barrier.await();
//                System.out.println("I'm work again currId=" + currentId);
            } catch (IOException ex) {
                System.out.println("currId" + currentId +"  ERROR IN MERGE=" + ex.getCause());
                ex.printStackTrace();
                break;
            } catch (InterruptedException ex) {
                System.out.println("InterruptedException=" + ex.getMessage());
            } catch (Exception ex) {
                System.out.println("Exception=" + ex.getMessage());
            }
        }
    }

}
