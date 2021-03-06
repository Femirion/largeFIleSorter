package com.puresteam.worker;

import com.puresteam.utils.FilesUtils;

import java.io.*;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
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

    /** внутренний объект синхронизицаи*/
    private static volatile Object syncObj = new Object();
    /** Обрабатываемые сейчас файлы */
    private static Set<Path> processedFile = new ConcurrentSkipListSet<>();
    /** Множество активных сейчас mergers */
    private static Set<Long> mergers = new HashSet<>();
    /**  Количество считываемых в память строк */
    private final long COUNT;
    /** Текущая позиция в файле */
    private long currentPosition = 0;
    /** Семафор на чтение */
    private Semaphore read;
    /** Семафор на запись */
    private Semaphore write;
    /** Путь к временной директории */
    private final Path tmpPath;
    /** Барьер по заверешнию сливания 2х файлов */
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
        // сохраним текущий id-потока в множество всех рабочих Merger-ов
        mergers.add(currentId);
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
                                // добавим сливаемые файлы в множество обрабатываемых, чтобы другие файлы
                                // не трогали их
                                processedFile.add(secondFile);
                                processedFile.add(firstFile);
                                mergingFile = Paths.get(tmpPath.toString(), FilesUtils.generateFileName(currentId));
                                // добавим файл в множество обрабатываемых, чтобы
                                // другие потоки не взяли его, до того как запись в него завершена
                                processedFile.add(mergingFile);
                                break;
                            }
                            countFile++;
                        }
                    }
                }

                // если хотя бы один из файлов Null, значит все файлы разобрали другие Merger-ы
                if (firstFile == null || secondFile == null) {
                    barrier.await();
                    continue;
                }

                try(FileWriter fw = new FileWriter(mergingFile.toString(), true);
                    BufferedWriter bw = new BufferedWriter(fw);
                    PrintWriter out = new PrintWriter(bw);
                    Scanner scanFirst = new Scanner(firstFile.toFile());
                    Scanner scanSecond= new Scanner(secondFile.toFile())
                ) {

                    boolean notEmptyFirstFile = true;
                    boolean notEmptySecondFile = true;
                    String stringFromFirstFile = null;
                    String stringFromSecondFile = null;
                    for (;;) {
                        // если на предыдущем щаге узнали что файл пуст, значит
                        // на следующем шаге он тоже будет пуст!
                        notEmptyFirstFile = notEmptyFirstFile && scanFirst.hasNextLine();
                        notEmptySecondFile = notEmptySecondFile && scanSecond.hasNextLine();

                        // если оба файла пустые, то нужно выйти
                        if (!notEmptyFirstFile && !notEmptySecondFile) {
                            break;
                        }

                        // есди первый файл не пуст, но пуст второй, то будем читать и писать из 1го файла
                        if (notEmptyFirstFile && !notEmptySecondFile) {
                            out.println(scanFirst.nextLine());
                            continue;
                        }

                        // есди второй файл не пуст, но пуст первый, то будем читать и писать из 2го файла
                        if (notEmptySecondFile && !notEmptyFirstFile) {
                            out.println(scanSecond.nextLine());
                            continue;
                        }

                        if (stringFromFirstFile == null) {
                            stringFromFirstFile = scanFirst.nextLine();
                        }

                        if (stringFromSecondFile == null) {
                            stringFromSecondFile = scanSecond.nextLine();
                        }

                        if (stringFromFirstFile.compareTo(stringFromSecondFile) <= 0) {
                            out.println(stringFromFirstFile);
                            stringFromFirstFile = null;
                        } else {
                            out.println(stringFromSecondFile);
                            stringFromSecondFile = null;
                        }
                    }
                } catch (IOException e) {
                    System.out.println("Error with write=" + e.getMessage());
                }

/*
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
                while (!firstLines.isEmpty() && !secondLines.isEmpty());*/
                Files.delete(firstFile);
                Files.delete(secondFile);
                processedFile.remove(mergingFile);
                // тк перешли к новому файлу, значит нужно обнулить позицию
                currentPosition = 0;
                barrier.await();
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
