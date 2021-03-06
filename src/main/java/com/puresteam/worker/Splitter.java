package com.puresteam.worker;


import com.puresteam.utils.FilesUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Рабочий, читающий порцию данных из большого файла и сортирующий ее
 * и записывающий в файл меньшего объема. По завершению работы
 * в директории tmp будет куча мелких файлов с отсорированными строками.
 * Их нужно слить в один большой, этим занимается Merger
 */
public class Splitter implements Callable<Boolean> {

    /** Дикетория с временными файлами */
    private final Path TMP_FILE_PATH;
    /** Путь к файлу с данными */
    private final Path LARGE_FILE_PATH;
    /** Количество считываемых в память строк */
    private final long COUNT;
    /** Текущая позиция в файле */
    private static AtomicLong currentPosition = new AtomicLong(0);
    private Semaphore read;
    private Semaphore write;

    /**
     * Конструктор с параметрами
     *
     * @param read семафор для чтения
     * @param write семафор для записи
     * @param largeFile путь к сортируемуму файлу
     * @param tmpPath путь к папке с временными данными
     * @param countLines размер считываемой порции
     */
    public Splitter(Semaphore read, Semaphore write, Path largeFile, Path tmpPath, long countLines) {
        TMP_FILE_PATH = tmpPath;
        LARGE_FILE_PATH = largeFile;
        COUNT = countLines;
        // эти два семафора накладывают ограничения на кол-во одновремнно читающих/записывающих потоков
        this.read = read;
        this.write = write;
    }

    @Override
    public Boolean call() {
        long currentId = Thread.currentThread().getId();
        List<String> lines;
        try  {
            while (true) {
                // семафор для чтения ограничивает количество одновременно читающих потоков.
                // чтобы не было ситуации, что все потоки одновременно читают
                read.acquire();
                try (Stream<String> currentLines = Files.lines(LARGE_FILE_PATH)) {
                    // пропустим указанное количество строк currentPosition
                    // и после этого атомарно увеличим currentPosition на COUNT
                    // теперь ограничим limit(COUNT),
                    // получается мы считали N-строк начиная с позиции M
                    // следующий поток будет считывать N-строк с позиции M+N и тд до конца файла
                    lines = currentLines
                            .skip(currentPosition.getAndAccumulate(COUNT, (prev, incrementValue) -> prev + incrementValue))
                            .limit(COUNT)
                            .collect(Collectors.toList());
                } finally {
                    read.release();
                }

                // если ни одной строки не считали, значит файл подошел к концу
                // выйдем из цикла
                if (lines.isEmpty()) {
                    System.out.println("id=" + currentId + "  finished");
                    break;
                }

                // сортируем файл
                Collections.sort(lines);

                // отсортированная последовательность будет сохранена во временный файл
                // с названием tmp_номер-потока_текущее-время.txt
                // так точно не будет пересечения по названию!
                Path file = Paths.get(TMP_FILE_PATH.toString(), FilesUtils.generateFileName(currentId));

                // семафор на запись ограничивает количество потоков,
                // одновременно пишуших в файлы, чтобы не было ситуации, что все потоки пишут одновременно
                write.acquire();
                try {
                    System.out.println("id=" + currentId + "  begining write");
                    Files.write(file, lines, Charset.forName("UTF-8"));
                } finally {
                    write.release();
                }
            }
        } catch (IOException | InterruptedException ex) {
            System.out.println("ERROR!!!!" + ex);
            return false;
        }
        return true;
    }
}
