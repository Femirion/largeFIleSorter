package com.puresteam;


import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Рабочий, читающий порцию данных из большого файла и сортирующий ее
 */
public class Worker implements Callable<Boolean> {

    /** Путь к файлу, потом понять на параметр */
    private static final String FILE_PATH = "/home/steam/program/test.txt";
    /** Количество считываемых в память строк */
    private static final int COUNT = 10;
    /** Текущая позиция в файле */
    private static AtomicLong currentPosition = new AtomicLong(0);

    @Override
    public Boolean call() {
        long currentId = Thread.currentThread().getId();
        List<String> lines;
        try  {
            while (true) {
                try (Stream<String> currentLines = Files.lines(Paths.get(FILE_PATH))) {
                    // пропустим указанное количество строк currentPosition
                    // и после этого атомарно увеличим currentPosition на COUNT
                    // теперь ограничим limit(COUNT),
                    // получается мы считали N-строк начиная с позиции M
                    // следующий поток будет считывать N-строк с позиции M+N и тд до конца файла
                    lines = currentLines
                            .skip(currentPosition.getAndAccumulate(COUNT, (prev, incrementValue) -> prev + incrementValue))
                            .limit(COUNT)
                            .collect(Collectors.toList());
                }

                // если ни одной строки не считали, значит файл подошел к концу
                // выйдем из цикла
                if (lines.isEmpty()) {
                    break;
                }

                // сортируем файл
                Collections.sort(lines);

                // отсортированная последовательность будет сохранена во временный файл
                // с названием tmp_номер-потока_текущее-время.txt
                // так точно не будет пересечения по названию!
                Path file = Paths.get("/home/steam/program/tmp_" + currentId + "_" + LocalDate.now() + ".txt");
                Files.write(file, lines, Charset.forName("UTF-8"));
            }
        } catch (IOException e) {
            return false;
        }
        return true;
    }
}
