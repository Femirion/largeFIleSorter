package com.puresteam.utils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Генератор файлов-заглушек для сортировки
 */
public class GenerateTestData {

    /** Количество строк в итоговом файле 50_000_000L ~ 750Мб, 600_000_000 ~ 9Гб*/
    private static final long LINE_COUNT = 600_000_000L;
    /** Выходной файл */
    private static final String FILE_NAME = "/media/steam/E4DE4FB4DE4F7DB4/tmp/large_file.txt";

    public static void main(String[] args) {
        Path path = Paths.get(FILE_NAME);
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            for (long i = 0; i < LINE_COUNT; i++) {
                String content = Long.toHexString(Double.doubleToLongBits(Math.random()));
                if (i % 10 == 0) {
                    content += "\n";
                }

                writer.write(content);
            }
        } catch (IOException e) {
            System.out.printf("Generate largeFile error" + e.getMessage());
        }
    }

}
