package com.puresteam.worker.entity;

import java.nio.file.Path;

/**
 * Пара фалов для слияния
 */
public class PairFile {

    private final Path firstFile;
    private final Path secondFile;
    private final String path;

    /**
     * Конструктор с параметрами
     *
     * @param firstFile первый файл
     * @param secondFile второй файл
     */
    public PairFile(Path firstFile, Path secondFile, String path) {
        this.firstFile = firstFile;
        this.secondFile = secondFile;
        this.path = path;
    }

    public Path getFirstFile() {
        return firstFile;
    }

    public Path getSecondFile() {
        return secondFile;
    }

    public String getPath() {
        return path;
    }
}
