package com.puresteam.worker.entity;

/**
 * Пара фалов для слияния
 */
public class PairFile {

    private final String firstFile;
    private final String secondFile;
    private final String path;

    /**
     * Конструктор с параметрами
     *
     * @param firstFile первый файл
     * @param secondFile второй файл
     */
    public PairFile(String firstFile, String secondFile, String path) {
        this.firstFile = firstFile;
        this.secondFile = secondFile;
        this.path = path;
    }

    public String getFirstFile() {
        return firstFile;
    }

    public String getSecondFile() {
        return secondFile;
    }

    public String getPath() {
        return path;
    }
}
