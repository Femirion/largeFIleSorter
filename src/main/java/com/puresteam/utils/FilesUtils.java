package com.puresteam.utils;

import java.time.LocalDateTime;

/**
 * Утилитные методы при работе с файлами
 */
public abstract class FilesUtils {

    /**
     * Генерация имени файла
     *
     * @param currentThreadId идентификатор потока
     * @return название для файла
     */
    public static String generateFileName(long currentThreadId) {
        // при формировании название участвует идентификатор потока
        // и текущая дата-время, это позволяет избежать пересечений в именах файлов
        return String.format("tmp_%s_%s.txt", currentThreadId, LocalDateTime.now());
    }

}
