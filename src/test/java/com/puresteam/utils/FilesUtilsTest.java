package com.puresteam.utils;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Тесты для FilesUtils
 */
public class FilesUtilsTest {

    @Test
    public void generateFileName() {
        long currentThreadId = 42L;
        String fileName = FilesUtils.generateFileName(currentThreadId);

        assertNotNull(fileName);
    }

}