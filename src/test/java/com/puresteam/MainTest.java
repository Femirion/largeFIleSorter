package com.puresteam;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Тест для основного класса приложения
 */
public class MainTest {

    @Test
    public void getName() throws Exception {
        Main subj = new Main();
        String name = subj.getName();

        assertEquals(name, "name");
    }

}