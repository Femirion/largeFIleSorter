package com.puresteam;

/**
 * Created by steam on 11.09.17.
 */
public class Main {


    public static void main(String[] args) {


        Thread worker1 = new Thread(new Worker());
        worker1.start();

        Thread worker2 = new Thread(new Worker());
        worker2.start();

        Thread worker3 = new Thread(new Worker());
        worker3.start();

        Thread worker4 = new Thread(new Worker());
        worker4.start();

    }

    public String getName() {
        return "name";
    }
}
