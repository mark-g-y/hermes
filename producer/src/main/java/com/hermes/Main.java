package com.hermes;

import com.hermes.zookeeper.ZKManager;

public class Main {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Error - incorrect number of arguments");
            System.out.println("Argument should be <zookeeper url> <channel name>");
            System.exit(1);
        }
        ZKManager.init(args[0]);
        Producer producer = new Producer(args[1]);
        producer.start();
    }
}
