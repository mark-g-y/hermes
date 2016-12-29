package com.hermes;


import com.hermes.utility.Parser;
import com.hermes.zookeeper.ZKManager;

public class Main {
    public static void main(String[] args) {
        if (args.length < 3 || args.length > 4 || !Parser.isInt(args[2])) {
            System.out.println("Error - incorrect argument format");
            System.out.println("Arguments should be <zookeeper host:port> <worker host> <worker port> [worker ID]");
            System.exit(1);
        }
        ZKManager.init(args[0]);
        Worker worker;
        if (args.length == 3) {
            worker = new Worker(args[1], Integer.parseInt(args[2]));
        } else {
            worker = new Worker(args[3], args[1], Integer.parseInt(args[2]));
        }
        worker.start();
    }
}
