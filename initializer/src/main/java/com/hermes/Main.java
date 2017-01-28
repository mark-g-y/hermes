package com.hermes;

public class Main {
    private static void printHelp() {
        System.out.println("Arguments: <zookeeper_url>");
        System.out.println("\t<zookeeper_url> : the URL of the ZooKeeper node, or comma separated URLs if multiple nodes");
        System.out.println();
        System.out.println("Entering 'help' as an argument prints this help menu");
        System.out.println();
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Error - invalid arguments\n");
            printHelp();
            System.exit(1);
        } else if ("help".equals(args[0])) {
            printHelp();
            System.exit(1);
        }
        Initializer initializer = new Initializer(args[0]);
        initializer.run();
    }
}
