package com.hermes.os;

public class MemoryUsage {
    public static double getPercentage() {
        long total = Runtime.getRuntime().totalMemory();
        long used  = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        return used * 100.0 / total;
    }
}
