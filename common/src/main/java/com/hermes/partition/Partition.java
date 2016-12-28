package com.hermes.partition;

public class Partition {
    public static String get(String channelName) {
        return Integer.toString(Math.abs(channelName.hashCode() % PartitionConfigs.NUM_PARTITIONS));
    }
}
