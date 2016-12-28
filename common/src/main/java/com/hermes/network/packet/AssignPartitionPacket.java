package com.hermes.network.packet;

public class AssignPartitionPacket extends Packet {
    private String partition;
    public AssignPartitionPacket(String partition) {
        super(PacketType.ASSIGN_PARTITION);
        this.partition = partition;
    }

    public String getPartition() {
        return partition;
    }
}
