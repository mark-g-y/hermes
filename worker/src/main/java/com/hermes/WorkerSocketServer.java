package com.hermes;

import com.hermes.network.packet.AckPacket;
import com.hermes.network.packet.AssignPartitionPacket;
import com.hermes.network.packet.Packet;
import com.hermes.network.SocketServer;
import com.hermes.zookeeper.ZKManager;
import com.hermes.zookeeper.ZKPaths;
import com.hermes.zookeeper.ZKUtility;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class WorkerSocketServer extends SocketServer {
    private String id;

    public WorkerSocketServer(String id, int port) {
        super(port);
        this.id = id;
    }

    @Override
    protected void onClientConnected(HandlerThread thread) {
        // do nothing, no need to send anything
    }

    @Override
    protected void onReceive(HandlerThread thread, Packet packet) {
        // handle received packet
        switch (packet.TYPE) {
            case ASSIGN_PARTITION:
                assignChannel(thread, (AssignPartitionPacket)packet);
                break;
            default:
                System.out.println("Error - received unrecognized packet type " + packet.TYPE);
        }
    }

    private void assignChannel(HandlerThread thread, AssignPartitionPacket packet) {
        ZooKeeper zk = ZKManager.get();
        try {
            // <TODO> decide whether maintain list of channels this worker is responsible for? only use case is if
            // <TODO> same worker is reassigned to different partition. The ephermeral won't die because worker is still
            // <TODO> alive, but worker should be removed from partition
            ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + packet.getPartition() + "/" + id, null,
                                         ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            send(thread, new AckPacket(packet.MESSAGE_ID));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
