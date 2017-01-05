package com.hermes.message;

import com.hermes.worker.metadata.Worker;
import com.hermes.network.packet.MessagePacket;

import java.util.List;

public class Message {
    private MessagePacket messagePacket;
    private List<Worker> backups;

    public Message(MessagePacket messagePacket, List<Worker> backups) {
        this.messagePacket = messagePacket;
        this.backups = backups;
    }

    public MessagePacket getMessagePacket() {
        return messagePacket;
    }

    public List<Worker> getBackups() {
        return backups;
    }
}
