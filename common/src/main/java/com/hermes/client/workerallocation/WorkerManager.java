package com.hermes.client.workerallocation;

import com.hermes.client.workerallocation.state.*;
import com.hermes.fsm.Context;
import com.hermes.fsm.Fsm;
import com.hermes.fsm.State;
import com.hermes.partition.Partition;
import org.apache.zookeeper.Watcher;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class WorkerManager {
    public static List<Worker> getAllWorkersForChannel(String channelName, Watcher workerWatcher) throws
                                                                                                  InterruptedException,
                                                                                                  ExecutionException {
        State defaultState = new Disconnected();
        State[] states = new State[] {new AssigningWorker(), new FinishedAllocatingWorkers(), new GettingWorkers(workerWatcher),
                                      defaultState };
        Fsm fsm = new Fsm();
        fsm.addStates(Arrays.asList(states));
        fsm.getContext().attrs.put("partition", Partition.get(channelName));

        final CompletableFuture<List<Worker>> future = new CompletableFuture<>();
        fsm.addTrigger(FinishedAllocatingWorkers.NAME, (Context context) -> future.complete((List<Worker>)context.attrs.get("workers")));
        fsm.run(defaultState);

        return future.get();
    }

    public static List<Worker> getAllWorkersForChannel(String channelName) throws InterruptedException,
                                                                                  ExecutionException {
        return getAllWorkersForChannel(channelName, null);
    }

    public static List<Worker> selectWorkers(List<Worker> workers, int numToSelect) {
        return selectWorkers(workers, numToSelect, Collections.emptySet());
    }

    public static List<Worker> selectWorkers(List<Worker> workers, int numToSelect, Collection<Worker> blacklisted) {
        Set<String> blacklistedIds = blacklisted.stream().map((worker) -> worker.getId()).collect(Collectors.toSet());
        int numCurrentBlacklisted = (int)workers.stream().filter((worker) -> blacklistedIds.contains(worker.getId())).count();
        Set<Worker> selected = new HashSet<>();
        if (numToSelect > workers.size() - numCurrentBlacklisted) {
            throw new RuntimeException("Not enough workers to satisfy request");
        }
        for (int i = 0; i < numToSelect; i++) {
            int index = (int) (Math.random() * workers.size());
            Worker worker = workers.get(index);
            // <TODO> use worker load to determine which worker to select instead of at random
            if (selected.contains(worker) || blacklistedIds.contains(worker.getId())) {
                i-=1;
            } else {
                selected.add(worker);
            }
        }
        return new ArrayList<>(selected);
    }
}
