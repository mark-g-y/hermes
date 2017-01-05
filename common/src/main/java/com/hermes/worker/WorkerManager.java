package com.hermes.worker;

import com.hermes.client.workerassignment.state.AllocatingWorker;
import com.hermes.client.workerassignment.state.Disconnected;
import com.hermes.client.workerassignment.state.FinishedAssigningWorkers;
import com.hermes.client.workerassignment.state.GettingWorkers;
import com.hermes.fsm.Fsm;
import com.hermes.fsm.State;
import com.hermes.partition.Partition;
import com.hermes.worker.allocation.WorkerAllocator;
import com.hermes.worker.metadata.Worker;
import com.hermes.worker.retrieve.state.FinishedGettingAllWorkers;
import com.hermes.worker.retrieve.state.GettingAllWorkers;
import com.hermes.zookeeper.ZKPaths;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class WorkerManager {
    public static List<Worker> getAllWorkersForChannel(String channelName, Watcher workerWatcher) throws
                                                                                                  InterruptedException,
                                                                                                  ExecutionException {
        return getAllWorkersForPartition(Partition.get(channelName), workerWatcher);
    }

    public static List<Worker> getAllWorkersForChannel(String channelName) throws InterruptedException,
                                                                                  ExecutionException {
        return getAllWorkersForChannel(channelName, null);
    }

    public static List<Worker> getAllWorkersForPartition(String partition, Watcher workerWatcher) throws
                                                                                                  InterruptedException,
                                                                                                  ExecutionException {
        State defaultState = new Disconnected();
        State[] states = new State[] {new AllocatingWorker(), new FinishedAssigningWorkers(), new GettingWorkers(workerWatcher),
                                      defaultState };
        Fsm fsm = new Fsm();
        fsm.addStates(Arrays.asList(states));
        fsm.getContext().attrs.put("partition", partition);

        CompletableFuture<List<Worker>> future = new CompletableFuture<>();
        fsm.addTrigger(FinishedAssigningWorkers.NAME, (context) -> future.complete((List<Worker>)context.attrs.get("workers")));
        fsm.run(defaultState);

        return future.get();
    }

    public static List<Worker> getAllWorkersForPartition(String partition) throws ExecutionException,
                                                                                  InterruptedException {
        return getAllWorkersForPartition(partition, null);
    }

    public static List<Worker> getAllWorkers() throws InterruptedException, ExecutionException {
        State defaultState = new com.hermes.worker.retrieve.state.Disconnected();
        State[] states = new State[] {new GettingAllWorkers(), new FinishedGettingAllWorkers(), defaultState};
        Fsm fsm = new Fsm();
        fsm.addStates(Arrays.asList(states));

        CompletableFuture<List<Worker>> future = new CompletableFuture<>();
        fsm.addTrigger(FinishedGettingAllWorkers.NAME, (context) -> future.complete((List<Worker>)context.attrs.get("workers")));
        fsm.run(defaultState);

        List<Worker> workers = future.get();
        Collections.sort(workers);
        return workers;
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
        Collections.sort(workers);
        int numSelected = 0;
        for (int i = 0; i < workers.size() && numSelected < numToSelect; i++) {
            Worker worker = workers.get(i);
            if (!blacklistedIds.contains(worker.getId())) {
                numSelected++;
                selected.add(worker);
            }
        }
        return new ArrayList<>(selected);
    }

    public static List<Worker> getWorkers(ZooKeeper zk, List<String> workerIds) {
        List<Worker> workers = new ArrayList<>();
        for (String workerId : workerIds) {
            try {
                String url = new String(zk.getData(ZKPaths.WORKERS + "/" + workerId, null, null));
                double load = Double.parseDouble(new String(zk.getData(ZKPaths.WORKER_LOADS + "/" + workerId, null, null)));
                workers.add(new Worker(workerId, url, load));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return workers;
    }
}
