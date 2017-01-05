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
import com.hermes.worker.select.state.AllocatingNewWorkers;
import com.hermes.worker.select.state.DoneSelecting;
import com.hermes.worker.select.state.Selecting;
import com.hermes.zookeeper.ZKPaths;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class WorkerManager {
    public static void allocateWorkersForPartition(String partition, int numWorkers) {
        allocateWorkersForPartition(partition, numWorkers, Collections.emptyList());
    }

    public static void allocateWorkersForPartition(String partition, int numWorkers, Collection<Worker> blackListed) {
        List<Worker> workers;
        try {
            workers = WorkerManager.getAllWorkers();
        } catch (Exception e) {
            return;
        }
        Set<String> blackListedIds = blackListed.stream().map((worker) -> worker.getId()).collect(Collectors.toSet());
        workers = workers.stream().filter((worker) -> !blackListedIds.contains(worker.getId())).collect(Collectors.toList());
        WorkerAllocator workerAllocator = new WorkerAllocator();
        int numAllocated = 0;
        for (int i = 0; i < workers.size() && numAllocated < numWorkers; i++) {
            if (workerAllocator.allocateToPartition(workers.get(i), partition)) {
                numAllocated++;
            }
        }
    }

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

    public static List<Worker> selectWorkersForPartition(String partition, int numToSelect) throws Exception {
        return selectWorkersForPartition(partition, numToSelect, Collections.emptySet());
    }

    public static List<Worker> selectWorkersForPartition(String partition, int numToSelect, Collection<Worker> blacklisted)
            throws Exception {
        List<Worker> workers = getAllWorkersForPartition(partition);
        State defaultState = new Selecting();
        State[] states = new State[] {new AllocatingNewWorkers(), new DoneSelecting(), defaultState};
        Fsm fsm = new Fsm();
        fsm.addStates(Arrays.asList(states));
        fsm.getContext().attrs.put("workers", workers);
        fsm.getContext().attrs.put("partition", partition);
        fsm.getContext().attrs.put("blacklisted", blacklisted);
        fsm.getContext().attrs.put("num_to_select", numToSelect);

        CompletableFuture<Collection<Worker>> future = new CompletableFuture<>();
        fsm.addTrigger(DoneSelecting.NAME, (context) -> future.complete((Collection<Worker>)context.attrs.get("selected_workers")));
        fsm.run(defaultState);
        return new ArrayList(future.get());
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
