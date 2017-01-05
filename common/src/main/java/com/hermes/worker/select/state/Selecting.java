package com.hermes.worker.select.state;

import com.hermes.fsm.Context;
import com.hermes.fsm.State;
import com.hermes.worker.WorkerManager;
import com.hermes.worker.metadata.Worker;

import java.util.*;
import java.util.stream.Collectors;

public class Selecting implements State {
    public static final String NAME = "selecting";

    @Override
    public State execute(Context context) {
        Collection<Worker> blacklisted = (Collection<Worker>)context.attrs.get("blacklisted");
        String partition = (String)context.attrs.get("partition");
        List<Worker> workers;
        try {
            workers = WorkerManager.getAllWorkersForPartition(partition);
        } catch (Exception e) {
            return context.states.getByName(Selecting.NAME);
        }
        int numToSelect = (int)context.attrs.get("num_to_select");
        Set<String> blacklistedIds = blacklisted.stream().map((worker) -> worker.getId()).collect(Collectors.toSet());
        int numCurrentBlacklisted = (int)workers.stream().filter((worker) -> blacklistedIds.contains(worker.getId())).count();
        Set<Worker> selected = new HashSet<>();
        if (numToSelect > workers.size() - numCurrentBlacklisted) {
            context.attrs.put("partition", partition);
            return context.states.getByName(AllocatingNewWorkers.NAME);
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
        context.attrs.put("selected_workers", selected);
        return context.states.getByName(DoneSelecting.NAME);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
