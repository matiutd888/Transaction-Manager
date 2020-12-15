package cp1.solution;

import cp1.base.Resource;
import cp1.base.ResourceId;
import cp1.base.ResourceOperation;

import java.util.*;

public class Transaction {
    private TransactionState state;
    private long startTime;
    private Thread thread;
    private Map<Resource, List<ResourceOperation>> resourcesChangedByTransaction;

    public Transaction(long startTime, Thread thread) {
        this.startTime = startTime;
        this.thread = thread;
        state = TransactionState.NOT_ABORTED;
        resourcesChangedByTransaction = new HashMap<>();
    }

    public TransactionState getState() {
        return state;
    }

    public long getStartTime() {
        return startTime;
    }

    public Set<Resource> getResourcesChanged() {
        return resourcesChangedByTransaction.keySet();
    }

    public void rollback() {
        for (Map.Entry<Resource, List<ResourceOperation>> entry : resourcesChangedByTransaction.entrySet()) {
            for (ResourceOperation op : entry.getValue()) {
                op.undo(entry.getKey());
            }
        }
    }

    public void updateOperationHistory(Resource resource, ResourceOperation operation) {
        resourcesChangedByTransaction.putIfAbsent(resource, new LinkedList<>());
        resourcesChangedByTransaction.get(resource).add(operation);
    }

    public void cancel() {
        state = TransactionState.ABORTED;
    }
}
