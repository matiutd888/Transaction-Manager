package cp1.solution;

import cp1.base.Resource;
import cp1.base.ResourceOperation;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

public class Transaction {
    private final long startTime;
    private final Thread thread;
    private final Map<Resource, Stack<ResourceOperation>> resourcesChangedByTransaction = new HashMap<>();
    private TransactionState state = TransactionState.NOT_ABORTED;

    public Transaction(long startTime, Thread thread) {
        this.startTime = startTime;
        this.thread = thread;
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
        for (Map.Entry<Resource, Stack<ResourceOperation>> entry : resourcesChangedByTransaction.entrySet()) {
            Stack<ResourceOperation> s = entry.getValue();
            while (!s.empty()) {
                ResourceOperation op = s.pop();
                op.undo(entry.getKey());
            }
        }
    }

    public void updateOperationHistory(Resource resource, ResourceOperation operation) {
        resourcesChangedByTransaction.putIfAbsent(resource, new Stack<>());
        resourcesChangedByTransaction.get(resource).add(operation);
    }

    public void cancel() {
        state = TransactionState.ABORTED;
    }
}