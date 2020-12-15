package cp1.solution;

import cp1.base.Resource;
import cp1.base.ResourceId;
import cp1.base.ResourceOperation;

import java.util.*;

public class Transaction {
    private TransactionState state;
    private long startTime;
    private Thread thread;
    private List<Resource> resourcesChangedByTransaction;
    private List<ResourceOperation> operationsApplied;
    private Set<ResourceId> controlledResources;
    public Transaction(long startTime, Thread thread) {
        this.startTime = startTime;
        this.thread = thread;
        state = TransactionState.NOT_ABORTED;
        resourcesChangedByTransaction = new LinkedList<>();
        operationsApplied = new LinkedList<>();
        controlledResources = new TreeSet<>();
    }

    public List<ResourceOperation> getOperationsApplied() {
        return operationsApplied;
    }

    public TransactionState getState() {
        return state;
    }

    public long getStartTime() {
        return startTime;
    }

    public List<Resource> getResourcesChangedByTransaction() {
        return resourcesChangedByTransaction;
    }

    public void commit() {

    }

    public void rollback() {
        int numOfOperations = resourcesChangedByTransaction.size();
        Iterator headRes = resourcesChangedByTransaction.iterator();
        Iterator headOp = operationsApplied.iterator();
        while (headRes.hasNext()) {
            Resource resource = (Resource) headRes.next();
            ResourceOperation operation = (ResourceOperation) headOp.next();
            operation.undo(resource);
        }
    }

    public void updateOperationHistory(Resource resource, ResourceOperation operation) {
        resourcesChangedByTransaction.add(resource);
        operationsApplied.add(operation);
    }

    public int getNumOfControlledResources() {
        return controlledResources.size();
    }

    public void addControlledResource(ResourceId rid) {
        controlledResources.add(rid);
    }

    public void cancel() {
        state = TransactionState.ABORTED;
    }
}
