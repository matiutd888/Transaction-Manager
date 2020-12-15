package cp1.solution;

import cp1.base.*;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;

public class MyManager implements TransactionManager {
    private ConcurrentMap<Thread, Transaction> transactions; // Przechowuje Stan
    // Transakcji dla każdego wątku
    private LocalTimeProvider timeProvider;
    private ConcurrentMap<ResourceId, Resource> resources;
    private ConcurrentMap<ResourceId, Thread> operating; // Przechowuje ID wątku który uzyskał dostęp do zasobu w aktywnej transakcji.
    private ConcurrentMap<Thread, ResourceId> waiting; // Przechowuje Resource, na który czeka wątek o danym ID.
    private ConcurrentMap<ResourceId, Semaphore> waitForResource; // Przechowuje semaphore na którym czekają wątki czekające na dany resource.
    private ConcurrentMap<ResourceId, Integer> countWaitingForResource; // Przechowuje liczbę wątków czekających na dany resos

    public MyManager(Collection<Resource> resources, LocalTimeProvider timeProvider) {
        this.resources = new ConcurrentHashMap<>();
        waitForResource = new ConcurrentHashMap<>();

        for (Resource r : resources) {
            this.resources.put(r.getId(), r);
            waitForResource.put(r.getId(), new Semaphore(0, true));
        }
        waiting = new ConcurrentHashMap<>();
        this.timeProvider = timeProvider;
        transactions = new ConcurrentHashMap<>();
        operating = new ConcurrentHashMap<>();
        countWaitingForResource = new ConcurrentHashMap<>();
    }

    @Override
    public void startTransaction() throws AnotherTransactionActiveException {
        Thread currentThread = Thread.currentThread();
        if (transactions.containsKey(currentThread))
            throw new AnotherTransactionActiveException();
        Transaction transaction = new Transaction(timeProvider.getTime(), currentThread);
        transactions.put(currentThread, transaction);
    }


    @Override
    public void operateOnResourceInCurrentTransaction(ResourceId rid, ResourceOperation operation) throws
            NoActiveTransactionException, UnknownResourceIdException, ActiveTransactionAborted, ResourceOperationException, InterruptedException {
        Thread currentThread = Thread.currentThread();
        if (!isTransactionActive())
            throw new NoActiveTransactionException();
        if (isTransactionAborted())
            throw new ActiveTransactionAborted();
        if (resources.get(rid) == null)
            throw new UnknownResourceIdException(rid);

        Thread operatingThread;
        boolean hasAccess = false;
        synchronized (operating) {
            operatingThread = operating.get(rid);
            hasAccess = (operatingThread == null && countWaitingForResource.getOrDefault(rid, 0) == 0)
                    || operatingThread == currentThread;
            if (hasAccess) {
                operating.put(rid, currentThread); // Zatwierdź, że to Ty teraz czekasz.
            } else {
                // Zapisz się na czekanie
                waiting.put(currentThread, rid);
                int howManyWaiting = countWaitingForResource.getOrDefault(rid, 0);
                countWaitingForResource.put(rid, howManyWaiting + 1);
            }
        }
        Transaction currTransaction = transactions.get(currentThread);

        if (!hasAccess) {
            if (operatingThread != null)
                checkForCycle(rid);
            Semaphore s = waitForResource.get(rid);
            try {
                s.acquire();
            } catch (InterruptedException interruptedException) {
                waiting.remove(currentThread, rid);

                int howManyWaiting = countWaitingForResource.getOrDefault(rid, 0);
                countWaitingForResource.put(rid, howManyWaiting - 1);

                throw interruptedException;
            }
            // currTransaction.addControlledResource(rid); // tę informację i tak sprawdzam tylko ja
            synchronized (operating) {
                // operating.remove(rid);
                operating.put(rid, currentThread);
                waiting.remove(currentThread, rid);
                int howManyWaiting = countWaitingForResource.getOrDefault(rid, 0);
                countWaitingForResource.put(rid, howManyWaiting - 1);
            }
        }
        Resource res = resources.get(rid);
        try {
            operation.execute(res);
        } catch (ResourceOperationException roe) {
            throw roe;
        }
        if (currentThread.isInterrupted()) {
            operation.undo(res); // TODO czy na pewno?
            throw new InterruptedException();
        }

        currTransaction.updateOperationHistory(res, operation);
    }

    @Override
    public void commitCurrentTransaction() throws NoActiveTransactionException, ActiveTransactionAborted {
        Thread thread = Thread.currentThread();
        if (!isTransactionActive())
            throw new NoActiveTransactionException();
        if (isTransactionAborted())
            throw new ActiveTransactionAborted();
        Transaction transaction = transactions.get(thread);
        if (transaction == null)
            throw new NoActiveTransactionException();
        transactions.remove(thread);
        synchronized (operating) {
            for (Resource r : transaction.getResourcesChangedByTransaction()) {
                int howManyWaiting = countWaitingForResource.getOrDefault(r.getId(), 0);
                operating.remove(r.getId());
                if (howManyWaiting > 0) {
                    Semaphore s = waitForResource.get(r.getId());
                    s.release();
                }
            }
        }
    }

    void checkForCycle(ResourceId resourceId) {
        boolean end = false;
        boolean czyCykl = false;
        ResourceId startResource = resourceId;
        synchronized (operating) {
            Thread itThread = operating.get(resourceId);
            Thread youngestThread = itThread;
            long maxTime = transactions.get(itThread).getStartTime();
            while (!end) {
                ResourceId waitingRes = waiting.get(itThread);
                if (waitingRes == startResource) {
                    // Znaleziono cykl;
                    czyCykl = true;
                    end = true;
                }
                if (waitingRes == null)
                    return;
                itThread = operating.get(waitingRes);
                if (itThread == null) {
                    // System.out.println("NIKT NIE OPERUJE A KTOS CZEKA? WTF");
                    return;
                }
                Transaction itTransaction = transactions.get(itThread);
                long time = itTransaction.getStartTime();
                if (time > maxTime) {
                    maxTime = time;
                    youngestThread = itThread;
                }
            }
            if (czyCykl) {
                // obudź
                Transaction toCancel = transactions.get(youngestThread);
                toCancel.cancel();
                youngestThread.interrupt();
            }
        }

    }

    @Override
    public void rollbackCurrentTransaction() {
        Thread thread = Thread.currentThread();
        Transaction transaction = transactions.get(thread);
        if (transaction == null)
            return;
        transactions.remove(thread);
        transaction.rollback();
        synchronized (operating) {
            for (Resource r : transaction.getResourcesChangedByTransaction()) {
                Semaphore s = waitForResource.get(r.getId());
                if (s.hasQueuedThreads())
                    s.release();
                else
                    operating.remove(thread);
            }
        }
    }

    @Override
    public boolean isTransactionActive() {
        Transaction t = transactions.get(Thread.currentThread());
        if (t == null)
            return false;
        return true;
    }

    @Override
    public boolean isTransactionAborted() {
        Thread thread = Thread.currentThread();
        Transaction t = transactions.get(thread);
        if (t == null)
            return false;
        return t.getState() == TransactionState.ABORTED;
    }
}
