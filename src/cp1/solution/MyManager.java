package cp1.solution;

import cp1.base.*;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;

public class MyManager implements TransactionManager {
    private static final boolean debug = false;
    private ConcurrentHashMap<Thread, Transaction> transactions; // Current thread transaction/
    private LocalTimeProvider timeProvider;
    private ConcurrentMap<ResourceId, Resource> resources;
    private ConcurrentMap<ResourceId, Thread> operating; // Which thread has access to resource.
    private ConcurrentMap<Thread, ResourceId> waiting; // Holds information about a resource that a thred is waiting for.
    private ConcurrentMap<ResourceId, Semaphore> waitForResource; // For every Resource it stores a semaphore that is used to wait for resource to get free.
    private ConcurrentMap<ResourceId, Integer> countWaitingForResource; // Holds information about number of threads waiting for resource.

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

        // If there exist other active transaction, raise AnotherTransactionActive.
        if (transactions.containsKey(currentThread))
            throw new AnotherTransactionActiveException();
        Transaction transaction = new Transaction(timeProvider.getTime(), currentThread);
        transactions.put(currentThread, transaction);
    }

    @Override
    public void operateOnResourceInCurrentTransaction(ResourceId rid, ResourceOperation operation) throws
            NoActiveTransactionException, UnknownResourceIdException, ActiveTransactionAborted, ResourceOperationException, InterruptedException {
        Thread currentThread = Thread.currentThread();
        Transaction currTransaction = transactions.get(currentThread);
        Resource res = resources.get(rid);
        if (currTransaction == null) {
            throw new NoActiveTransactionException();
        }
        if (currTransaction.getState() == TransactionState.ABORTED) {
            throw new ActiveTransactionAborted();
        }
        if (res == null) {
            throw new UnknownResourceIdException(rid);
        }

        Thread operatingThread;
        boolean hasAccess = false;
        synchronized (operating) {
            operatingThread = operating.get(rid);
            int id = operatingThread == null ? -1 : (int) operatingThread.getId();

            if (debug)
                System.out.println("WĄTEK " + currentThread.getId() + " PROBUJE SIE DOSTAC DO " + rid + " w posiadaniu " + id);
            hasAccess = (operatingThread == null && countWaitingForResource.getOrDefault(rid, 0) == 0)
                    || operatingThread == currentThread;
            if (hasAccess) {
                operating.put(rid, currentThread);
                if (debug)
                    System.out.println("WĄTEK " + currentThread.getId() + " DOSTAJE " + rid + " w posiadaniu " + id);
            } else {
                // Mark, that you will be waiting.
                waiting.put(currentThread, rid);
                int howManyWaiting = countWaitingForResource.getOrDefault(rid, 0);
                countWaitingForResource.put(rid, howManyWaiting + 1);
                if (operatingThread != null) {
                    checkForCycle(rid);
                }
            }
        }

        if (!hasAccess) {
            // If you are going to be waiting because a
            // awaken resource hasnt claimed his resource
            // dont check for Cycle, it is not possible.

            Semaphore s = waitForResource.get(rid);
            try {
                s.acquire();
            } catch (InterruptedException interruptedException) {
                // You didnt get access, undo your waiting.
                if (debug)
                    System.out.println("WĄTEK " + Thread.currentThread().getId() + " INTERRPUTED WHILE WAITING!");
                synchronized (operating) {
                    waiting.remove(currentThread, rid);

                    int howManyWaiting = countWaitingForResource.getOrDefault(rid, 0);
                    countWaitingForResource.put(rid, howManyWaiting - 1);
                    
                    // Kod na złośliwy przeplot: Po interrputedException
                    // podczas czekania na resource wątek nie zdąży zaznaczyć, że nie jest już (jedynym)
                    // czekającym, w tym samym czasie wątek mający dotyczas dostęp do resource zwalnia go.
                    // Semafor będzie ustawiony na 1, ale wątek nie chce już z niego korzystać.
                    // Ustawiamy więc semafor na 0.
                    if (howManyWaiting == 1 && s.availablePermits() > 0) {
                        s.drainPermits();
                    }
                }
                if (currTransaction.getState() == TransactionState.ABORTED)
                    throw new ActiveTransactionAborted();
                throw interruptedException;
            }
            // If a thread made it here, it has access to resource.
            synchronized (operating) {
                if (debug) System.out.println("WĄTEK " + currentThread.getId() + " DOSTAJE " + rid + " po czekaniu");

                operating.put(rid, currentThread);
                waiting.remove(currentThread, rid);
                int howManyWaiting = countWaitingForResource.getOrDefault(rid, 0);
                countWaitingForResource.put(rid, howManyWaiting - 1);
            }
        }
        try {
            operation.execute(res);
        } catch (ResourceOperationException roe) {
            throw roe;
        }
        if (currentThread.isInterrupted()) {
            if (debug)
                System.out.println("WĄTEK " + Thread.currentThread().getId() + " INTERRPUTED WHILE OPERATION! " + rid);
            operation.undo(res);
            throw new InterruptedException();
        }
        currTransaction.updateOperationHistory(res, operation);
    }

    private void checkForCycle(ResourceId resourceId) {
        try {
            if (debug)
                System.out.println("WĄTEK " + Thread.currentThread().getId() + " SPRAWDZA CYKL CZEKAJĄC NA " + resourceId);
            boolean end = false;
            boolean isCycle = false;
            ResourceId startResource = resourceId;
            Thread youngestThread;

            Thread itThread = operating.get(resourceId);
            youngestThread = itThread;

            long maxTime = transactions.get(itThread).getStartTime();
            while (!end) {
                ResourceId waitingRes = waiting.get(itThread);
                if (waitingRes == startResource) {
                    // Cycle found!
                    isCycle = true;
                    end = true;
                }
                if (waitingRes == null)
                    return;
                itThread = operating.get(waitingRes);
                if (itThread == null)
                    return;

                Transaction itTransaction = transactions.get(itThread);

                // Shouldnt be nessesarry, but lets check anyway.
                if (itTransaction.getState() == TransactionState.ABORTED)
                    return;

                long time = itTransaction.getStartTime();
                if (time > maxTime || (time == maxTime && itThread.getId() > youngestThread.getId())) {
                    maxTime = time;
                    youngestThread = itThread;
                }
            }
            if (isCycle) {
                if (debug) System.out.println("START TIMES");
                for (Map.Entry<Thread, Transaction> e : transactions.entrySet()) {
                    if (debug) System.out.println(e.getKey().getId() + " start time = " + e.getValue().getStartTime());
                }
                if (debug) System.out.println("Cykl wykryty, watek: " + youngestThread.getId());
                // cancel
                Transaction toCancel = transactions.get(youngestThread);
                toCancel.cancel();
                youngestThread.interrupt();
            }
        } finally {
            if (debug) System.out.println("WĄTEK " + Thread.currentThread().getId() + " kończy sprawdzanie!");
        }
    }

    @Override
    public void commitCurrentTransaction() throws NoActiveTransactionException, ActiveTransactionAborted {
        Thread currentThread = Thread.currentThread();
        Transaction currentTransaction = transactions.get(currentThread);
        if (currentTransaction == null)
            throw new NoActiveTransactionException();
        if (currentTransaction.getState() == TransactionState.ABORTED)
            throw new ActiveTransactionAborted();


        // Free resources.
        Set<Resource> changedResources = currentTransaction.getResourcesChanged();
        synchronized (operating) {
            for (Map.Entry<ResourceId, Thread> entry : operating.entrySet()) {
                if (entry.getValue() == currentThread) {
                    ResourceId rid = entry.getKey();
                    int howManyWaiting = countWaitingForResource.getOrDefault(rid, 0);
                    operating.remove(rid);
                    if (debug) System.out.println("WĄTEK " + currentThread.getId() + " removing " + rid);
                    if (howManyWaiting > 0) {
                        Semaphore s = waitForResource.get(rid);
                        s.release();
                    }
                }
            }
            transactions.remove(currentThread);
            if (debug) System.out.println("WĄTEK " + currentThread.getId() + " usuwa transakcję!");
        }
    }

    @Override
    public void rollbackCurrentTransaction() {
        Thread currentThread = Thread.currentThread();
        Transaction currentTransaction = transactions.get(currentThread);
        if (currentTransaction == null)
            return;
        currentTransaction.rollback();
        // Free resources.
        Set<Resource> changedResources = currentTransaction.getResourcesChanged();
        synchronized (operating) {
            for (Map.Entry<ResourceId, Thread> entry : operating.entrySet()) {
                if (entry.getValue() == currentThread) {
                    ResourceId rid = entry.getKey();
                    int howManyWaiting = countWaitingForResource.getOrDefault(rid, 0);
                    operating.remove(rid);
                    if (debug) System.out.println("WĄTEK " + currentThread.getId() + " removing " + rid);
                    if (howManyWaiting > 0) {
                        Semaphore s = waitForResource.get(rid);
                        s.release();
                    }
                }
            }
            transactions.remove(currentThread);
            if (debug) System.out.println("WĄTEK " + currentThread.getId() + " usuwa transakcję!");
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

    public void print() {
        if (debug) System.out.println(transactions.size() + ", "
                + operating.size() + ", "
                + waiting.size() + ", "
                + waitForResource.size() + ", "
                + countWaitingForResource);
    }
}
