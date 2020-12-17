package cp1.solution;

import cp1.base.*;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;

public class MyManager implements TransactionManager {
    private final LocalTimeProvider timeProvider;
    /**
     * Current thread transaction
     */
    private final ConcurrentMap<Thread, Transaction> transactions = new ConcurrentHashMap<>();
    /**
     * Current thread transaction
     */
    private final ConcurrentMap<ResourceId, Resource> resources = new ConcurrentHashMap<>();
    /**
     * Which thread has access to resource.
     */
    private final ConcurrentMap<ResourceId, Thread> operating = new ConcurrentHashMap<>();
    /**
     * Holds information about a resource that a thred is waiting for.
     */
    private final ConcurrentMap<Thread, ResourceId> waiting = new ConcurrentHashMap<>();
    /**
     * For every Resource it stores a semaphore that is used to wait for resource to get free.
     */
    private final ConcurrentMap<ResourceId, Semaphore> waitForResource = new ConcurrentHashMap<>();
    /**
     * Holds information about number of threads waiting for resource.
     */
    private final ConcurrentMap<ResourceId, Integer> countWaitingForResource = new ConcurrentHashMap<>();

    public MyManager(Collection<Resource> resources, LocalTimeProvider timeProvider) {
        for (Resource r : resources) {
            this.resources.put(r.getId(), r);
            waitForResource.put(r.getId(), new Semaphore(0, true));  // semafory w java domyslnie sa fair, niepotrzebny parametr, ale redundancja moze byc celowa i przydatna jesli chcemy dac do zrozumienia ze fakt: "fair = true" jest wazny. (subjectivity: $DEPENDS-ON-RATIONALE%)
        }
        this.timeProvider = timeProvider;
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
        boolean hasAccess = false; // IMO: GOOD, po usunieciu redundancji, ktos moglby sie zastanawiac czy nie zapomniales zainicjalizowac. (chodzi o to, ze boolean domyslnie inicjalizuje sie do false) (subjectivity: 50%)
        synchronized (operating) {
            operatingThread = operating.get(rid);
            int id = operatingThread == null ? -1 : (int) operatingThread.getId();

            System.out.println("WĄTEK " + currentThread.getId() + " PROBUJE SIE DOSTAC DO " + rid + " w posiadaniu " + id);
            hasAccess = (operatingThread == null && countWaitingForResource.getOrDefault(rid, 0) == 0)
                    || operatingThread == currentThread;
            if (hasAccess) {
                operating.put(rid, currentThread);
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
                System.out.println("WĄTEK " + Thread.currentThread().getId() + " INTERRPUTED WHILE WAITING!");
                synchronized (operating) {
                    waiting.remove(currentThread, rid);
                    // TODO do i need ochrona for this? IMO: TODOS są najs! (subjectivity: 40%)
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

                throw interruptedException;
            }
            // If a thread made it here, it has access to resource.
            synchronized (operating) {
                System.out.println("WĄTEK " + currentThread.getId() + " DOSTAJE " + rid + " po czekaniu");

                // operating.remove(rid);
                operating.put(rid, currentThread);
                waiting.remove(currentThread, rid);
                int howManyWaiting = countWaitingForResource.getOrDefault(rid, 0);
                countWaitingForResource.put(rid, howManyWaiting - 1);
            }
        }

        operation.execute(res);

        if (currentThread.isInterrupted()) {
            System.out.println("WĄTEK " + Thread.currentThread().getId() + " INTERRPUTED WHILE OPERATION! " + rid);
            operation.undo(res);
            throw new InterruptedException();
        }
        currTransaction.updateOperationHistory(res, operation);
    }

    private void checkForCycle(ResourceId resourceId) {
        try {
            System.out.println("WĄTEK " + Thread.currentThread().getId() + " SPRAWDZA CYKL CZEKAJĄC NA " + resourceId);
            boolean end = false;
            ResourceId startResource = resourceId;  // Probably delete this. I get why you did this. This redundancy might be good, but is VERY hard to swallow (chodzi o to ze start resource nie jest potrzebne, ale jego nazwa sugeruje zastosowanie w kodzie (iterowanie). z drugiej strony nie mozna zmienic nazwy parametru na startResource, bo to moze zastanowic uzytkownika funkcji (kod wywolujacy), a tego nalezy unikac) (subjectivity: 30%)
            Thread youngestThread;

            Thread itThread = operating.get(resourceId);
            youngestThread = itThread;

            long maxTime = -1;
            try {
                maxTime = transactions.get(itThread).getStartTime();
            } catch (NullPointerException e) {
                System.err.println("NULL POINTER EXCEPTION " + Thread.currentThread().getId() + ", " + itThread.getId() + ", " + resourceId);
            }
            while (!end) {
                ResourceId waitingRes = waiting.get(itThread);
                if (waitingRes == startResource) {
                    // Cycle found!
                    end = true;
                }
                if (waitingRes == null)
                    return;
                itThread = operating.get(waitingRes);
                if (itThread == null)
                    return;

                Transaction itTransaction = transactions.get(itThread);

                // Shouldnt be nessesarry, but lets check anyway. PIZDA. (subjectivity: 0%)
                if (itTransaction.getState() == TransactionState.ABORTED)
                    return;

                long time = itTransaction.getStartTime();
                if (time > maxTime || (time == maxTime && itThread.getId() > youngestThread.getId())) {
                    maxTime = time;
                    youngestThread = itThread;
                }
            }
            System.out.println("START TIMES");
            for (Map.Entry<Thread, Transaction> e : transactions.entrySet()) {
                System.out.println(e.getKey().getId() + " start time = " + e.getValue().getStartTime());
            }
            System.out.println("Cykl wykryty, watek: " + youngestThread.getId());
            // cancel
            Transaction toCancel = transactions.get(youngestThread);
            toCancel.cancel();
            youngestThread.interrupt();
        } finally {
            System.out.println("WĄTEK " + Thread.currentThread().getId() + " kończy sprawdzanie!");
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

        freeResources();
    }

    @Override
    public void rollbackCurrentTransaction() {
        Thread currentThread = Thread.currentThread();
        Transaction currentTransaction = transactions.get(currentThread);
        if (currentTransaction == null)
            return;
        currentTransaction.rollback();
        freeResources();
    }

    private void freeResources() {
        Thread currentThread = Thread.currentThread();
        synchronized (operating) {
            for (Map.Entry<ResourceId, Thread> entry : operating.entrySet()) {
                if (entry.getValue() == currentThread) {
                    ResourceId rid = entry.getKey();
                    int howManyWaiting = countWaitingForResource.getOrDefault(rid, 0);
                    operating.remove(rid);
                    System.out.println("WĄTEK " + currentThread.getId() + " removing " + rid);
                    if (howManyWaiting > 0) {
                        Semaphore s = waitForResource.get(rid);
                        s.release();
                    }
                }
            }
            transactions.remove(currentThread);
            System.out.println("WĄTEK " + currentThread.getId() + " usuwa transakcję!");
        }
    }

    @Override
    public boolean isTransactionActive() {
        Transaction t = transactions.get(Thread.currentThread());
        return t != null;
    }

    @Override
    public boolean isTransactionAborted() {
        Thread thread = Thread.currentThread();
        Transaction t = transactions.get(thread);
        if (t == null)
            return false;
        return t.getState() == TransactionState.ABORTED;
    }

    // DEBUG
    public void print() {
        System.out.println(String.format("%d, %d, %d, %d, %d",
                transactions.size(),
                operating.size(),
                waiting.size(),
                waitForResource.size(),
                countWaitingForResource.size()));
    }
}