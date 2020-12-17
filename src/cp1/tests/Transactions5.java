package cp1.demo;

import cp1.base.*;
import cp1.solution.TransactionManagerFactory;

import java.util.*;

/** Author : Krzysztof Jankowski
 *
 * Sposób użytkowania:
 * W pętli nieskończonej wypisują się kolejne wartości licznika.
 * Nie powinno być błędów.
 * UWAGA: LICZBY WYPISUJĄ SIĘ NAPRAWDĘ WOLNO. PROSZĘ CIERPLIWIE CZEKAĆ. CHYBA ŻE JEST DEADLOCK, TO NIE CZEKAĆ.
 *
 * Działanie:
 * 20 wątków losowo zajmuje ustalone zasoby oraz czasami losowe zasoby. co jakiś czas pewien wątek
 * jest przerywany.
 *
 * UWAGA2: NIE WIEM CO MOŻE ZDARZYĆ SIĘ W TRAKCIE, BO TEST JEST MOCNO LOSOWY.
 *
 * Można zmienić N na więcej wątków lub mniej jeśi się chce.
 * Dodatkowo wypisują się kropki, żeby nie czekać bezsensownie na deadlocka.
 * */

public class Transactions5 {

    private static final long BASE_WAIT_TIME = 500;

    public static void main(String[] args) {
        int licznik = 0;
        while (true) {
            licznik++;
            System.out.println(licznik);
            // Set up resources.
            ResourceImpl r0 = new ResourceImpl(ResourceIdImpl.generate());
            ResourceImpl r1 = new ResourceImpl(ResourceIdImpl.generate());
            ResourceImpl r2 = new ResourceImpl(ResourceIdImpl.generate());
            ResourceImpl r3 = new ResourceImpl(ResourceIdImpl.generate());
            List<Resource> resources =
                    Collections.unmodifiableList(
                            Arrays.asList(r0, r1, r2, r3)
                    );

            // Set up a transaction manager.
            TransactionManager tm =
                    TransactionManagerFactory.newTM(
                            resources,
                            new LocalTimeProviderImpl()
                    );

            // Set up threads operation on the resources.
            ArrayList<Thread> threads = new ArrayList<Thread>();
            Random r = new Random();
            final int N = 10;
            for (int i = 0; i < N; i++) {
                threads.add(
                        new Thread(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    Thread.sleep(r.nextInt(3) * BASE_WAIT_TIME);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
// Thread-0 starts
                                System.out.print(".");
                                try {
                                    tm.startTransaction();
                                } catch (AnotherTransactionActiveException e) {
                                    //throw new AssertionError(e);
                                }
// Thread-0 gets to R0
                                try {
                                    tm.operateOnResourceInCurrentTransaction(
                                            r0.getId(),
                                            ResourceOpImpl.get()
                                    );
                                    Thread.sleep(r.nextInt(4) * BASE_WAIT_TIME);
// Thread-0 waits on R1
                                    int interrupt = r.nextInt(10);
                                    if (interrupt < 3)
                                        Thread.currentThread().interrupt();
                                    tm.operateOnResourceInCurrentTransaction(
                                            r1.getId(),
                                            ResourceOpImpl.get()
                                    );
                                    System.out.print(".");
                                    Thread.sleep(r.nextInt(4) * BASE_WAIT_TIME);
                                    tm.operateOnResourceInCurrentTransaction(
                                            r2.getId(),
                                            ResourceOpImpl.get()
                                    );
                                    Thread.sleep(r.nextInt(4) * BASE_WAIT_TIME);
// Thread-0 waits on R1
                                    tm.operateOnResourceInCurrentTransaction(
                                            r3.getId(),
                                            ResourceOpImpl.get()
                                    );

                                    /*interrupt = r.nextInt(10);
                                    if (interrupt < 5)
                                        Thread.currentThread().interrupt();*/

                                    Thread.sleep(r.nextInt(3) * BASE_WAIT_TIME);
                                    Resource rr = resources.get(r.nextInt(3));
                                    tm.operateOnResourceInCurrentTransaction(
                                            rr.getId(),
                                            ResourceOpImpl.get()
                                    );
                                    Resource rrr = resources.get(r.nextInt(3));
                                    Thread.sleep(r.nextInt(3) * BASE_WAIT_TIME);
                                    tm.operateOnResourceInCurrentTransaction(
                                            rrr.getId(),
                                            ResourceOpImpl.get()
                                    );
                                    Thread.sleep(r.nextInt(4) * BASE_WAIT_TIME);
                                    Thread.currentThread().interrupt();
                                    interrupt = r.nextInt(10);
                                   /* if (interrupt < 3)
                                        Thread.currentThread().interrupt();*/
                                    tm.commitCurrentTransaction();
                                    if (tm.isTransactionActive()) {
                                        throw new AssertionError("Failed to commit a transaction");
                                    }
                                } catch (InterruptedException |
                                        ActiveTransactionAborted |
                                        NoActiveTransactionException |
                                        UnknownResourceIdException |
                                        ResourceOperationException e) {
                                    //throw new AssertionError(e);
                                } finally {
                                    tm.rollbackCurrentTransaction();
                                }
                            }
                        })
                );
            }
            // Start the threads and wait for them to finish.
            for (Thread t : threads) {
                t.start();
            }
            try {
                for (Thread t : threads) {
                    t.join(10 * BASE_WAIT_TIME);
                }
            } catch (InterruptedException e) {
                throw new AssertionError("The main thread has been interrupted");
            }
//         Check the results.
//        expectResourceValue(r1, 2);
//        expectResourceValue(r2, 1);
//        expectResourceValue(r3, 0);
        }
    }

    private static final void expectResourceValue(ResourceImpl r, long val) {
        if (r.getValue() != val) {
            throw new AssertionError(
                    "For resource " + r.getId() +
                            ", expected value " + val +
                            ", but got value " + r.getValue()
            );
        }
    }

    // ---------------------------------------------------------
    // -                                                       -
    // -     Sample implementations of the base interfaces     -
    // -                                                       -
    // ---------------------------------------------------------

    private static final class LocalTimeProviderImpl implements LocalTimeProvider {
        @Override
        public long getTime() {
            return System.currentTimeMillis();
        }
    }

    private static final class ResourceIdImpl implements ResourceId {
        private static volatile int next;

        public static synchronized ResourceId generate() {
            return new ResourceIdImpl(next++);
        }

        private final int value;

        private ResourceIdImpl(int value) {
            this.value = value;
        }

        @Override
        public int compareTo(ResourceId other) {
            if (!(other instanceof ResourceIdImpl)) {
                throw new RuntimeException("Comparing incompatible resource IDs");
            }
            ResourceIdImpl second = (ResourceIdImpl) other;
            return Integer.compare(this.value, second.value);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ResourceIdImpl)) {
                return false;
            }
            ResourceIdImpl second = (ResourceIdImpl) obj;
            return this.value == second.value;
        }

        @Override
        public int hashCode() {
            return Integer.hashCode(this.value);
        }

        @Override
        public String toString() {
            return "R" + this.value;
        }
    }

    private static final class ResourceImpl extends Resource {
        private volatile long value;

        public ResourceImpl(ResourceId id) {
            super(id);
        }

        public void incValue() {
            long x = this.value;
            ++x;
            this.value = x;
        }

        public void decValue() {
            long x = this.value;
            --x;
            this.value = x;
        }

        public long getValue() {
            return this.value;
        }
    }

    private static final class ResourceOpImpl extends ResourceOperation {
        private static final ResourceOpImpl singleton = new ResourceOpImpl();

        public static ResourceOperation get() {
            return singleton;
        }

        private ResourceOpImpl() {
        }

        @Override
        public String toString() {
            return "OP_" + super.toString();
        }

        @Override
        public void execute(Resource r) {
            if (!(r instanceof ResourceImpl)) {
                throw new AssertionError("Unexpected resource type " +
                        r.getClass().getCanonicalName());
            }
            ((ResourceImpl) r).incValue();
        }

        @Override
        public void undo(Resource r) {
            if (!(r instanceof ResourceImpl)) {
                throw new AssertionError("Unexpected resource type " +
                        r.getClass().getCanonicalName());
            }
            ((ResourceImpl) r).decValue();
        }
    }
}
