/* Każdy wątek w każdej z REPS iteracji chce dwukrotnie inkrementować każdy zasób.
 * Main w międzyczasie strzela w wątki interruptami.
 * Sprawdzamy transakcyjność: chcemy, żeby na koniec wszystkie zasoby miały takie same wartości.
 *
 * Autor: Szymon Karpiński, na podstawie testu p. Konrada Iwanickiego
 */
package cp1.tests;

import cp1.base.*;
import cp1.solution.TransactionManagerFactory;

import java.util.*;


public class StressTest {

    private static final long BASE_WAIT_TIME = 500;
    private static final long RESOURCES = 100;
    private static final long THREADS = 20;
    private static final long REPS = 1000;
    private static final long INTERRUPTS = 10000;

    public static void main(String[] args) {
        System.err.println("Setting up the resources");
        List<ResourceImpl> RI = new ArrayList<>();
        List<Resource> R = new ArrayList<>();
        for (int i = 0; i < RESOURCES; i++) {
            ResourceImpl ri = new ResourceImpl(ResourceIdImpl.generate());
            RI.add(ri);
            R.add(ri);
        }

        System.err.println("Setting up TM");
        TransactionManager tm = TransactionManagerFactory.newTM(R, new LocalTimeProviderImpl());

        ArrayList<Thread> threads = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) threads.add(
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        int ok = 0, aborted = 0, interrupted = 0;
                        for (int i = 0; i < REPS; i++) {
                            try {
                                tm.startTransaction();
                                int offset = new Random().nextInt(R.size());
                                for (int j = 0; j < 2 * R.size(); j++) {
                                    int index = (offset + j) % R.size();
                                    tm.operateOnResourceInCurrentTransaction(R.get(index).getId(), StressTest.ResourceOpImpl.get());
                                }
                                tm.commitCurrentTransaction();
                                ok++;
                            } catch (NoActiveTransactionException | ResourceOperationException | UnknownResourceIdException | AnotherTransactionActiveException e) {
                                throw new AssertionError(e);
                            } catch (ActiveTransactionAborted e) {
                                aborted++;
                                if (!tm.isTransactionAborted() || !tm.isTransactionActive()) {
                                    throw new AssertionError(e);
                                }
                            } catch (InterruptedException e) {
                                interrupted++;
                            }
                            finally {
                                tm.rollbackCurrentTransaction();
                            }
                            try {
                                Thread.sleep(new Random().nextInt(10));
                            } catch (InterruptedException e) {
                                //throw new AssertionError(e);
                            }
                        }
                        System.err.printf("Thread %d had %d ops OK and %d aborted. Interrupted %d times.\n", Thread.currentThread().getId(), ok, aborted, interrupted);
                    }
                })
        );

        System.err.println("Starting threads");
        for (Thread t : threads) {
            t.start();
        }
        System.err.println("Hitting threads with some random interrupts");
        for (int i = 0; i < INTERRUPTS; i++) {
            for (Thread t : threads) {
                t.interrupt();
            }
        }

        System.err.println("Waiting for threads");
        try {
            for (Thread t : threads) {
                t.join();
            }
        } catch (InterruptedException e) {
            throw new AssertionError("The main thread has been interrupted");
        }

        long expected = RI.get(0).getValue();
        System.err.printf("Expecting value %d in all resources\n", expected);
        for (ResourceImpl ri : RI) {
            expectResourceValue(ri, expected);
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
    // -     Sample implementations of the cp1.base interfaces     -
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
            if (! (other instanceof ResourceIdImpl)) {
                throw new RuntimeException("Comparing incompatible resource IDs");
            }
            ResourceIdImpl second = (ResourceIdImpl)other;
            return Integer.compare(this.value, second.value);
        }
        @Override
        public boolean equals(Object obj) {
            if (! (obj instanceof ResourceIdImpl)) {
                return false;
            }
            ResourceIdImpl second = (ResourceIdImpl)obj;
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
            if (! (r instanceof ResourceImpl)) {
                throw new AssertionError("Unexpected resource type " +
                        r.getClass().getCanonicalName());
            }
            ((ResourceImpl)r).incValue();
        }
        @Override
        public void undo(Resource r) {
            if (! (r instanceof ResourceImpl)) {
                throw new AssertionError("Unexpected resource type " +
                        r.getClass().getCanonicalName());
            }
            ((ResourceImpl)r).decValue();
        }
    }
}