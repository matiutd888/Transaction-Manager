package cp1.tests;

import cp1.base.*;
import cp1.solution.TransactionManagerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

// Thread-0 starts
// Thread-0 gets to R0
// Thread-1 starts
// Thread-1 gets to R1
// Thread-2 starts
// Thread-2 gets to R2
// Thread-0 waits on R1
// Thread-1 waits on R2
// Thread-2 waits on R0

// Deadlock :////

public class TestSimpleDeadlock {

    private final static long BASE_WAIT_TIME = 500;

    public static void main(String[] args) {
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
        threads.add(
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Thread.sleep((long) (1 * BASE_WAIT_TIME));
                        } catch (Exception e){
                            e.printStackTrace();
                        }
// Thread-0 starts
                        System.out.println("Starting transaction 0");

                        try {
                            tm.startTransaction();
                        } catch (AnotherTransactionActiveException e) {
                            throw new AssertionError(e);
                        }
// Thread-0 gets to R0
                        try {
                            tm.operateOnResourceInCurrentTransaction(
                                    r0.getId(),
                                    ResourceOpImpl.get()
                            );
                            Thread.sleep(2 * BASE_WAIT_TIME);
// Thread-0 waits on R1
                            tm.operateOnResourceInCurrentTransaction(
                                    r1.getId(),
                                    ResourceOpImpl.get()
                            );
                            tm.commitCurrentTransaction();
                            if (tm.isTransactionActive()) {
                                throw new AssertionError("Failed to commit a transaction");
                            }
                        } catch (InterruptedException |
                                ActiveTransactionAborted |
                                NoActiveTransactionException |
                                UnknownResourceIdException |
                                ResourceOperationException e) {
                            throw new AssertionError(e);
                        } finally {
                            tm.rollbackCurrentTransaction();
                        }
                    }
                })
        );
        threads.add(
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Thread.sleep(1 * BASE_WAIT_TIME);
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
// Thread-1 starts
                        System.out.println("Starting transaction 1");
                        try {
                            tm.startTransaction();
                        } catch (AnotherTransactionActiveException e) {
                            throw new AssertionError(e);
                        }
                        if (!tm.isTransactionActive()) {
                            throw new AssertionError("Failed to start a transaction");
                        }
                        try {
// Thread-1 gets to R1
                            tm.operateOnResourceInCurrentTransaction(
                                    r1.getId(),
                                    ResourceOpImpl.get()
                            );
                            Thread.sleep(4 * BASE_WAIT_TIME);
// Thread-1 waits on R2
                            tm.operateOnResourceInCurrentTransaction(
                                    r2.getId(),
                                    ResourceOpImpl.get()
                            );
                            Thread.sleep(1 * BASE_WAIT_TIME);
                        } catch (InterruptedException |
                                ActiveTransactionAborted |
                                NoActiveTransactionException |
                                ResourceOperationException |
                                UnknownResourceIdException ignored) {
                        } finally {
                            try {
                                tm.commitCurrentTransaction();
                            } catch (ActiveTransactionAborted | NoActiveTransactionException e){
                            }
                            tm.rollbackCurrentTransaction();
                            if (tm.isTransactionActive()) {
                                throw new AssertionError("Failed to rollback a transaction");
                            }
                        }
                    }
                })
        );
        threads.add(
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Thread.sleep(1 * BASE_WAIT_TIME);
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
// Thread-2 starts
                        System.out.println("Starting transaction 2");

                        try {
                            tm.startTransaction();
                        } catch (AnotherTransactionActiveException e) {
                            throw new AssertionError(e);
                        }
                        if (!tm.isTransactionActive()) {
                            throw new AssertionError("Failed to start a transaction");
                        }
                        try {
// Thread-2 gets to R2
                            tm.operateOnResourceInCurrentTransaction(
                                    r2.getId(),
                                    ResourceOpImpl.get()
                            );
                            Thread.sleep(7 * BASE_WAIT_TIME);
// Thread-2 waits on R0
                            tm.operateOnResourceInCurrentTransaction(
                                    r0.getId(),
                                    ResourceOpImpl.get()
                            );
                            Thread.sleep(1 * BASE_WAIT_TIME);
                        } catch (InterruptedException |
                                ActiveTransactionAborted |
                                NoActiveTransactionException |
                                ResourceOperationException |
                                UnknownResourceIdException ignored) {
                        } finally {
                            try {
                                tm.commitCurrentTransaction();
                            } catch (ActiveTransactionAborted | NoActiveTransactionException e){
                            }
                            tm.rollbackCurrentTransaction();
                            if (tm.isTransactionActive()) {
                                throw new AssertionError("Failed to rollback a transaction");
                            }
                        }
                    }
                })
        );
//        threads.add(
//                new Thread(new Runnable() {
//                    @Override
//                    public void run() {
//                        try {
//                            Thread.sleep(2 * BASE_WAIT_TIME);
//                        } catch (InterruptedException e) {
//                            throw new AssertionError(e);
//                        }
//                        try {
//                            tm.startTransaction();
//                        } catch (AnotherTransactionActiveException e) {
//                            throw new AssertionError(e);
//                        }
//                        if (!tm.isTransactionActive()) {
//                            throw new AssertionError("Failed to start a transaction");
//                        }
//                        if (tm.isTransactionAborted()) {
//                            throw new AssertionError("Invalid transaction state");
//                        }
//                        try {
//                            tm.operateOnResourceInCurrentTransaction(
//                                    r2.getId(),
//                                    ResourceOpImpl.get()
//                            );
//                            Thread.sleep(1 * BASE_WAIT_TIME);
//                            tm.operateOnResourceInCurrentTransaction(
//                                    r1.getId(),
//                                    ResourceOpImpl.get()
//                            );
//                            tm.commitCurrentTransaction();
//                            if (tm.isTransactionActive()) {
//                                throw new AssertionError("Failed to commit a transaction");
//                            }
//                        } catch (InterruptedException |
//                                ActiveTransactionAborted |
//                                NoActiveTransactionException |
//                                ResourceOperationException |
//                                UnknownResourceIdException e) {
//                            throw new AssertionError(e);
//                        } finally {
//                            tm.rollbackCurrentTransaction();
//                        }
//                    }
//                })
//        );
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

    private final static void expectResourceValue(ResourceImpl r, long val) {
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
        private static volatile int next = 0;

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
        private volatile long value = 0;

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
        private final static ResourceOpImpl singleton = new ResourceOpImpl();

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