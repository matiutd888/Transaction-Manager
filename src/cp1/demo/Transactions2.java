package cp1.demo;

import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.Semaphore;

import cp1.base.ActiveTransactionAborted;
import cp1.base.AnotherTransactionActiveException;
import cp1.base.LocalTimeProvider;
import cp1.base.NoActiveTransactionException;
import cp1.base.Resource;
import cp1.base.ResourceId;
import cp1.base.ResourceOperation;
import cp1.base.ResourceOperationException;
import cp1.base.TransactionManager;
import cp1.base.UnknownResourceIdException;
import cp1.solution.TransactionManagerFactory;

// autor Krzysztof Rogowski z wyłączeniem tych kawałków kodu, które są skopiowane z przykładu
// rób co chcesz z tym kodem niczego nie gwarantuję

public class Transactions2 {
    static long currentTime;
    static final boolean DEBUG_INFO = false;

    public static void main(String[] args) {
        Scanner input = new Scanner(System.in);
        if (DEBUG_INFO) {
            System.out.println("Enter resources num:");
        }
        int resourcesNum = input.nextInt();
        if (DEBUG_INFO) {
            System.out.println("Enter threads num:");
        }
        int threadsNum = input.nextInt();

        ResourceImpl[] resources = new ResourceImpl[resourcesNum];
        for (int i = 0; i < resourcesNum; ++i) {
            resources[i] = new ResourceImpl(ResourceIdImpl.generate());
        }

        TransactionManager tm = TransactionManagerFactory.newTM(Arrays.asList(resources),
                new LocalTimeProviderImpl());

        Semaphore[] locks = new Semaphore[threadsNum + 1];
        for (int i = 0; i <= threadsNum; ++i) {
            locks[i] = new Semaphore(0);
        }

        Inputter[] inputters = new Inputter[threadsNum];
        Thread[] threads = new Thread[threadsNum];
        for (int i = 0; i < threadsNum; ++i) {
            inputters[i] = new Inputter(tm, locks[i], locks[threadsNum], resources);
            threads[i] = new Thread(inputters[i]);
        }

        for (Thread t : threads) {
            t.start();
        }

        input.nextLine();

        int lineNum = 2;
        while (input.hasNextLine()) {
            String command = input.nextLine();
            ++lineNum;
            if (command.startsWith("#") || command.equals("")) {
                continue;
            }
            Scanner tokenizer = new Scanner(command);
            String tok1 = tokenizer.next().toLowerCase();
            if (tok1.equals("thread")) {
                int threadId = tokenizer.nextInt();
                inputters[threadId].pass(tokenizer);
            } else if (tok1.equals("resourcesinfo")) {
                for (ResourceImpl r : resources) {
                    System.out.print(r.getValue());
                    System.out.print(";");
                }
                System.out.println();
            } else if (tok1.equals("assertnobusy")) {
                int threadId = tokenizer.nextInt();
                if (inputters[threadId].busy) {
                    throw new AssertionError(
                            "line " + lineNum + ": thread num " + threadId + " busy");
                }
            } else if (tok1.equals("assertbusy")) {
                int threadId = tokenizer.nextInt();
                if (!inputters[threadId].busy) {
                    throw new AssertionError(
                            "line " + lineNum + ": thread num " + threadId + " not busy");
                }
            } else if (tok1.equals("assertresources")) {
                int resNum = 0;
                for (ResourceImpl r : resources) {
                    int expectedVal = tokenizer.nextInt();
                    if (r.getValue() != expectedVal) {
                        throw new AssertionError("line " + lineNum + ": resource " + resNum
                                + " value was expected to be " + expectedVal + " but is "
                                + r.getValue());
                    }
                    ++resNum;
                }
            } else if (tok1.equals("sleep")) {
                int value = tokenizer.nextInt();
                try {
                    Thread.sleep(value);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else if (tok1.equals("advancetime")) {
                ++currentTime;
            } else if (tok1.equals("timeinfo")) {
                System.out.println(currentTime);
            } else if (tok1.equals("interrupt")) {
                int threadId = tokenizer.nextInt();
                threads[threadId].interrupt();
            } else {
                System.out.println("unrecognized command");
            }
        }
        System.out.println("test passed");
        System.exit(0);
    }

    private static final class Inputter implements Runnable {
        private final TransactionManager tm;
        private final Semaphore myLock;
        private final Semaphore nextLock;
        private final boolean stopped;
        private final Resource[] resources;
        private volatile boolean busy;
        private volatile Scanner input;
        private final StringBuilder output = new StringBuilder();

        public void pass(Scanner input) {
            this.input = input;
            myLock.release();
            nextLock.acquireUninterruptibly();
        }

        public boolean isBusy() {
            return busy;
        }

        public Inputter(TransactionManager transactionManager, Semaphore myLock, Semaphore nextLock,
                        Resource[] resources) {
            tm = transactionManager;
            this.myLock = myLock;
            this.nextLock = nextLock;
            this.resources = resources;
        }

        @Override
        public void run() {
            boolean skipLock = false;
            for (;;) {
                if (!skipLock) {
                    myLock.acquireUninterruptibly();
                } else {
                    skipLock = false;
                }

                if (!stopped) {
                    if (input.hasNext()) {
                        String command = input.next().toLowerCase();
                        if (command.equals("status")) {
                            System.out.println("thread: " + Thread.currentThread().getId());
                            System.out.println("transaction status: " + (tm.isTransactionActive()
                                    ? (tm.isTransactionAborted() ? "aborted" : "active")
                                    : "inactive"));
                            nextLock.release();
                        } else if (command.equals("transaction")) {
                            try {
                                tm.startTransaction();
                                if (!tm.isTransactionActive()) {
                                    throw new AssertionError("Failed to start a transaction");
                                }
                                if (tm.isTransactionAborted()) {
                                    throw new AssertionError("Invalid transaction state");
                                }
                                System.out.println("transaction started");
                            } catch (AnotherTransactionActiveException e) {
                                System.out.println("transaction already in progress");
                            }
                            nextLock.release();
                        } else if (command.equals("rollback")) {
                            tm.rollbackCurrentTransaction();
                            nextLock.release();
                        } else if (command.equals("commit")) {
                            try {
                                tm.commitCurrentTransaction();
                                if (tm.isTransactionActive()) {
                                    throw new AssertionError("Failed to commit a transaction");
                                }
                            } catch (NoActiveTransactionException e) {
                                System.out.println("no active transaction");
                            } catch (ActiveTransactionAborted e) {
                                System.out.println("active transaction aborted");
                            }
                            nextLock.release();
                        } else if (command.equals("increase") || command.equals("decrease")
                                || command.equals("error")) {
                            int resNum = input.nextInt();
                            busy = true;
                            System.out.println("...");
                            ResourceOperation op = command.equals("increase") ? ResourceOpInc.get()
                                    : (command.equals("decrease") ? ResourceOpDec.get()
                                            : ResourceOpErr.get());
                            nextLock.release();
                            try {
                                tm.operateOnResourceInCurrentTransaction(resources[resNum].getId(),
                                        op);
                                busy = false;
                                myLock.acquireUninterruptibly();
                                System.out.println("...success");
                            } catch (NoActiveTransactionException e) {
                                busy = false;
                                myLock.acquireUninterruptibly();
                                System.out.println("...no active transaction");
                            } catch (UnknownResourceIdException e) {
                                busy = false;
                                myLock.acquireUninterruptibly();
                                System.out.println("...unknown resource id");
                            } catch (ActiveTransactionAborted e) {
                                busy = false;
                                myLock.acquireUninterruptibly();
                                System.out.println("...active transaction aborted");
                            } catch (ResourceOperationException e) {
                                busy = false;
                                myLock.acquireUninterruptibly();
                                System.out.println("...resource operation exception");
                            } catch (InterruptedException e) {
                                busy = false;
                                myLock.acquireUninterruptibly();
                                System.out.println("...thread interuppted");
                            } finally {
                                skipLock = true;
                            }
                        } else {
                            System.out.println("unrecognized command");
                            nextLock.release();
                        }
                    } else {
                        nextLock.release();
                    }
                }
            }
        }
    }

    private static final class LocalTimeProviderImpl implements LocalTimeProvider {
        @Override
        public long getTime() {
            return currentTime;
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

    private static final class ResourceOpInc extends ResourceOperation {
        private static final ResourceOpInc singleton = new ResourceOpInc();

        public static ResourceOperation get() {
            return singleton;
        }

        private ResourceOpInc() {
        }

        @Override
        public String toString() {
            return "OP_" + super.toString();
        }

        @Override
        public void execute(Resource r) {
            if (!(r instanceof ResourceImpl)) {
                throw new AssertionError(
                        "Unexpected resource type " + r.getClass().getCanonicalName());
            }
            ((ResourceImpl) r).incValue();
        }

        @Override
        public void undo(Resource r) {
            if (!(r instanceof ResourceImpl)) {
                throw new AssertionError(
                        "Unexpected resource type " + r.getClass().getCanonicalName());
            }
            ((ResourceImpl) r).decValue();
        }
    }

    private static final class ResourceOpDec extends ResourceOperation {
        private static final ResourceOpDec singleton = new ResourceOpDec();

        public static ResourceOperation get() {
            return singleton;
        }

        private ResourceOpDec() {
        }

        @Override
        public String toString() {
            return "OP_" + super.toString();
        }

        @Override
        public void execute(Resource r) {
            if (!(r instanceof ResourceImpl)) {
                throw new AssertionError(
                        "Unexpected resource type " + r.getClass().getCanonicalName());
            }
            ((ResourceImpl) r).decValue();
        }

        @Override
        public void undo(Resource r) {
            if (!(r instanceof ResourceImpl)) {
                throw new AssertionError(
                        "Unexpected resource type " + r.getClass().getCanonicalName());
            }
            ((ResourceImpl) r).incValue();
        }
    }

    private static final class ResourceOpErr extends ResourceOperation {
        private static final ResourceOpErr singleton = new ResourceOpErr();

        public static ResourceOperation get() {
            return singleton;
        }

        private ResourceOpErr() {
        }

        @Override
        public String toString() {
            return "OP_" + super.toString();
        }

        @Override
        public void execute(Resource r) throws ResourceOperationException {
            if (!(r instanceof ResourceImpl)) {
                throw new AssertionError(
                        "Unexpected resource type " + r.getClass().getCanonicalName());
            }
            throw new ResourceOperationException(r.getId(), this);
        }

        @Override
        public void undo(Resource r) {
            if (!(r instanceof ResourceImpl)) {
                throw new AssertionError(
                        "Unexpected resource type " + r.getClass().getCanonicalName());
            }
        }
    }
}
