import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class ProducerConsumer {
    static final int TOTAL_TO_PRODUCE = 1000;
    static final int YEAR = 16;

    private final SalesRecord[] buffer;
    private final int bufferSize;
    private final Semaphore empty;
    private final Semaphore full;
    private final Object mutex = new Object();
    private int in = 0;
    private int out = 0;

    private final AtomicInteger totalProduced = new AtomicInteger(0);
    private final AtomicInteger producersRunning = new AtomicInteger(0);
    private volatile boolean productionDone = false;

    private final Object globalLock = new Object();
    private final double[] globalStoreSales;
    private final double[] globalMonthSales;
    private double globalAggregate = 0.0;

    public ProducerConsumer(int bufferSize, int maxStores) {
        this.bufferSize = bufferSize;
        this.buffer = new SalesRecord[bufferSize];
        this.empty = new Semaphore(bufferSize);
        this.full = new Semaphore(0);
        this.globalStoreSales = new double[maxStores + 1];
        this.globalMonthSales = new double[13];
    }

    static class SalesRecord {
        final int day;
        final int month;
        final int year;
        final int storeId;
        final int register;
        final double amount;
        SalesRecord(int d, int m, int y, int storeId, int reg, double amt) {
            this.day = d; this.month = m; this.year = y; this.storeId = storeId; this.register = reg; this.amount = amt;
        }
        public String toString() {
            return String.format("%02d/%02d/%02d | store=%d reg=%d $%.2f", day, month, year, storeId, register, amount);
        }
    }

    class Producer implements Runnable {
        private final int storeId;
        private final Random rng;
        Producer(int storeId, long seed) {
            this.storeId = storeId;
            this.rng = (seed == 0) ? new Random() : new Random(seed + storeId);
        }
        @Override
        public void run() {
            producersRunning.incrementAndGet();
            try {
                while (true) {
                    int producedSoFar = totalProduced.get();
                    if (producedSoFar >= TOTAL_TO_PRODUCE) break;

                    int dd = 1 + rng.nextInt(30);
                    int mm = 1 + rng.nextInt(12);
                    int yy = YEAR;
                    int reg = 1 + rng.nextInt(6);
                    double amt = 0.5 + rng.nextDouble() * (999.99 - 0.5);
                    SalesRecord rec = new SalesRecord(dd, mm, yy, storeId, reg, amt);

                    empty.acquire();
                    synchronized (mutex) {
                        buffer[in] = rec;
                        in = (in + 1) % bufferSize;
                    }
                    full.release();

                    int after = totalProduced.incrementAndGet();
                    if (after >= TOTAL_TO_PRODUCE) break;

                    int sleepMs = 5 + rng.nextInt(36);
                    try { Thread.sleep(sleepMs); } catch (InterruptedException ignored) {}
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                producersRunning.decrementAndGet();
            }
        }
    }

    class Consumer implements Runnable {
        private final int consumerId;
        private final double[] localStoreSales;
        private final double[] localMonthSales;
        private double localAggregate = 0.0;

        Consumer(int consumerId, int maxStores) {
            this.consumerId = consumerId;
            this.localStoreSales = new double[maxStores + 1];
            this.localMonthSales = new double[13];
        }

        @Override
        public void run() {
            try {
                while (true) {
                    if (productionDone && full.availablePermits() == 0) break;

                    boolean got = full.tryAcquire(100, TimeUnit.MILLISECONDS);
                    if (!got) {
                        if (productionDone && full.availablePermits() == 0) break;
                        else continue;
                    }

                    SalesRecord rec;
                    synchronized (mutex) {
                        rec = buffer[out];
                        buffer[out] = null;
                        out = (out + 1) % bufferSize;
                    }
                    empty.release();

                    localStoreSales[rec.storeId] += rec.amount;
                    localMonthSales[rec.month] += rec.amount;
                    localAggregate += rec.amount;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                synchronized (globalLock) {
                    for (int i = 1; i < localStoreSales.length; i++) globalStoreSales[i] += localStoreSales[i];
                    for (int m = 1; m <= 12; m++) globalMonthSales[m] += localMonthSales[m];
                    globalAggregate += localAggregate;
                }
                printLocalSummary();
            }
        }

        private void printLocalSummary() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("Consumer %02d local summary:%n", consumerId));
            sb.append(String.format("  Aggregate sales seen: $%.2f%n", localAggregate));
            sb.append("  Per-store totals (non-zero):\n");
            for (int i = 1; i < localStoreSales.length; i++) {
                if (localStoreSales[i] > 0.0) sb.append(String.format("    store %d : $%.2f%n", i, localStoreSales[i]));
            }
            sb.append("  Month totals (non-zero):\n");
            for (int m = 1; m <= 12; m++) {
                if (localMonthSales[m] > 0.0) sb.append(String.format("    %02d : $%.2f%n", m, localMonthSales[m]));
            }
            System.out.println(sb.toString());
        }
    }

    public void runSimulation(int p, int c, long seed, BufferedWriter bw) throws InterruptedException, IOException {
        long start = System.nanoTime();

        Thread[] producerThreads = new Thread[p];
        Thread[] consumerThreads = new Thread[c];

        for (int i = 0; i < c; i++) {
            consumerThreads[i] = new Thread(new Consumer(i+1, globalStoreSales.length-1));
            consumerThreads[i].start();
        }

        for (int i = 0; i < p; i++) {
            long s = (seed == 0) ? 0L : seed + i;
            producerThreads[i] = new Thread(new Producer(i+1, s));
            producerThreads[i].start();
        }

        for (Thread t : producerThreads) if (t != null) t.join();
        productionDone = true;
        for (Thread t : consumerThreads) if (t != null) t.join();

        long end = System.nanoTime();
        double elapsedMs = (end - start) / 1_000_000.0;

        printGlobalSummary(elapsedMs, p, c, bw);
    }

    private void printGlobalSummary(double elapsedMs, int p, int c, BufferedWriter bw) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("\n===== SUMMARY =====\n");
        sb.append(String.format("Total producers (p): %d, consumers (c): %d%n", p, c));
        sb.append(String.format("Total items produced (target): %d%n", TOTAL_TO_PRODUCE));
        sb.append(String.format("Total aggregate sales: $%.2f%n", globalAggregate));
        sb.append(String.format("Elapsed time (ms): %.2f%n", elapsedMs));
        sb.append("Per-store totals:\n");
        for (int i = 1; i < globalStoreSales.length; i++) {
            sb.append(String.format("  store %d : $%.2f%n", i, globalStoreSales[i]));
        }
        sb.append("Month totals:\n");
        for (int m = 1; m <= 12; m++) {
            sb.append(String.format("  %02d : $%.2f%n", m, globalMonthSales[m]));
        }
        sb.append("==========================\n\n");
        System.out.println(sb.toString());
        bw.write(sb.toString());
        bw.flush();
    }

    public static void runSimulation(int p, int c, int bufferSize, int seed, BufferedWriter bw) throws Exception {
        ProducerConsumer pc = new ProducerConsumer(bufferSize, p);
        pc.runSimulation(p, c, seed, bw);
    }

    public static void main(String[] args) throws Exception {
        String inputFile = "sample_input.txt";
        String reportFile = "report.txt";

        BufferedReader br = new BufferedReader(new FileReader(inputFile));
        BufferedWriter bw = new BufferedWriter(new FileWriter(reportFile));

        String line;
        int runNumber = 1;

        while ((line = br.readLine()) != null) {
            String[] parts = line.trim().split("\\s+");
            if (parts.length != 4) continue;

            int p = Integer.parseInt(parts[0]);
            int c = Integer.parseInt(parts[1]);
            int bufferSize = Integer.parseInt(parts[2]);
            int seed = Integer.parseInt(parts[3]);

            bw.write("===== RUN " + runNumber + " =====\n");
            bw.write("Producers: " + p + ", Consumers: " + c + ", Buffer: " + bufferSize + "\n");

            runSimulation(p, c, bufferSize, seed, bw);

            bw.write("\n\n");
            bw.flush();
            runNumber++;
        }

        br.close();
        bw.close();
        System.out.println("all runs completed");
    }
}
