package src;

import java.io.*;
import java.util.*;
import java.util.function.BiConsumer;

/**
 * CPU Scheduler Simulator for FIFO and SJF (non-preemptive)
 */
public class CPUScheduler {

    static class Proc {
        int id;
        long arrival;
        long burst;
        long start;
        long finish;

        Proc(int id, long arrival, long burst) {
            this.id = id;
            this.arrival = arrival;
            this.burst = burst;
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.out.println("Usage: java CPUScheduler <inputfile> <FIFO|SJF>");
            return;
        }

        String inputFile = args[0];
        String mode = args[1].toUpperCase();

        if (!mode.equals("FIFO") && !mode.equals("SJF")) {
            System.out.println("ERROR: mode must be FIFO or SJF");
            return;
        }

        // Output file name based on mode
        String outputFile = "../outputs/output_" + mode + ".txt";
        PrintWriter out = new PrintWriter(new FileWriter(outputFile));

        List<Proc> procs = new ArrayList<>();

        // Read input file
        BufferedReader br = new BufferedReader(new FileReader(inputFile));
        String line;
        int pid = 1;
        long totalBurst = 0;

        while ((line = br.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) continue;

            String[] parts = line.split("\\s+");
            long arrival = Long.parseLong(parts[0]);
            long burst = Long.parseLong(parts[1]);
            procs.add(new Proc(pid++, arrival, burst));
            totalBurst += burst;
        }
        br.close();

        // Sort processes by arrival time
        procs.sort(Comparator.comparingLong(p -> p.arrival));

        LinkedList<Proc> ready = new LinkedList<>();
        List<Proc> completed = new ArrayList<>();

        long time = 0;
        int i = 0;
        int n = procs.size();

        // MAIN SIMULATION LOOP
        while (completed.size() < n) {

            while (i < n && procs.get(i).arrival <= time) {
                ready.add(procs.get(i));
                i++;
            }

            if (ready.isEmpty()) {
                if (i < n) {
                    time = procs.get(i).arrival;
                    continue;
                } else break;
            }

            Proc p;

            if (mode.equals("FIFO")) {
                ready.sort(Comparator
                        .comparingLong((Proc x)->x.arrival)
                        .thenComparingInt(x->x.id));
                p = ready.removeFirst();
            } else {
                ready.sort(Comparator
                        .comparingLong((Proc x)->x.burst)
                        .thenComparingLong(x->x.arrival)
                        .thenComparingInt(x->x.id));
                p = ready.removeFirst();
            }

            p.start = time;
            p.finish = time + p.burst;
            time = p.finish;
            completed.add(p);
        }

        // ----- COMPUTE STATISTICS -----
        long firstArrival = procs.get(0).arrival;
        long lastFinish = completed.get(completed.size() - 1).finish;
        long totalElapsed = lastFinish - firstArrival;

        double cpuUtil = totalElapsed == 0 ? 0 : (double) totalBurst / totalElapsed;
        double throughput = totalElapsed == 0 ? 0 : (double) completed.size() / totalElapsed;

        double totalWait = 0, totalTurn = 0, totalResp = 0;

        for (Proc p : completed) {
            long wait = p.start - p.arrival;
            long turn = p.finish - p.arrival;
            long resp = p.start - p.arrival;

            totalWait += wait;
            totalTurn += turn;
            totalResp += resp;
        }

        double avgWait = totalWait / completed.size();
        double avgTurn = totalTurn / completed.size();
        double avgResp = totalResp / completed.size();

        // Helper to print to both console and file
        PrintStream console = System.out;
        BiConsumer<String, Object[]> printBoth = (fmt, args2) -> {
            String s = String.format(fmt, args2);
            console.print(s);
            out.print(s);
        };

        // ----- PRINT OUTPUT -----
        printBoth.accept("Scheduling mode: %s\n", new Object[]{mode});
        printBoth.accept("Number of processes: %d\n\n", new Object[]{completed.size()});

        printBoth.accept("%5s %8s %8s %8s %8s\n",
                new Object[]{"PID", "ARR", "BURST", "START", "FINISH"});

        for (Proc p : completed) {
            printBoth.accept("%5d %8d %8d %8d %8d\n",
                    new Object[]{p.id, p.arrival, p.burst, p.start, p.finish});
        }

        printBoth.accept("\nStatistics for the Run\n", new Object[]{});
        printBoth.accept("----------------------\n", new Object[]{});
        printBoth.accept("Total burst time: %d\n", new Object[]{totalBurst});
        printBoth.accept("Total elapsed time: %d\n", new Object[]{totalElapsed});
        printBoth.accept("Throughput: %.6f\n", new Object[]{throughput});
        printBoth.accept("CPU Utilization: %.6f\n", new Object[]{cpuUtil});
        printBoth.accept("Average waiting time: %.6f\n", new Object[]{avgWait});
        printBoth.accept("Average turnaround time: %.6f\n", new Object[]{avgTurn});
        printBoth.accept("Average response time: %.6f\n", new Object[]{avgResp});

        out.close();
        System.out.println("\nResults also written to: " + outputFile);
    }
}
