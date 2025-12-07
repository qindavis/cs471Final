CS471 Final Project CPU Scheduling Problem  

Files & Directory Structure  
-----------------------------  
CPUSCHED/  
│  
├── README.md  
├── src/  
│   └── CPUScheduler.java        <- Source code  
│  
├── executable/  
│   └── src/  
│       ├── CPUScheduler.class   <- Compiled executable  
│       └── CPUScheduler$Proc.class  
│  
├── inputs/  
│   └── inputSample.txt          <- Sample process input file  
│  
└── outputs/  
    ├── output_FIFO.txt          <- FIFO run output  
    └── output_SJF.txt           <- SJF run output  
  
Description  
------------------  
This Java program simulates two CPU scheduling algorithms:  
 - FIFO (First-In-First-Out)  
 - SJF (Shortest Job First, non-preemptive)  

The scheduler reads a list of processes from an input file (arrival time and burst time), executes the selected algorithm, and outputs:  
 - Scheduling order  
 - Start/finish times  
 - Waiting, response, and turnaround times  
 - Throughput  
 - CPU utilization  
 - Output is both printed to the console and written to a file inside the outputs/ directory.  

Requirements  
------------------------------   
 - Java 17 or newer  

How to Compile  
-----------------  
Open a terminal in the CPUSCHED/src directory.  
Run:  

    javac CPUScheduler.java -d ../executable  


This will generate:  

`executable/src/CPUScheduler.class`  
`executable/src/CPUScheduler$Proc.class`  

These .class files are your executable program.  

How to Run  
------------------------  
Change directory into the executable folder:  
`cd ../executable`  
Run the program using:  

FIFO Mode  

    java src.CPUScheduler ../inputs/datafile-txt.txt FIFO  

SJF Mode  

    java src.CPUScheduler ../inputs/datafile-txt.txt SJF  
  
  
The program will:  
 - Read process data from inputs/inputSample.txt  
 - Run the selected scheduling algorithm  
 - Print the results to the console  
 - Write the results into:  
    - outputs/output_FIFO.txt  
    - outputs/output_SJF.txt  
    (depending on the selected mode)  

Sample Input Format (inputs/datafile-txt.txt)  
----------------------------------------------  
Each line contains:  

    <arrival_time> <burst_time>  
    
Example:  
  
    0 5  
    2 3  
    4 1  
    7 4  
  
Output File Contents    
---------------------------------  
Each output file includes:  
 - Process table (PID, arrival, burst, start, finish)  
 - Total burst time  
 - Total elapsed time  
 - Throughput  
 - CPU utilization  
 - Average waiting time  
 - Average turnaround time  
 - Average response time  

Notes
--------------------------------------
 - The program automatically sorts processes by arrival time.
 - SJF mode breaks ties with arrival time, then PID.
 - FIFO mode schedules strictly by arrival order.