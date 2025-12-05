CS471 Final Project Producer-Consumer Problem

Files:
---------------
1. ProducerConsumer.java   -> Source code
2. sample_input.txt        -> Sanmple input file to generate the 9 runs
3. report.txt              -> Output file where statistics about each run are listed

Description:
-------------------
This is a Java program that runs the Producer-Consumer problem by utilizing threads, semaphores, and buffers.

Requirements to run:
-------------
- Java 17 or newer

How to compile and run:
---------------
Open a terminal in the PRODUCER_CONSUMER directory and run:

    javac ProducerConsumer.java

This will generate the executable `ProducerConsumer.class`.

Make sure `sample_input.txt` is in the same directory as `ProducerConsumer.class`.
Run the program with:

    'java ProducerConsumer'

The program will:
- Read the list of runs from `sample_input.txt`
- Execute each run
- Print consumer local summaries and global summary to the console and write all results to the 'report.txt'

Sample Input File Format (`sample_input.txt`):
-----------------------------------------------
Each line specifies a simulation run (total of 9) with the following fields:

    <num_producers> <num_consumers> <buffer_size> <seed>

Sample input file contents:  
    2 2 50 12345  
    2 5 50 12345  
    2 10 50 12345  
    5 2 50 12345  
    5 5 50 12345  
    5 10 50 12345   
    10 2 50 12345  
    10 5 50 12345  
    10 10 50 12345  

Output file contents:
------------------------
1. Run number and configuration
2. Local consumer summaries
3. Global summary
Notes:
------
- The seed is used for reproducibility of random numbers (it can be any number).
- The program automatically stops each run after a total of 1000 items.

