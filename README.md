# DistributedSystemUsingMPI
Dive deep into a 2.21 GB flight data CSV, exploring specifics on airlines, flight schedules, and more. This project uses multi-threading, multi-processing, and MPI to answer key questions and then contrasts the performance of these techniques through extensive analysis.

# MPI Workers analysis
![image](https://github.com/HaithamHany/DistributedSystemUsingMPI/assets/20623059/1629a848-660f-490b-8c0e-9cd8c88b09d9)

#Multi-threading analysis
time taken to process each chunk by each thread increases gradually, even though they are all supposed 
to be started at almost the same time.
For example, 1st thread finished after 17.34 seconds and the second thread finished at 22.6
They both started at the same time but finished with a 5.26 difference.
![image](https://github.com/HaithamHany/DistributedSystemUsingMPI/assets/20623059/8a80c8f0-a8bd-4444-b40e-f49ea44db69b)

# Multi-processing
time taken to process each chunk by each process increases gradually, with an average of only 1.29 delay 
compared to 4.176 for the threads

![image](https://github.com/HaithamHany/DistributedSystemUsingMPI/assets/20623059/579f2d06-de4f-4a8f-893c-45c102b1229b)

Even though each chunk processing time (for 4 processes) takes more time than each chunk in (10 
threads). The total handling time for 4 processes for the entire large CSV file is faster than 10 threads by 
at least 59%

# MPI - 4 workers:
Using 4 MPI workers. Workers get executed one after another with a bit of a delay between each 
execution.
time taken to process each chunk by each worker increases gradually, with an average of only 1.32 delay 
slightly higher than multiprocessing with 1.29 seconds and 4.176 for the threads

![image](https://github.com/HaithamHany/DistributedSystemUsingMPI/assets/20623059/13d3d552-138b-433c-ba4a-d7b692a90046)

The total handling time was more or less the same as of Multiprocessing 24.4 compared to 24.7 for 
multiprocessing
