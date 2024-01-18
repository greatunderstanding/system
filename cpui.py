import multiprocessing

def cpu_intensive_task():
    def fib(n):
        if n <= 1:
            return n
        else:
            return fib(n-1) + fib(n-2)

    # The higher the value, the more CPU-intensive the task.
    # Be cautious with very high numbers as it can significantly load the CPU.
    while True:
        fib(35)

if __name__ == "__main__":
    # Number of processes equal to the number of CPU cores
    num_processes = multiprocessing.cpu_count()

    # Create and start the processes
    processes = []
    for _ in range(num_processes):
        p = multiprocessing.Process(target=cpu_intensive_task)
        p.start()
        processes.append(p)

    # Optionally, join the processes
    for p in processes:
        p.join()
