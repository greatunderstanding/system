import numpy as np

def memory_intensive_task():
    # Allocating a large array of floats
    large_array = np.zeros((200000, 30000), dtype='float64')
    while True:
        large_array += 1

memory_intensive_task()
