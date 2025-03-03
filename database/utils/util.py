import time
import inspect
import threading
import concurrent.futures
from queue import Queue
from functools import wraps
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..datafeed import DataFeedManager


# def rate_limiter(max_calls_per_minute):
#     def decorator(func):
#         calls = 0
#         start_time = time.time()

#         @wraps(func)
#         def wrapper(*args, **kwargs):
#             nonlocal calls, start_time
#             calls += 1
#             if calls > max_calls_per_minute:
#                 elapsed_time = time.time() - start_time
#                 if elapsed_time < 10:
#                     sleep_time = 10 - elapsed_time
#                     time.sleep(sleep_time)
#                 calls = 0
#                 start_time = time.time()
#             return func(*args, **kwargs)

#         return wrapper
#     return decorator


def rate_limiter(max_calls_per_minute):
    def decorator(func):
        calls = 0
        start_time = time.time()
        lock = threading.Lock()

        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal calls, start_time
            with lock:
                current_time = time.time()
                if current_time - start_time > 60:
                    # Reset the counter and the start time every minute
                    calls = 0
                    start_time = current_time
                if calls >= max_calls_per_minute:
                    # Sleep until the minute is over
                    sleep_time = 60 - (current_time - start_time)
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                    # Reset after sleeping
                    calls = 0
                    start_time = time.time()
                calls += 1
            return func(*args, **kwargs)

        return wrapper
    return decorator


class verboser(object):

    def __init__(self, verbose=True):
        self.verbose = verbose
 
    def __call__(self, func):
        def wrapper(*args, **kwargs):
            if self.verbose:
                print(f'Loading data from {args[0]}:')
            result = func(*args, **kwargs)
            if self.verbose:
                print('Done')
            return result
        return wrapper


def worker(queue):
    while True:
        func, args, kwargs = queue.get()
        if func is None:
            break
        func(*args, **kwargs)
        queue.task_done()


def parallel_task(manager: "DataFeedManager", tasks: list):

    from ..datafeed import DataFeedManager
    
    # fetch_methods = [name for name, obj in DataFeedManager.__dict__.items() if callable(obj) and not name.startswith("_")]
    methods= inspect.getmembers(DataFeedManager, predicate=inspect.isfunction)
    fetch_methods = [method for method in methods if not method[0].startswith("_")]

    task_queue = Queue()
    num_threads = len(tasks)

    # Start worker threads
    threads = []
    for _ in range(num_threads):
        thread = threading.Thread(target=worker, args=(task_queue,))
        thread.start()
        threads.append(thread)

    # Enqueue tasks with parameters
    for param in tasks:
        # fetch_method = next((method[1] for method in fetch_methods if param[0].split("_")[-2] in method[0].split("_")), None)
        # fetch_func = fetch_method.__call__
        # if fetch_func is None:
        #     # raise ValueError(f"Error, datafeed function for {param} not found!")
        #     fetch_func = manager.fetch_volumn_price_data
        fetch_func = next((method[1].__get__(manager, DataFeedManager) for method in fetch_methods if param[0].split("_")[-2] in method[0].split("_")), manager.fetch_volumn_price_data)
        task_queue.put((fetch_func, param, {}))

    # Block until all tasks are done
    task_queue.join()

    # Stop workers
    for _ in range(num_threads):
        task_queue.put((None, (), {}))

    for thread in threads:
        thread.join()


# def rate_limiter(max_calls_per_minute):
#     lock = threading.Lock()
#     calls = 0
#     start_time = time.time()
    
#     def decorator(func):
#         @wraps(func)
#         def wrapper(*args, **kwargs):
#             nonlocal calls, start_time
#             with lock:
#                 calls += 1
#                 if calls > max_calls_per_minute:
#                     elapsed_time = time.time() - start_time
#                     if elapsed_time < 60:
#                         sleep_time = 60 - elapsed_time
#                         time.sleep(sleep_time)
#                     calls = 0
#                     start_time = time.time()
#             return func(*args, **kwargs)

#         return wrapper
#     return decorator


# def parallel_task(manager: "DataFeedManager", tasks: list):

#     from ..datafeed import DataFeedManager
    
#     methods= inspect.getmembers(DataFeedManager, predicate=inspect.isfunction)
#     fetch_methods = [method for method in methods if not method[0].startswith("_")]
    
#     num_threads = len(tasks)
#     with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
#         futures = []
#         for param in tasks:
#             worker = next((method[1].__get__(manager, DataFeedManager) for method in fetch_methods if param[0].split("_")[-2] in method[0].split("_")), manager.fetch_volumn_price_data)
#             futures.append(executor.submit(worker, *param))

#         for future in concurrent.futures.as_completed(futures):
#             future.result()
