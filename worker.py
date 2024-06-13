import threading
import queue
import time
import os
from enum import Enum

from dealings import extract_data_from_zip

class TypeFunction(Enum):
    EXTRACT_DATA = "extract_data_from_zip"

q = queue.Queue()
threads = []
lock = threading.Lock()
items_in_queue = set()

def worker():
    while True:
        try:
            item = q.get()
            if item is None:
                break
            if item[0] == TypeFunction.EXTRACT_DATA:
                print("Extrayendo de", item[1])
                extract_data_from_zip(item[1])
            q.task_done()
        except Exception as e:
            print(f'Error en hilo {threading.current_thread().name}: {e}')

# def start_worker_threads():
#     threading.Thread(target=worker, daemon=True).start()
#     for _ in range(os.cpu_count()):
#         t = threading.Thread(target=worker, daemon=True)
#         t.start()
#         threads.append(t)

def clear_queue():
    while not q.empty():
        q.get_nowait()
        q.task_done()
    with lock:
        items_in_queue.clear()

def add(item):
    q.put(item)


def add_batch(items):
    for item in items:
        q.put(item)

def stop_threads():
    for _ in range(len(threads)):
        q.put(None)
    for t in threads:
        t.join()

threading.Thread(target=worker, daemon=True).start()
for _ in range(os.cpu_count()):
    t = threading.Thread(target=worker, daemon=True)
    t.start()
    threads.append(t)
