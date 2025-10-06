import time
import psutil
import os
import main

def benchmark():
    start_time = time.time()
    process = psutil.Process(os.getpid())

    # Memoria antes de ejecutar
    mem_before = process.memory_info().rss / (1024 * 1024)  # en MB
    cpu_before = process.cpu_times()

    # Ejecutar la función principal
    main.main()

    # Memoria y CPU después
    mem_after = process.memory_info().rss / (1024 * 1024)
    cpu_after = process.cpu_times()
    end_time = time.time()

    # Calcular métricas
    exec_time = end_time - start_time
    cpu_user = cpu_after.user - cpu_before.user
    cpu_system = cpu_after.system - cpu_before.system

    print("\n==== 📊 Benchmark Results ====")
    print(f"Execution time: {exec_time:.2f} seconds")
    print(f"Memory before: {mem_before:.2f} MB")
    print(f"Memory after: {mem_after:.2f} MB")
    print(f"Memory difference: {mem_after - mem_before:.2f} MB")
    print(f"User CPU time: {cpu_user:.2f} seconds")
    print(f"System CPU time: {cpu_system:.2f} seconds")
    print(f"Total CPU time: {cpu_user + cpu_system:.2f} seconds")

if __name__ == "__main__":
    benchmark()
