import subprocess
import re

def get_cpu_cache_sizes():
    try:
        # Run the lscpu command
        result = subprocess.run(['lscpu'], stdout=subprocess.PIPE, text=True, check=True)
        output = result.stdout

        # Initialize cache sizes dictionary
        cache_sizes = {}

        # Split the output into lines and parse each line for cache sizes
        lines = output.splitlines()
        for line in lines:
            if "L1d" in line or "L1i" in line or "L2" in line or "L3" in line:
                parts = line.split(":")
                cache_type = parts[0].strip()
                size_info = parts[1].strip().split()
                size = int(size_info[0])
                unit = size_info[1]

                if unit == "K":
                    size_bytes = size * 1024
                elif unit == "M":
                    size_bytes = size * 1024 ** 2
                elif unit == "G":
                    size_bytes = size * 1024 ** 3
                else:
                    size_bytes = size

                cache_sizes[cache_type] = f"{size_bytes} bytes"

        return cache_sizes

    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running lscpu: {e}")
        return None

if __name__ == "__main__":
    cache_sizes = get_cpu_cache_sizes()
    if cache_sizes:
        for cache, size in cache_sizes.items():
            print(f"{cache}: {size}")
    else:
        print("Failed to retrieve CPU cache sizes.")
