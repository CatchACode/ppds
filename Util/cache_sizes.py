import subprocess
import re

def get_cpu_cache_sizes():
    try:
        # Run the lscpu command
        result = subprocess.run(['lscpu', '-B'], stdout=subprocess.PIPE, text=True, check=True)
        output = result.stdout

        # Regular expression to find cache sizes
        cache_sizes = {}
        cache_pattern = re.compile(r"^\s*(L\d+[id]?|L\d+)\s*:\s*(\d+)\s*\((\d+)\s*instances\)$", re.MULTILINE)

        for match in cache_pattern.finditer(output):
            cache_type = match.group(1)
            cache_size = int(match.group(2))
            instances = int(match.group(3))
            total_size = cache_size * instances
            cache_sizes[cache_type] = f"{total_size} bytes"

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
