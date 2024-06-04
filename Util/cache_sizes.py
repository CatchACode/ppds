#!/usr/bin/env python3
import subprocess
import re
import argparse


def get_cpu_cache_size(level: int) -> int:
    try:
        # Run the lscpu command
        result = subprocess.run(['lscpu', '-B'], stdout=subprocess.PIPE, text=True, check=True)
        output = result.stdout

        # Map level to cache type string
        level_map = {
            '1': ['L1d', 'L1i'],
            '2': ['L2'],
            '3': ['L3']
        }

        if level not in level_map:
            print(f"Invalid level: {level}. Valid levels are 1, 2, 3.")
            return None

        # Initialize cache sizes dictionary
        cache_sizes = []

        # Split the output into lines and parse each line for cache sizes
        lines = output.splitlines()
        for line in lines:
            for cache_type in level_map[level]:
                if cache_type in line:
                    parts = line.split(":")
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

                    cache_sizes.append(size_bytes)

        # Sum up the cache sizes for the same level if there are multiple entries like L1d and L1i
        total_cache_size = sum(cache_sizes)
        return total_cache_size

    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running lscpu: {e}")
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Get CPU cache size for a specified level.')
    parser.add_argument('level', type=str, help='The cache level to retrieve (1, 2, or 3)')

    args = parser.parse_args()
    cache_size = get_cpu_cache_size(args.level)
    if cache_size is not None:
        print(cache_size)
    else:
        print("Failed to retrieve CPU cache size.")
