#!/usr/bin/env python3
import subprocess
import re
import argparse
import os
import platform
import sys


def get_phyiscal_core_count() -> int:
    if platform.system() == 'Linux':
        lscpu = subprocess.check_output(['lscpu']).decode().strip()
        for line in lscpu.split('\n'):
            if "Core(s) per socket:" in line:
                return int(line.split(":")[1].strip())
        return -1
    elif platform.system() == 'Darwin':
        line = subprocess.check_output(['sysctl', 'hw.perflevel0.physicalcpu']).decode().strip()
        return int(line.split(":")[1].strip())



def get_cpu_cache_size_linux(level: int) -> int:
    try:
        # Get number of processors
        num_cores = get_phyiscal_core_count()
        if num_cores is None:
            raise RuntimeError('No available CPU cores.')

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
        if level != 3:
            total_cache_size = total_cache_size / num_cores;

        return total_cache_size

    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running lscpu: {e}")
        return None
    except Exception as e:
        print(-1)


def get_cpu_cache_size_windows(level: int) -> int:
    return None


def get_cpu_cache_size_darwin(level: int) -> int:
    try:
        performance_cores = int(
            subprocess.check_output(["sysctl", "-n", "hw.perflevel0.physicalcpu"], text=True).strip())
        efficiency_cores = int(
            subprocess.check_output(["sysctl", "-n", "hw.perflevel1.physicalcpu"], text=True).strip())

        level_map = {
            '1': ['hw.perflevel0.l1dcachesize', 'hw.perflevel0.l1icachesize'],
            '2': ['hw.perflevel0.l2cachesize'],
            '3': ['hw.perflevel0.l3cachesize']
        }

        if level not in level_map:
            print(f"Invalid level: {level}. Valid levels are 1, 2, 3.")
            return None

        cache_sizes = []

        for cache_key in level_map[level]:
            result = subprocess.run(['sysctl', cache_key], stdout=subprocess.PIPE, text=True, check=True)
            output = result.stdout
            size = int(output.split(":")[1].strip())
            cache_sizes.append(size)

        total_cache_size = sum(cache_sizes)
        if level != 3:
            total_cache_size = total_cache_size / performance_cores

        return total_cache_size

    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running sysctl: {e}")
        return None
    except Exception as e:
        print(-1)


def get_processor_name():
    if platform.system() == "Windows":
        return platform.processor()
    elif platform.system() == "Darwin":
        os.environ['PATH'] = os.environ['PATH'] + os.pathsep + '/usr/sbin'
        command =["sysctl", "-n", "machdep.cpu.brand_string"]
        return subprocess.check_output(command).strip().decode()
    elif platform.system() == "Linux":
        command = ["lscpu"]
        all_info = subprocess.check_output(command).decode().strip()
        for line in all_info.split("\n"):
            if "Model name" in line:
                return line.split(":")[1].strip()

    return ""




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Get CPU cache size per instance for a specified level.')
    parser.add_argument('level', type=str, help='The cache level to retrieve (1, 2, or 3)')
    #print("Running script to get CPU cache size per instance for a specified level.")

    args = parser.parse_args()
    level = args.level
    if level == "4":
        print(get_processor_name())
        exit(0)
    if platform.system() == 'Linux':
        cache_size = get_cpu_cache_size_linux(level)
    elif platform.system() == 'Darwin':
        cache_size = get_cpu_cache_size_darwin(level)



    if cache_size is not None:
        print(int(cache_size))
    else:
        exit(-1)
