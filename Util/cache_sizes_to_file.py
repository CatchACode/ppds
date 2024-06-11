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




def get_cpu_cache_sizes_unix() -> dict[str, int]:
    lscpu = subprocess.check_output(['lscpu', '-B']).decode().strip()

    cache_sizes = {'L1': -1, 'L2': -1, 'L3': -1}

    for line in lscpu.splitlines():
        if 'L1d cache:' in line:
            cache_sizes['L1'] = line.split(':')[1].strip()
        elif 'L1i cache:' in line:
            cache_sizes['L1'] = line.split(':')[1].strip()
        elif 'L2 cache:' in line:
            cache_sizes['L2'] = line.split(':')[1].strip()
        elif 'L3 cache:' in line:
            cache_sizes['L3'] = line.split(':')[1].strip()

    return cache_sizes

def get_cpu_cache_size_darwin() -> dict[str, int]:
    sysctl_commands = {'L1': 'hw.perflevel0.l1dcachesize',
                       'L2': 'hw.perflevel0.l2cachesize',
                       'L3': 'hw.perflevel0.l3cachesize'}

    cache_sizes = {'L1': -1, 'L2': -1, 'L3': -1}

    for cache, command in sysctl_commands.items():
        try:
            # Run the sysctl command
            result = subprocess.run(['sysctl', command], capture_output=True, text=True, check=True)
            # Parse the output to get the cache size
            output = result.stdout.split(':')[1].strip()
            cache_sizes[cache] = int(output)
        except (subprocess.CalledProcessError, IndexError, ValueError) as e:
            print(f"An error occurred while retrieving {cache} cache size: {e}")
            cache_sizes[cache] = -1  # Set to -1 if there's an error

    return cache_sizes


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


def write_sizes_to_file(cache_sizes: dict[str, int]) -> None:
    if os.path.exists('./include/cache_sizes.h'):
        os.remove('./include/cache_sizes.h')

    data = f"#pragma once\n"
    data += f"constexpr const size_t L1_CACHE_SIZE={cache_sizes.get('L1', -1)};\n"
    data += f"constexpr const size_t L2_CACHE_SIZE={cache_sizes.get('L2', -1)};\n"
    data += f"constexpr const size_t L3_CACHE_SIZE={cache_sizes.get('L3', -1)};\n"

    with open('./include/cache_sizes.h', 'w') as file:
        file.write(data)




if __name__ == "__main__":
    os_name = platform.system()
    cache_sizes = {'L1': -1, 'L2': -1, 'L3': -1}
    if os_name == "Darwin":
        cache_sizes = get_cpu_cache_size_darwin()
    elif os_name == "Unix":
        cache_sizes = get_cpu_cache_sizes_unix()
    elif os_name == "Windows":
        print("Not implemented!")
    else:
        print("Unknown OS!")
    write_sizes_to_file(cache_sizes)



