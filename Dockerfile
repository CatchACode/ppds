# Use the official Ubuntu base image
FROM ubuntu:20.04

# Set environment variables to avoid prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Update the package list and install necessary dependencies
RUN apt-get update && \
    apt-get install -y software-properties-common wget gnupg build-essential gdb git perf valgrind

# Add the repository for GCC 11
RUN add-apt-repository ppa:ubuntu-toolchain-r/test -y && \
    apt-get update && \
    apt-get install -y gcc-11 g++-11

# Set GCC 11 as the default compiler
RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 100 && \
    update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-11 100

# Install CMake 3.21
RUN wget https://github.com/Kitware/CMake/releases/download/v3.21.0/cmake-3.21.0-linux-x86_64.sh && \
    chmod +x cmake-3.21.0-linux-x86_64.sh && \
    ./cmake-3.21.0-linux-x86_64.sh --skip-license --prefix=/usr/local && \
    rm cmake-3.21.0-linux-x86_64.sh

# Install additional dependencies
RUN apt-get install -y libssl-dev

# Verify the installation
RUN gcc --version && g++ --version && cmake --version && gdb --version && git --version

# Set the working directory
WORKDIR /workspace

# Set the entrypoint to bash
ENTRYPOINT ["/bin/bash"]