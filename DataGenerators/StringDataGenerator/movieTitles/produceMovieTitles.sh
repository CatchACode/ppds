#!/bin/bash

# Check if exactly three arguments are passed
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 MIN_LENGTH MAX_LENGTH NUM_LINES" >&2
    exit 1
fi

# Assign the command line arguments to variables
MIN_LENGTH=$1
MAX_LENGTH=$2
NUM_LINES=$3

# Counter for output lines
count=0

# Read from stdin line by line
while IFS= read -r line; do
    # Get the length of the current line
    length=${#line}

    # Check if the line length is within the specified bounds
    if [ "$length" -ge "$MIN_LENGTH" ] && [ "$length" -le "$MAX_LENGTH" ]; then
        echo "$line"
        # Increment the count
        ((count++))

        # Check if the required number of lines has been reached
        if [ "$count" -eq "$NUM_LINES" ]; then
            # Break the loop if the specified number of lines is reached
            break
        fi
    fi
done
