#! /usr/bin/env bash
# This bash script purely executes each command one after the other so that all data necessary data files are being created

# Heavily inspired by https://stackoverflow.com/a/42098494
max_children=16
function parallel {
  local time1=$(date +"%H:%M:%S")
  local time2=""

  # for the sake of the example, I'm using $2 as a description, you may be interested in other description
  echo "starting $2 ($time1)..."
  "$@" && time2=$(date +"%H:%M:%S") && echo "finishing $2 ($time1 -- $time2)..." &

  local my_pid=$$
  local children=$(ps -eo ppid | grep -w $my_pid | wc -w)
  children=$((children-1))
  if [[ $children -ge $max_children ]]; then
    wait -n
  fi
}

one_gebi_byte=1073741824
one_hundret_mebi_byte=104857600
one_mebi_byte=1048576
one_kibi_byte=1024;
half_mebi_byte=524288;
#default_output_size=12400;
#default_output_size=1
default_output_size=$one_kibi_byte
#default_output_size=$half_mebi_byte
#default_output_size=$one_mebi_byte
#default_output_size=$one_gebi_byte
#default_output_size=$one_hundret_mebi_byte
fldr_name="data"

[ ! -d "$fldr_name" ] && mkdir -p "$fldr_name"
echo "Deleting all data in $fldr_name..."
rm -rf $fldr_name/*_matching.csv

echo "Creating the data files now..."
cd generator_code
# We require only one file for the title table, as we purely change cast_info
parallel python3 -m main --generator_type=Title --key_field_name="id" --output_file_size="$default_output_size" --output_file="../$fldr_name"/"title_info_matching.csv" --num_records=20000

# Files for Throughput over Threads figure
parallel python3 -m main --generator_type=MatchRate --output_file_size="$default_output_size" --output_file="../$fldr_name"/"cast_info_matching.csv" --match_rate=1.0 --max_value=20000 --num_records=20000
cd ..

wait