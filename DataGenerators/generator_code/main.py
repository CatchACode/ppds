#! /usr/bin/env python3


from util import *
from generators.ZipfianCastInfoGenerator import ZipfianCastInfoGenerator
from generators.UniformCastInfoGenerator import UniformCastInfoGenerator
from generators.MatchRateCastInfoGenerator import MatchRateCastInfoGenerator
from generators.TitleGenerator import TitleGenerator


import argparse
import numpy as np

DEFAULT_OUTPUT_FILE_SIZE = 100 * 1024 * 1024 # 100 MiB
CAST_INFO = {"schema": [("id", "INTEGER"), 
						("person_id", "INTEGER"), 
						("movie_id", "INTEGER"), 
						("person_role_id", "INTEGER"), 
						("note", "VARCHAR(100)"), 
						("nr_order", "INTEGER"), 
						("role_id", "INTEGER")],
			 "size" : 124}
TITLE = {"schema": [("id" , "INTEGER"),
					("title", "VARCHAR(200)"),
					("imdb_index", "VARCHAR(12)"),
					("kind_id", "INTEGER"),
					("production_year", "INTEGER"),
					("imdb_id", "INTEGER"),
					("phonetic_code", "VARCHAR(5)"),
					("episode_of_id", "INTEGER"),
					("season_nr", "INTEGER"),
					("episode_nr", "INTEGER"),
					("series_years", "VARCHAR(49)"),
					("md5sum", "VARCHAR(32)")],
			 "size" : 322}

def main():
	parser = argparse.ArgumentParser(description='Generate data using different types of data generators.')
	parser.add_argument('--generator_type', required=True, choices=['Uniform', 'Zipfian', 'MatchRate', 'Sorted', 'Title'], help='Type of data generator for the cast_info table')
	parser.add_argument('--min_value', required=False, type=int, default=1, help='Minimum value (included)')
	parser.add_argument('--max_value', required=False, type=int, default=1001, help='Maximum value (excluded)')
	parser.add_argument('--key_field_name', required=False, type=str, default="movie_id", help='Name of the key field of cast_info')
	parser.add_argument('--id_field_name', required=False, type=str, default="id", help='Name of the id field of both tables')
	parser.add_argument('--match_rate', required=False, type=float, default=100, help='Match rate, so the percentage of keys that will find/match a key')
	parser.add_argument('--output_file_size', required=True, type=int, default=DEFAULT_OUTPUT_FILE_SIZE, help='Filesize of the output file in Byte')
	parser.add_argument('--output_file', required=True, help='Output file for generated data')


	print("\n\n")
	args = parser.parse_args()

	if args.generator_type == 'Uniform':
		generator = UniformCastInfoGenerator(args.min_value, args.max_value, args.key_field_name, args.id_field_name)
		numberOfRecords = calcNumberOfRecords(args.output_file_size, CAST_INFO["size"])
		schema = CAST_INFO["schema"]
	elif args.generator_type == 'Zipfian':
		generator = ZipfianCastInfoGenerator(args.min_value, args.max_value, args.key_field_name, args.id_field_name)
		numberOfRecords = calcNumberOfRecords(args.output_file_size, CAST_INFO["size"])
		schema = CAST_INFO["schema"]
	elif args.generator_type == 'Title':
		generator = TitleGenerator(args.min_value, args.max_value, args.key_field_name, args.id_field_name)
		numberOfRecords = calcNumberOfRecords(args.output_file_size, TITLE["size"])
		schema = TITLE["schema"]
	else:
		printFail(f"Invalid generator type {args.generator_type}")
		exit(-1)

	
	try:
		printHighlight(f"Generating data for {args.generator_type} and saving to {args.output_file}...")
		if generator.generateData(args.output_file, numberOfRecords, schema):
			printSuccess(f"Data generated correctly using {args.generator_type} and saved to {args.output_file}")
		else:
			raise Exception("Data was not created correctly!")
	except Exception as e:
		printFail(f"Could not generate data for {args.generator_type} and output file {args.output_file}")
		raise e





if __name__ == "__main__":
	main()
