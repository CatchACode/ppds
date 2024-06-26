#! /usr/bin/env python3
from abc import ABC, abstractmethod

import pandas as pd
import numpy as np
import re
import random

class DataGenerator(ABC):
	"""docstring for DataGenerator"""
	def __init__(self, minValue, maxValue, keyFieldName, idFieldName, titleLength):
		self.minValue = minValue
		self.maxValue = maxValue
		self.keyFieldName = keyFieldName
		self.idFieldName = idFieldName
		self.shortMovieTitles = list()
		titleFilePath = "movieTitles/movieListLonger.txt"
		if titleLength == 'medium':
			titleFilePath = "movieTitles/twentyMediumLongMovies.txt"
		elif titleLength == 'long':
			titleFilePath = "movieTitles/twentyLongMovies.txt"
		elif titleLength == 'longUnique':
			titleFilePath = "movieTitles/longMoviesUniqueFirstCharacter.txt"
		with open(titleFilePath, 'r') as file:
			self.shortMovieTitles.append(file.read().splitlines())

	def generateDataKeyField(self, numberOfRecords):
		return np.arange(1, numberOfRecords + 1)


	def checkForCorrectness(self, dataFrame):
		return (dataFrame[self.keyFieldName] == np.arange(1, len(dataFrame[self.keyFieldName]) + 1)).all()

	def generateUniformRandomData(self, numberOfRecords, dataType):
		if dataType == "INTEGER":
			return np.random.randint(self.minValue, self.maxValue, numberOfRecords)
		elif dataType == "FLOAT":
			return np.random.uniform(self.minValue, self.maxValue, numberOfRecords)
		elif match := re.match(r"VARCHAR\((\d+)\)", dataType):
			return [''.join(random.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(int(match.group(1)))) for _ in range(numberOfRecords)]
		else:
			raise NotImplementedError(f"We do not expect the field to be of another data type then INTEGER, FLOAT, or VARCHAR! But it was {dataType}.")

	def generateTitleStrings(self, numberOfRecords):
		# random.seed(42)
		# return random.sample(self.shortMovieTitles[0], k=numberOfRecords) # <-- unique
		# return random.choices(self.shortMovieTitles[0], k=numberOfRecords)
		# start = 4611686018427387904
		start = 100000000000000000000000000000000000000000000000000000000000
		# start = 10000000000000000000000000000000000000000000000000
		count = numberOfRecords
		numbers = np.arange(start, start + count)
		return numbers
	def generateCastTitleStrings(self, numberOfRecords):
		# start = 4611686018427387904 # + 1000000
		start = 100000000000000000000000000000000000000000000000000000000000
		# start = 10000000000000000000000000000000000000000000000000
		count = numberOfRecords
		numbers = np.arange(start, start + count)
		return numbers
		# random.seed(1337)
		# return random.choices(self.shortMovieTitles[0], k=numberOfRecords)

	def generateData(self, outputFile, numberOfRecords, schema):
		"""Generates data and then writes this data to the outputFile"""
		data = pd.DataFrame()
		for [fieldName, fieldDataType] in schema:
			if fieldName == self.keyFieldName:
				data[fieldName] = self.generateDataKeyField(numberOfRecords)
			elif fieldName == self.idFieldName:
				data[fieldName] = np.arange(1, numberOfRecords + 1)
			elif fieldName == "title":
				data[fieldName] = self.generateTitleStrings(numberOfRecords)
			elif fieldName == "last_title":
				data[fieldName] = self.generateCastTitleStrings(numberOfRecords)
			else:
				data[fieldName] = self.generateUniformRandomData(numberOfRecords, fieldDataType)


			if fieldDataType == "INTEGER":
				data.astype({fieldName :'int32'})
			elif fieldDataType == "FLOAT":
				data.astype({fieldName : 'float32'})
			elif fieldDataType.startswith("VARCHAR"):
				data.astype({fieldName : 'str'})
			else:
				raise NotImplementedError(f"We do not expect the field to be of another data type then INTEGER, FLOAT, or VARCHAR! But it was {fieldDataType}")

		data.to_csv(outputFile, index=False, header=True)
		# Checking if the data has been created correctly
		csvData = pd.read_csv(outputFile)
		return self.checkForCorrectness(csvData)
