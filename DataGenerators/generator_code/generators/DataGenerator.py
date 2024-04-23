#! /usr/bin/env python3
from abc import ABC, abstractmethod

import pandas as pd
import numpy as np
import re
import random

class DataGenerator(ABC):
	"""docstring for DataGenerator"""
	def __init__(self, minValue, maxValue, keyFieldName, idFieldName):
		self.minValue = minValue
		self.maxValue = maxValue
		self.keyFieldName = keyFieldName
		self.idFieldName = idFieldName

	def generateUniformRandomData(self, numberOfRecords, dataType):
		if dataType == "INTEGER":
			return np.random.randint(self.minValue, self.maxValue, numberOfRecords)
		elif dataType == "FLOAT":
			return np.random.uniform(self.minValue, self.maxValue, numberOfRecords)
		elif match := re.match(r"VARCHAR\((\d+)\)", dataType):
			return [''.join(random.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(int(match.group(1)))) for _ in range(numberOfRecords)]
		else:
			raise NotImplementedError(f"We do not expect the field to be of another data type then INTEGER, FLOAT, or VARCHAR! But it was {dataType}.")

	def generateData(self, outputFile, numberOfRecords, schema):
		"""Generates data and then writes this data to the outputFile"""
		data = pd.DataFrame()
		for [fieldName, fieldDataType] in schema:
			if fieldName == self.keyFieldName:
				data[fieldName] = self.generateDataKeyField(numberOfRecords)
			elif fieldName == self.idFieldName:
				data[fieldName] = np.arange(1, numberOfRecords + 1)
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

	@abstractmethod
	def checkForCorrectness(self, dataFrame):
		"""Each generators performs some sort of check, if the data has been created correctly"""
		return False

	@abstractmethod
	def generateDataKeyField(self, numberOfRecords):
		"""Generates data for the key field, e.g., uniform, Zipfian, sorted. We expect the key field to be an integer"""
		pass
