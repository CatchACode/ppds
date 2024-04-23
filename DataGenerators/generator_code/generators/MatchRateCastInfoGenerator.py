#! /usr/bin/env python3

from . import DataGenerator
import numpy as np
import math

# This class generates uniform drawn data for the cast_info table that will produce a given rate of matches during a join
class MatchRateCastInfoGenerator(DataGenerator.DataGenerator):

	def __init__(self, minValue, maxValue, keyFieldName, idFieldName, matchRate):
		super().__init__(minValue, maxValue, keyFieldName, idFieldName)
		self.matchRate = matchRate
		self.newKeyValue = maxValue + 1000 # Setting the key to something not in the range


	def generateDataKeyField(self, numberOfRecords):
		numberOfRecordsToReplace = math.floor(numberOfRecords * (1 - self.matchRate))
		replaceIndices = np.random.choice(numberOfRecords, numberOfRecordsToReplace, replace = False)
				
		keyData = self.generateUniformRandomData(numberOfRecords, "INTEGER")
		keyData[replaceIndices] = self.newKeyValue

		return keyData


	def checkForCorrectness(self, dataFrame):
		# Counting the percentage of items that are equal to self.newKeyValue. This should be roughly equal to the match rate
		actualMatchRate = 1 - dataFrame[self.keyFieldName].value_counts(normalize=True)[self.newKeyValue]
		return abs(actualMatchRate - self.matchRate) < 0.05