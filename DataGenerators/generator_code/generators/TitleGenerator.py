#! /usr/bin/env python3

from . import DataGenerator
import numpy as np


# This class generates uniform drawn data for the title table
class TitleGenerator(DataGenerator.DataGenerator):

	def __init__(self, minValue, maxValue, keyFieldName, idFieldName):
		super().__init__(minValue, maxValue, keyFieldName, idFieldName)


	def generateDataKeyField(self, numberOfRecords):
		return np.arange(1, numberOfRecords + 1)
		
	
	def checkForCorrectness(self, dataFrame):
		return (dataFrame[self.keyFieldName] == np.arange(1, len(dataFrame[self.keyFieldName]) + 1)).all()