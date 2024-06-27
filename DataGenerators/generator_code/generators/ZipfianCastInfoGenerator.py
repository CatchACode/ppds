#! /usr/bin/env python3

from . import DataGenerator
from scipy import stats
import numpy as np


# This class generates skewed drawn data for the cast_info table
class ZipfianCastInfoGenerator(DataGenerator.DataGenerator):
	def __init__(self, minValue, maxValue, keyFieldName, idFieldName, alpha=2.0):
		super().__init__(minValue, maxValue, keyFieldName, idFieldName)
		self.alpha = alpha

	def generateDataKeyField(self, numberOfRecords):

		if self.minValue == 0:
			raise ZeroDivisionError("")
				
		# Heavily inspired by https://stackoverflow.com/a/57420941
		v = np.arange(self.minValue, self.maxValue + 1)
		p = 1.0 / np.power(v, self.alpha)
		p /= np.sum(p)

		return np.random.choice(v, size=numberOfRecords, replace=True, p=p)

	def checkForCorrectness(self, dataFrame):
		skew = stats.skew(dataFrame[self.keyFieldName])

		# As Zipfian distribution typically has a skewness of more than 2
		if skew < 2:
			printFail(f"skew: {skew}")
			return False
		return True