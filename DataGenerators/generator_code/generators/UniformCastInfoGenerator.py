#! /usr/bin/env python3

from . import DataGenerator
from scipy import stats
import numpy as np


# This class generates uniform drawn data for the cast_info table
class UniformCastInfoGenerator(DataGenerator.DataGenerator):
	def generateDataKeyField(self, numberOfRecords):
		return np.random.randint(self.minValue, self.maxValue, numberOfRecords)


	def checkForCorrectness(self, dataFrame):
		skew = stats.skew(dataFrame[self.keyFieldName])

		# As Zipfian distribution typically has a skewness of close to 0
		if skew >= 0.25:
			printFail(f"skew: {skew}")
			return False
		return True