#!/usr/bin/env python

import sys
import pprint

from nupic.frameworks.opf.common_models.cluster_params import (
        getScalarMetricWithTimeOfDayAnomalyParams)


def getParams(columnNb,min,max):

	params = getScalarMetricWithTimeOfDayAnomalyParams(
        metricData=[0],
        minVal=float(min),
        maxVal=float(max))
	
	pprint.pprint (params)


if __name__ == '__main__':
	inputParam=sys.argv[1:]
	print inputParam
	columnNb=[0]
	min=inputParam[1]
	max=inputParam[2]
	getParams(columnNb,min,max)