#!/usr/bin/python3
from itertools import islice
from pyspark import SparkContext
import numpy as np
import argparse
sc = SparkContext(appName="Spark")
parser = argparse.ArgumentParser()
parser.add_argument("--input", help="the input path",
                        default='~/')
parser.add_argument("--output", help="the output path", 
                        default='rating_out_script') 
args = parser.parse_args()
#generate RDD from the input file
video_rdd = sc.textFile(args.input) 
