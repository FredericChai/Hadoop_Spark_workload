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
def map_valid_record(item):
	'''format the record to key value pair
		use ',' to split each part and convert date to int
	Input: line of file
	Output: ['video_id, category', [{category},[like,dislike,trending date]]]
	'''
	item = item.split(',')
	date = item[1].split('.')
	date_to_int = int(date[0]+date[2]+date[1])
	return [','.join((item[0],item[11])),[{'category':item[3]},np.array([[int(item[6]),int(item[7]),date_to_int]])]]

def growth_number(item):
	'''calculate the growth_number for each key : 'video_id, category'
		to get the growth, we caculate ((second dislike - first dislike) - (second like - first like))
	Input: line
	Output: ['video_id, category', [{category},growth_number]]
	'''
	var_num = item[1][1][1] - item[1][1][0]
	return [item[0].encode('utf-8'), [item[1][0]['category'].encode('utf-8'),var_num[1] - var_num[0]]]
video_rdd = video_rdd.mapPartitionsWithIndex(
 lambda index, item: islice(item, 1, None) if index == 0 else item 
).map(map_valid_record)
video_rdd = video_rdd.reduceByKey(lambda x,y: [x[0],np.concatenate((x[1],y[1]),axis=0)])#allocate the first category as its final category and merge the number of like and dislike by video_id and country
video_rdd = video_rdd.filter(lambda item: len(item[1][1]) >= 2) #filter videos at least have two records
video_rdd = video_rdd.mapValues(lambda item : [item[0],sorted(item[1],key = lambda x : x[2])])#sort trending date with asc order
video_rdd = video_rdd.map(growth_number)
top_10 = video_rdd.takeOrdered(10, key = lambda item: -item[1][1])#return the top 10
res_list = [', '.join((item[0].split(',')[0],str(item[1][1]),item[1][0],item[0].split(',')[1])) for item in top_10]
res_rdd = sc.parallelize(res_list)
res_rdd.coalesce(1).saveAsTextFile(args.output)