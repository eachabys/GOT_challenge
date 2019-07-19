"""This is a solution to the indexing "Game of Thrones" challenge. 
The challenge description can be found here
https://github.com/Samariya57/coding_challenges/blob/master/challenge.pdf
The solution can be implemented in pyspark (Python v.2.7.12)"""
import re
import sys
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row, DataFrame,SparkSession, SQLContext
import configparser
from datetime import datetime
import time
import operator
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from itertools import chain
from collections import Counter

#set up Input/Output parameters
#Location of all input data
source_bucket='got-challenge-insight'

def get_dictionary(sc):
    """Read the data from the source as words with unique indexes"""
    print("STARTTTTTTTTTTTTT")
    
    #read all the data from input directory
    rdd=sc.textFile('s3a://'+source_bucket+'/*')

    #get rdd for each single word in an input file. 
    #assign each word a special index
    rdd=rdd.map(lambda x: x.encode("utf-8", "ignore"))\
      .flatMap(lambda x: (''.join(x)).split(' '))\
      .map(lambda word: re.sub(ur"[^\w\d'\s]+", '', word))\
      .map(lambda word: re.sub(r'\d+', '', word))\
      .filter(lambda word: len(word)>0)\
      .map(lambda word: word.lower())\
      .distinct()\
      .zipWithIndex()
    print rdd.take(20)
    return(rdd)

def invert_indexes(sc):
    """For each input file generate a set of words with corresponding indexes"""
    print("STARTTTTTTTTTTTTT")
    
    #group set of words with unique ids by corresponding book ID (unique filename) 
    rdd=rdd.map(lambda x: x.encode("utf-8", "ignore"))\
      .flatMap(lambda x: (''.join(x)).split(' '))\
      .map(lambda word: re.sub(ur"[^\w\d'\s]+", '', word))\
      .map(lambda word: re.sub(r'\d+', '', word))\
      .filter(lambda word: len(word)>0)\
      .map(lambda word: word.lower())\
      .distinct()\
      .zipWithIndex()
    #print rdd.take(20)
    return(rdd)

def output_data():
    #Save the outputs
    print("CONTINUEEEEEEEEEEEE")
    rdd=get_dictionary(sc) 
    words_dictionary=rdd.collect()
    file1=open(output1,"w+")
    for row in words_dictionary:
    	file1.write(' '.join(str(r) for r in row) + "\n")

if __name__ == '__main__':
    start_time=time.time()
    sc = SparkContext(conf=SparkConf().setAppName("indexing"))
    spark_session = SparkSession.builder.appName("indexing").getOrCreate()
    sqlContext = SQLContext(sc)
    output1='dictionary.txt'
    output_data()
    