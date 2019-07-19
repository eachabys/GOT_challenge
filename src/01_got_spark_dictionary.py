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

#set up Input/Output parameters
#Location of all input data
source_bucket='got-challenge-insight'
file_lists=['12','13']


def get_data(sc):
    """Read the data from the source as words with unique indexes"""
    print("STARTTTTTTTTTTTTT")
    indexed_documents=[]
    for i in file_lists:
        #read all the data from input directory
        rdd=sc.textFile('s3a://'+source_bucket+'/'+i+'*')
        #get rdd for each single word in an input file. 
        #assign each word a special index
        rdd=rdd.map(lambda x: x.encode("utf-8", "ignore"))\
          .flatMap(lambda x: (''.join(x)).split(' '))\
          .map(lambda word: re.sub(ur"[^\w\d'\s]+", '', word))\
          .map(lambda word: re.sub(r'\d+', '', word))\
          .filter(lambda word: len(word)>0)\
          .map(lambda word: (word.lower(),i))\
          .distinct()\
          .zipWithIndex()
        
        #assign unique indexes to each word
        rddindex=rdd.map(lambda word: str(word).split(',')[2])\
          .map(lambda index: index.replace(')',''))\
          .map(lambda index: index)

        #collect list of files (documents) with words and corresponding unique ids
        indexed_documents.append((i,rddindex.collect()))
    
    return(rdd,indexed_documents)

def output_data():
    #Save the outputs
    print("OUT OUT OUT")
    dictionary=get_data(sc)[0]
    indexed_documents=get_data(sc)[1]
    #print rdd.take(20)
    words_dictionary=dictionary.collect()
    file1=open(output1,"w+")
    for row in words_dictionary:
        file1.write(' '.join(str(r) for r in row) + "\n")
    file2=open(output2,"w+")
    for row in indexed_documents:
        file2.write(' : '.join(str(r) for r in row) + "\n")

if __name__ == '__main__':
    start_time=time.time()
    sc = SparkContext(conf=SparkConf().setAppName("indexing"))
    spark_session = SparkSession.builder.appName("indexing").getOrCreate()
    sqlContext = SQLContext(sc)
    output1='dictionary.txt'
    output2='inverted_index.txt'
    output_data()
    