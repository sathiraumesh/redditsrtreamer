import praw
from pyspark.sql import SparkSession, SQLContext
from operator import add
import csv
import os
import re
from time import time, sleep
import sched, time

reddit = praw.Reddit(client_id='', 
                     client_secret='', 
                     user_agent='wordcount', 
                     username='', 
                     password='')

sparkSession = SparkSession.builder.appName('wordcounter').getOrCreate()

sc = sparkSession.sparkContext

def get_keyval(row):
    text = row.title
    text=re.sub("\\W"," ",text)
    words = text.lower ().split (" ")
    return [[w, 1] for w in words]


def get_counts(df):
    df.show (2, False)
    mapped_rdd = df.rdd.flatMap (lambda row: get_keyval (row))
    counts_rdd = mapped_rdd.reduceByKey (add)
    word_count = counts_rdd.collect ()
    for e in word_count:
        print (e)

def process_csv(abspath, sparkcontext):
    sqlContext = SQLContext (sparkcontext)
    df = sqlContext.read.load (os.path.join (abspath, "new_data.csv"),format='com.databricks.spark.csv',header='true',inferSchema='true').select("title")
    get_counts(df)


s = sched.scheduler(time.time, time.sleep)

def count_words(sci):
    data = []
    no_subreddit = reddit.subreddit('all')
    hot = no_subreddit.new(limit=10000)
    for row in hot:
        data.append((row.title, len(row.title)))
    df = sparkSession.createDataFrame(data, ["title", "count"])
    df.write.csv("hdfs:///myfiles/new_data.csv", mode="overwrite", header=True)
    process_csv("hdfs:///myfiles", sc)
    sci.enter(2*60, 2*60, count_words, (sci,))

if __name__ == '__main__':
    s.enter(2*60, 2*60, count_words, (s,))
    s.run()