from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('wordCount').getOrCreate()

from pyspark.sql.functions import lower, split, explode, substring, count


# Create DataFrame representing the stream of input lines from connection to localhost:5555
tweets = spark.readStream.format("socket").option("host", "localhost").option("port", 5555).load()

# 
words_train = tweets.select(split(lower(tweets.value), " ").alias("value"))
words_df = words_train.select(explode(words_train.value).alias('word_explode'))

# 
hashtags = words_df.filter(substring(words_df.word_explode,0,1)=='#')
hashtags = hashtags.select(hashtags.word_explode.alias('hashtag'))

# 
count_hashtags = hashtags.groupBy('hashtag').count()
count_hashtags_order = count_hashtags.orderBy(count_hashtags['count'].desc())

# 
# query = count_hashtags_order.writeStream.outputMode("complete").format("console").start()
query = count_hashtags_order.writeStream.outputMode("complete").format("memory").queryName("topTagsTable").start()


# 
query.awaitTermination(timeout=600)

result = spark.sql("select * from topTagsTable").limit(5).toPandas()

print('result:', result)

# 
query.stop()

