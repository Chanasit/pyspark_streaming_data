from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('wordCount').getOrCreate()

from pyspark.sql.functions import lower, split, explode, substring, count
from matplotlib.ticker import MaxNLocator

import matplotlib.pyplot as plt



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


# set timeout 1 equal 1 mins
query.awaitTermination(timeout=300)

result = spark.sql("select * from topTagsTable").limit(12)

print('result:', result)

query.stop()

top_hashtags = result.toPandas()

bnk48_count = top_hashtags[top_hashtags['hashtag']=='#bnk48']['count'].values

fig, ax = plt.subplots(1,1,figsize=(10,6)) 

top_hashtags[top_hashtags['hashtag']!='#bnk48'].plot(kind='bar', x='hashtag', y='count', legend=False, ax=ax)

ax.set_title("Top 10 hashtags related to #BNK48 (%d counts)" % bnk48_count, fontsize=18)

ax.set_xlabel("Hashtag", fontsize=18)

ax.set_ylabel("Count", fontsize=18)

ax.set_xticklabels(ax.get_xticklabels(), {"fontsize":14}, rotation=30)

ax.yaxis.set_major_locator(MaxNLocator(integer=True)) # show only integer yticks

plt.yticks(fontsize=14)

# show graph ploted
plt.show()
