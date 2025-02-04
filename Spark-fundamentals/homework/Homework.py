#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark.stop()

# Add some configurations to help with memory
spark = SparkSession.builder \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "16") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.enabled", "true") \
    .appName("MyHomework") \
    .getOrCreate()

# Load the required tables
matches = spark.read.option("header", "true").csv("/home/iceberg/data/matches.csv")
medal_matches_players = spark.read.option("header","true").csv("/home/iceberg/data/medals_matches_players.csv")
match_details = spark.read.option("header", "true").csv("/home/iceberg/data/match_details.csv")
medals = spark.read.option("header", "true").csv("/home/iceberg/data/medals.csv")
maps = spark.read.option("header", "true").csv("/home/iceberg/data/maps.csv")

# Disable default broadcast joins
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")


# In[131]:


matches.printSchema()


# In[ ]:


# Small table
# medals.printSchema()


# In[132]:


match_details.printSchema()


# In[ ]:


# Small table
# maps.printSchema()


# In[133]:


medal_matches_players.printSchema()


# In[ ]:


# Explicitly broadcast JOINs maps
brd_result_matches_maps = matches.join(
    broadcast(maps),
    on="mapid",
    how="left"
).select(matches.match_id,
         maps.mapid,
         maps.name)
brd_result_matches_maps.show()


# In[ ]:


# Explicitly broadcast JOINs medals
brd_result_matches_medals = medal_matches_players.join(
    broadcast(medals),
    on="medal_id",
    how="left"
).select(medal_matches_players.match_id,
         medal_matches_players.player_gamertag,
         medals.medal_id,
         medals.name
        )
brd_result_matches_medals.show()


# In[219]:


spark.sql("""DROP TABLE IF EXISTS bootcamp.matches_bucketed""")

# This is if you want to partition by date
# spark.sql("""
# CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (
#     match_id STRING,
#     mapid STRING,
#     playlist_id STRING,
#     completion_date STRING
# )
# USING iceberg
# CLUSTERED BY (match_id) INTO 16 BUCKETS
# PARTITIONED BY (completion_date)
# """)
# NOTE THE USAGE OF 'CLUSTERED BY ABOVE' vs
# PARTITIONED BY (completion_date, bucket(16, match_id))

spark.sql("""
CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (
    match_id STRING,
    mapid STRING,
    playlist_id STRING
)
USING iceberg
CLUSTERED BY (match_id) INTO 16 BUCKETS
""")


# In[220]:


matches.select("match_id", "mapid", "playlist_id") \
     .write.mode("append") \
     .bucketBy(16, "match_id").saveAsTable("bootcamp.matches_bucketed")


# In[ ]:


# This is if you want to partition by date first
# A simpler approach above results in OOM in Java execution
# def write_in_batches(matches_df):
#     # Get distinct dates to process in batches
#     dates = matches_df.select("completion_date").distinct().collect()
        
#     # Process one date at a time
#     for date_row in dates:
#         date_val = date_row['completion_date']
# #        print(f"Processing date: {date_val}")
        
#         # Filter data for current date
#         current_batch = matches_df.filter(
#             col("completion_date") == date_val
#         ).select(
#             "match_id", "mapid", "playlist_id", "completion_date"
#         )
        
#         # Optimize the write for current batch
#         current_batch.repartition(16, "match_id") \
#             .write \
#             .mode("append") \
#             .format("iceberg") \
#             .option("write.format.default", "parquet") \
#             .insertInto("bootcamp.matches_bucketed")
        
#         # Clean up after each batch
#         spark.catalog.clearCache()
#         current_batch.unpersist()

#     print(f"Completed")

# # Execute the batched write
# write_in_batches(matches)


# In[144]:


get_ipython().run_cell_magic('sql', '', '-- SELECT COUNT(1) FROM bootcamp.matches_bucketed.files\n')


# In[ ]:


spark.sql("""DROP TABLE IF EXISTS bootcamp.match_details_bucketed""")

spark.sql("""
CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
   match_id STRING,
   player_gamertag STRING,
   player_total_kills STRING,
   player_total_deaths STRING
)
USING iceberg
CLUSTERED BY (match_id) INTO 16 BUCKETS
""")

match_details.select("match_id", "player_gamertag", "player_total_kills", "player_total_deaths") \
    .write.mode("append") \
    .bucketBy(16, "match_id").saveAsTable("bootcamp.match_details_bucketed")


# In[ ]:


get_ipython().run_cell_magic('sql', '', '-- SELECT COUNT(1) FROM bootcamp.match_details_bucketed.files\n')


# In[ ]:


spark.sql("""DROP TABLE IF EXISTS bootcamp.medal_matches_players_bucketed""")

spark.sql("""
CREATE TABLE IF NOT EXISTS bootcamp.medal_matches_players_bucketed (
   match_id STRING,
   player_gamertag STRING,
   medal_id STRING,
   count STRING
)
USING iceberg
CLUSTERED BY (match_id) INTO 16 BUCKETS
""")

medal_matches_players.select("match_id", "player_gamertag", "medal_id", "count") \
    .write.mode("append") \
    .bucketBy(16, "match_id").saveAsTable("bootcamp.medal_matches_players_bucketed")


# In[221]:


m = spark.table("bootcamp.matches_bucketed")
mmp = spark.table("bootcamp.medal_matches_players_bucketed")
md = spark.table("bootcamp.match_details_bucketed")

aggregateDF1 = m.alias("m").join((md).alias("md"), col("m.match_id") == col("md.match_id")
).select(col("m.match_id"), col("m.mapid"), col("m.playlist_id"), col("md.player_gamertag"),  col("md.player_total_kills")
)

aggregateDF2 = aggregateDF1.alias("adf1").join((mmp).alias("mmp"), col("adf1.match_id") == col("mmp.match_id")
).select(col("adf1.*"), col("mmp.medal_id"), col("mmp.count").alias("medal_count"))

aggregateDF2.show()

aggregateView = aggregateDF2


# In[222]:


sorted_df = medals.sortWithinPartitions(col("medal_id").desc())

# Get size statistics
size_bytes = sorted_df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()
size_mb = size_bytes / (1024 * 1024)
num_partitions = sorted_df.rdd.getNumPartitions()
    
print(f"Medals Size: {size_mb:.2f} MB")
print(f"Medals Partitions: {num_partitions}")

sorted_df = m.sortWithinPartitions(col("playlist_id").desc())

# Get size statistics
size_bytes = sorted_df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()
size_mb = size_bytes / (1024 * 1024)
num_partitions = sorted_df.rdd.getNumPartitions()
    
print(f"Matches Size: {size_mb:.2f} MB")
print(f"Matches Partitions: {num_partitions}")

sorted_df = md.sortWithinPartitions(col("match_id").desc())

# Get size statistics
size_bytes = sorted_df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()
size_mb = size_bytes / (1024 * 1024)
num_partitions = sorted_df.rdd.getNumPartitions()
    
print(f"Matches Details Size: {size_mb:.2f} MB")
print(f"Matches Details Partitions: {num_partitions}")

sorted_df = mmp.sortWithinPartitions(col("medal_id").desc())

# Get size statistics
size_bytes = sorted_df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()
size_mb = size_bytes / (1024 * 1024)
num_partitions = sorted_df.rdd.getNumPartitions()
    
print(f"Matches Medals Players Details Size: {size_mb:.2f} MB")
print(f"Matches Medals Players Partitions: {num_partitions}")


# In[ ]:


get_ipython().run_cell_magic('sql', '', '-- SELECT COUNT(1) FROM bootcamp.medal_matches_players_bucketed.files\n')


# In[ ]:


get_ipython().run_cell_magic('sql', '', '-- -- Top player with most avg kills per match\n-- SELECT player_gamertag, CAST(AVG(player_total_kills) AS INT) as avg_kills FROM bootcamp.match_details_bucketed\n--  GROUP BY player_gamertag\n--  ORDER BY CAST(AVG(player_total_kills) AS INT) DESC\n--  LIMIT 1\n')


# In[223]:


# Top player with most avg kills per match
spark.table("bootcamp.match_details_bucketed"
).groupBy("player_gamertag"
).agg(avg("player_total_kills").alias("avg_kills")
).orderBy(desc("avg_kills")
).limit(1).show()


# In[147]:


get_ipython().run_cell_magic('sql', '', '-- -- Top played playlist\n-- SELECT playlist_id, COUNT(match_id) as top_played FROM bootcamp.matches_bucketed\n-- GROUP BY playlist_id\n-- ORDER BY COUNT(match_id) DESC\n-- LIMIT 1\n')


# In[224]:


# Top played playlist
spark.table("bootcamp.matches_bucketed"
).groupBy("playlist_id"
).agg(count("match_id").alias("top_played")
).orderBy(desc("top_played")
).limit(1).show()


# In[225]:


# Top played map
spark.table("bootcamp.matches_bucketed").join(
    broadcast(maps),
    on="mapid",
    how="left"
).select("match_id",
         "mapid",
         col("name").alias("map_name")
).groupBy("mapid", "map_name"
).agg(countDistinct("match_id").alias("top_played")
).orderBy(desc("top_played")
).limit(1).show()


# In[226]:


# Top map for Killing Spree medal

spark.table("bootcamp.medal_matches_players_bucketed").join(
    broadcast(medals),
    on="medal_id",
    how="left"
).select("match_id",
         "medal_id",
         col("name").alias("medal_name"),
         "count"
).filter(col("name") == "Killing Spree"
).join(
    brd_result_matches_maps,
    on="match_id",
    how="left"
).select("match_id",
         "mapid",
         col("name").alias("map_name"),
         "medal_name",
         "count"
).orderBy(desc("count")
).limit(1).show()


# In[227]:


# Top played map using aggregated table
# TODO: why numbers do not match?

aggregateView.join(
    broadcast(maps),
    on="mapid",
    how="left"
).select("match_id",
         "mapid",
         col("name").alias("map_name")
).groupBy("mapid", "map_name"
).agg(countDistinct("match_id").alias("top_played")
).orderBy(desc("top_played")
).limit(1).show()


# In[228]:


# Top map for Killing Spree medal using aggregated table

aggregateView.join(
    broadcast(medals),
    on="medal_id",
    how="left"
).select("match_id",
         "medal_id",
         col("name").alias("medal_name"),
         "medal_count"
).filter(col("name") == "Killing Spree"
).join(
    brd_result_matches_maps,
    on="match_id",
    how="left"
).select("match_id",
         "mapid",
         col("name").alias("map_name"),
         "medal_name",
         "medal_count"
).orderBy(desc("medal_count")
).limit(1).show()


# In[ ]:




