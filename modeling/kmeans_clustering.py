from datetime import datetime
from pyspark.ml.linalg import Vectors
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.sql.functions import abs
from time import time

data_url = "gs://bigdatasystems_alex_bucket/project/user_info16/part*"
raw_data = (spark.read.option("header", "true").option("inferschema","true").option("mode","DROPMALFORMED").csv(data_url))

assembler = VectorAssembler(inputCols=["followers", "friends", "favorited", "status_count", "region_id", "user_desc_rating", "count"], outputCol="feat_vector")
featured_data = assembler.transform(raw_data.na.fill(0))
featuresScaler = StandardScaler(inputCol="feat_vector", outputCol="features")
featuresModel = featuresScaler.fit(featured_data)
scFeatData = featuresModel.transform(featured_data)


for k in range(2, 25, 2):
	start = time()
	model = KMeans().setK(k).setSeed(0).fit(scFeatData)
	delta = time() - start
	wssse = model.computeCost(scFeatData)
	print(k, "\t", wssse, "\t", delta)
