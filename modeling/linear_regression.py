from datetime import datetime
from pyspark.ml.linalg import Vectors
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.sql.functions import abs

data_url = "gs://bigdatasystems_alex_bucket/project/user_info16/part*"
raw_data = (spark.read.option("header", "true").option("inferschema","true").option("mode","DROPMALFORMED").csv(data_url))

assembler = VectorAssembler(inputCols=["region_id", "user_desc_rating", "count"], outputCol="features")
featured_data = assembler.transform(raw_data.na.fill(0))
featured_data = featured_data.filter(featured_data.user_desc_rating != 0.0)
train, test = featured_data.randomSplit([.8, .2], 0)

# Train model
lr = LinearRegression(labelCol="tweet_rating")
lrModel = lr.fit(train)

# Model and Training info
print("Coefficients: %s" % str(lrModel.coefficients))
print("Intercept: %s" % str(lrModel.intercept))
trainingSummary = lrModel.summary
print("numIterations: %d" % trainingSummary.totalIterations)
print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
trainingSummary.residuals.show()
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)

# Compute simple error
tested = lrModel.transform(test)
err = tested.select('prediction').subtract(tested.select('tweet_rating'))
err = err.withColumn('error', abs(err.prediction))
avgerr = err.agg({'error':'avg'})

### Row(avg(error)=0.5060241689374068)

# Compared to average rating
avgrate = raw_data.agg({'tweet_rating':'avg'})
avgrate.head()

### Row(avg(tweet_rating)=0.608442380075144)