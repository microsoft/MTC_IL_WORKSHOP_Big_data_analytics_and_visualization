# Databricks notebook source
# MAGIC %md # Train the model

# COMMAND ----------

# MAGIC %md Margie's Travel wants to build a model to predict if a departing flight will have a 15-minute or greater delay. In the historical data they have provided, the indicator for such a delay is found within the DepDel15 (where a value of 1 means delay, 0 means no delay). To create a model that predicts such a binary outcome, we can choose from the various Two-Class algorithms provided by Spark MLlib. For our purposes, we choose Decision Tree. This type of classification module needs to be first trained on sample data that includes the features important to making a prediction and must also include the actual historical outcome for those features. 
# MAGIC 
# MAGIC The typical pattern is to split the historical data so a portion is shown to the model for training purposes, and another portion is reserved to test just how well the trained model performs against examples it has not seen before.

# COMMAND ----------

# MAGIC %md To start, let's import the Python libraries and modules we will use in this notebook.

# COMMAND ----------

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql.functions import array, col, lit

# COMMAND ----------

# MAGIC %md ## Load the cleaned flight and weather data

# COMMAND ----------

# MAGIC %md Load the data from the global table.

# COMMAND ----------

dfDelays = spark.sql("select OriginAirportCode, cast(Month as int) Month, cast(DayofMonth as int) DayofMonth, CRSDepHour, cast(DayOfWeek as int) DayOfWeek, Carrier, DestAirportCode, DepDel15, WindSpeed, SeaLevelPressure, HourlyPrecip from flight_delays_with_weather")
cols = dfDelays.columns

# COMMAND ----------

display(dfDelays)

# COMMAND ----------

# MAGIC %md We can get a sense of which origin and destination airports suffer the most delays by querying against the table and displaying the output as an area chart. We've already configured the chart's settings so you should see a nice visual when you run the below command. If it displays in a table instead, just select the area chart option below the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC select OriginAirportCode, DestAirportCode, count(DepDel15)
# MAGIC from flight_delays_with_weather where DepDel15 = 1
# MAGIC group by OriginAirportCode, DestAirportCode
# MAGIC ORDER BY count(DepDel15) desc

# COMMAND ----------

# MAGIC %md ## Sampling the data

# COMMAND ----------

# MAGIC %md To begin, let's evaluate the data to compare the flights that are delayed (DepDel15) to those that are not. What we're looking for is whether one group has a much higher count than the other.

# COMMAND ----------

dfDelays.groupBy("DepDel15").count().show()

# COMMAND ----------

# MAGIC %md Judging by the delay counts, there are almost four times as many non-delayed records as there are delayed.
# MAGIC 
# MAGIC We want to ensure our model is sensitive to the delayed samples. To do this, we can use stratified sampling provided by the `sampleBy()` function. First we create fractions of each sample type to be returned. In our case, we want to keep all instances of delayed (value of 1) and downsample the not delayed instances to 30%.

# COMMAND ----------

fractions = {0: .30, 1: 1.0}
trainingSample = dfDelays.sampleBy("DepDel15", fractions, 36)
trainingSample.groupBy("DepDel15").count().show()

# COMMAND ----------

# MAGIC %md You can see that the number of delayed and not delayed instances are now much closer to each other. This should result in a better-trained model.

# COMMAND ----------

# MAGIC %md ## Select an algorithm and transform features

# COMMAND ----------

# MAGIC %md Because we are trying to predict a binary label (flight delayed or not delayed), we need to use binary classification. For this, we will be using the [Decision Tree](https://spark.apache.org/docs/latest/ml-classification-regression.html#decision-tree-classifier) classifier algorithm provided by the Spark MLlib library. We will also be using the [Pipelines API](https://spark.apache.org/docs/latest/ml-guide.html) to put our data through all of the required feature transformations in a single call. The Pipelines API provides higher-level API built on top of DataFrames for constructing ML pipelines.

# COMMAND ----------

# MAGIC %md In the data cleaning phase, we identified the important features that most contribute to the classification. The `flight_delays_with_weather` is the result of the data preparation and feature identification process. The features are:
# MAGIC 
# MAGIC | OriginAirportCode | Month | DayofMonth | CRSDepHour | DayOfWeek | Carrier | DestAirportCode | WindSpeed | SeaLevelPressure | HourlyPrecip |
# MAGIC | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
# MAGIC | LGA | 5 | 2 | 13 | 4 | MQ | ORD | 6 | 29.8 | 0.05 |
# MAGIC 
# MAGIC We also have a label named `DepDelay15` which equals 0 if no delay, and 1 if there was a delay.
# MAGIC 
# MAGIC As you can see, this dataset contains nominal variables like OriginAirportCode (LGA, MCO, ORD, ATL, etc). In order for the machine learning algorithm to use these nominal variables, they need to be transformed and put into Feature Vectors, or vectors of numbers representing the value for each feature.
# MAGIC 
# MAGIC For simplicity's sake, we will use One-Hot Encoding to convert all categorical variables into binary vectors. We will use a combination of StringIndexer and OneHotEncoderEstimator to convert the categorical variables. The `OneHotEncoderEstimator` will return a `SparseVector`.
# MAGIC 
# MAGIC Since we will have more than 1 stage of feature transformations, we use a Pipeline to tie the stages together. This simplifies our code.

# COMMAND ----------

# MAGIC %md The ML package needs the label and feature vector to be added as columns to the input dataframe. We set up a pipeline to pass the data through transformers in order to extract the features and label. We index each categorical column using the `StringIndexer` to a column of number indices, then convert the indexed categories into one-hot encoded variables with at most a single one-value. These binary vectors are appended to the end of each row. Encoding categorical features allows decision trees to treat categorical features appropriately, improving performance. We then use the `StringIndexer` to encode our labels to label indices.

# COMMAND ----------

categoricalColumns = ["OriginAirportCode", "Carrier", "DestAirportCode"]
stages = [] # stages in our Pipeline
for categoricalCol in categoricalColumns:
    # Category Indexing with StringIndexer
    stringIndexer = StringIndexer(inputCol=categoricalCol, outputCol=categoricalCol + "Index")
    # Use OneHotEncoderEstimator to convert categorical variables into binary SparseVectors
    encoder = OneHotEncoder(dropLast=False, inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
    # Add stages.  These are not run here, but will run all at once later on.
    stages += [stringIndexer, encoder]

# Convert label into label indices using the StringIndexer
label_stringIdx = StringIndexer(inputCol="DepDel15", outputCol="label")
stages += [label_stringIdx]

# COMMAND ----------

# MAGIC %md Now we need to use the `VectorAssembler` to combine all the feature columns into a single vector column. This includes our numeric columns as well as the one-hot encoded binary vector columns.

# COMMAND ----------

# Transform all features into a vector using VectorAssembler
numericCols = ["Month", "DayofMonth", "CRSDepHour", "DayOfWeek", "WindSpeed", "SeaLevelPressure", "HourlyPrecip"]
assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [assembler]

# COMMAND ----------

# MAGIC %md ## Create and train the Decision Tree model

# COMMAND ----------

# MAGIC %md Before we can train our model, we need to randomly split our data into test and training sets. As is standard practice, we will allocate a larger portion (70%) for training. A seed is set for reproducibility, so the outcome is the same (barring any changes) each time this cell and subsequent cells are run.
# MAGIC 
# MAGIC Remember to use our stratified sample (`trainingSample`).

# COMMAND ----------

### Randomly split data into training and test sets. set seed for reproducibility
(trainingData, testData) = trainingSample.randomSplit([0.7, 0.3], seed=100)
# We want to have two copies of the training and testing data, since the pipeline runs transformations and we want to run a couple different iterations
trainingData2 = trainingData
testData2 = testData
print(trainingData.count())
print(testData.count())

# COMMAND ----------

# MAGIC %md Our pipeline is ready to be built and run, now that we've created all the transformation stages. We just have one last stage to add, which is the Decision Tree. Let's run the pipeline to put the data through all the feature transformations within a single call.
# MAGIC 
# MAGIC Calling `pipeline.fit(trainingData)` will transform the test data and use it to train the Decision Tree model.
# MAGIC 
# MAGIC We will also use the MLflow library to track the details of this experiment, including testing results and the model we create.

# COMMAND ----------

from pyspark.ml.classification import DecisionTreeClassifier
import mlflow
import mlflow.spark

mlflow.start_run()

# Create initial Decision Tree Model
dt = DecisionTreeClassifier(labelCol="label", featuresCol="features", maxDepth=3)
stages += [dt]

# Create a Pipeline.
pipeline = Pipeline(stages=stages)
# Run the feature transformations.
#  - fit() computes feature statistics as needed.
#  - transform() actually transforms the features.
pipelineModel = pipeline.fit(trainingData)
trainingData = pipelineModel.transform(trainingData)
# Keep relevant columns
selectedcols = ["label", "features"] + cols
trainingData = trainingData.select(selectedcols)
display(trainingData)

# COMMAND ----------

# MAGIC %md Let's make predictions on our test dataset using the `transform()`, which will only use the 'features' column. We'll display the prediction's schema afterward so you can see the three new prediction-related columns.

# COMMAND ----------

# Make predictions on test data using the Transformer.transform() method.
predictions = pipelineModel.transform(testData)

# COMMAND ----------

# MAGIC %md To properly train the model, we need to determine which parameter values of the decision tree produce the best model. A popular way to perform model selection is k-fold cross validation, where the data is randomly split into k partitions. Each partition is used once as the testing data set, while the rest are used for training. Models are then generated using the training sets and evaluated with the testing sets, resulting in k model performance measurements. The model parameters leading to the highest performance metric produce the best model.
# MAGIC 
# MAGIC We can use `BinaryClassificationEvaluator` to evaluate our model. We can set the required column names in `rawPredictionCol` and `labelCol` Param and the metric in `metricName` Param.

# COMMAND ----------

# MAGIC %md Let's evaluate the Decision Tree model with `BinaryClassificationEvaluator`.

# COMMAND ----------

from pyspark.ml.evaluation import BinaryClassificationEvaluator
# Evaluate model
evaluator = BinaryClassificationEvaluator()
areaUnderRoc = evaluator.evaluate(predictions)
mlflow.log_metric("Area Under ROC", areaUnderRoc)
areaUnderRoc

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, we will save the model to disk and end the first MLflow run.

# COMMAND ----------

mlflow.spark.log_model(pipelineModel, "model")
modelpath = "/dbfs/mlflow/mt/model-dtree"
mlflow.spark.save_model(pipelineModel, modelpath)
mlflow.end_run()

# COMMAND ----------

# MAGIC %md Now we will try tuning the model with the `ParamGridBuilder` and the `CrossValidator`.
# MAGIC 
# MAGIC As we indicate 3 values for maxDepth and 3 values for maxBin, this grid will have 3 x 3 = 9 parameter settings for `CrossValidator` to choose from. We will create a 3-fold CrossValidator.

# COMMAND ----------

from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# Create ParamGrid for Cross Validation
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
paramGrid = (ParamGridBuilder()
             .addGrid(dt.maxDepth, [1, 2, 6, 10])
             .addGrid(dt.maxBins, [20, 40, 80])
             .build())

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to create your 3-fold CrossValidator and use it to run cross validations. It can take **up to 5 minutes** to execute the cell.
# MAGIC 
# MAGIC Because we are training a new model, we will do this in another run of the same experiment.  That way, we can compare the cross-validated version to the original decision tree and choose the better model for deployment.

# COMMAND ----------

mlflow.start_run()

# Create 3-fold CrossValidator
cv = CrossValidator(estimator=pipeline, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=3)

# Run cross validations (this can take several minutes to execute)
cvModel = cv.fit(trainingData2)

# COMMAND ----------

# MAGIC %md Now let's create new predictions with which to measure the accuracy of our model.

# COMMAND ----------

predictions = cvModel.transform(testData2)

# COMMAND ----------

# MAGIC %md We'll use the predictions to evaluate the best model. `cvModel` uses the best model found from the Cross Validation.

# COMMAND ----------

areaUnderRoc = evaluator.evaluate(predictions)
mlflow.log_metric("Area Under ROC", areaUnderRoc)
areaUnderRoc

# COMMAND ----------

# MAGIC %md Now let's view the best model's predictions and probabilities of each prediction class.

# COMMAND ----------

selected = predictions.select("label", "prediction", "probability", "OriginAirportCode", "DestAirportCode")
display(selected)

# COMMAND ----------

# MAGIC %md We need to take the best model from `cvModel` and generate predictions for the entire dataset (dfDelays), then evaluate the best model.

# COMMAND ----------

bestModel = cvModel.bestModel
finalPredictions = bestModel.transform(dfDelays)
areaUnderRoc = evaluator.evaluate(finalPredictions)
mlflow.log_metric("Final Area Under ROC", areaUnderRoc)
areaUnderRoc

# COMMAND ----------

# MAGIC %md Finally, we will save this model to disk and end the run.

# COMMAND ----------

mlflow.spark.log_model(bestModel, "model")
modelpath = "/dbfs/mlflow/mt/model-dtree-cv"
mlflow.spark.save_model(bestModel, modelpath)
mlflow.end_run()

# COMMAND ----------

# MAGIC %md ## Save the model for batch scoring

# COMMAND ----------

# MAGIC %md There are two reasons for saving the model in this lab. The first is so you can access the trained model later if your cluster restarts for any reason, and also from within another notebook. Secondly, we will need to make the model available externally so we can perform batch scoring against it in Exercise 5. Save the model to DBFS so it can be accessed across any clusters in the Databricks Workspace.
# MAGIC 
# MAGIC NOTE: Save the model in the root of DBFS as this is where Spark Pipelines will look by default.

# COMMAND ----------

# Save the best model under /dbfs/flightDelayModel
bestModel.write().overwrite().save("/flightDelayModel")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next step
# MAGIC 
# MAGIC Continue to the next notebook, [03 Deploy as Web Service]($./03%20Deploy%20as%20Web%20Service).
