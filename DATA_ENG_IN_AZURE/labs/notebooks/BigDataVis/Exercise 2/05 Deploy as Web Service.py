# Databricks notebook source
# MAGIC %md #Deploy model to Azure Databricks

# COMMAND ----------

# MAGIC %md In this notebook, you will deploy the best performing model you selected previously as a web service hosted in in Azure Databricks cluster.

# COMMAND ----------

import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient
import time
from mlflow.entities.model_registry.model_version_status import ModelVersionStatus

client = MlflowClient()

# COMMAND ----------

# MAGIC %md First, get the experiment from the prior notebook.

# COMMAND ----------

user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
experiment_name = "/Users/{user_name}/BigDataVis/Exercise 2/04 Train and Evaluate Models".format(user_name=user_name)

experiment = client.get_experiment_by_name(experiment_name)

# COMMAND ----------

# MAGIC %md Get the latest run from the experiment--as a reminder, this is the cross-validation model.  In order to retrieve the model itself, you will need to get the **parent** run ID, as that is where the model details are stored.

# COMMAND ----------

experiment_id = experiment.experiment_id
runs_df = client.search_runs(experiment_id, order_by=["attributes.start_time desc"], max_results=1)
display(runs_df[0].data.tags)

# COMMAND ----------

run_id = '<current run_id>' #runs_df[0].data.tags["mlflow.parentRunId"]

model_name = "Delay Estimator"

artifact_path = "model"
model_uri = "runs:/{run_id}/{artifact_path}".format(run_id=run_id, artifact_path=artifact_path)
model_uri

# COMMAND ----------

# MAGIC %md The next step is to register this model with MLflow.  This may take anywhere fdrom 30 seconds to 5 minutes to complete.

# COMMAND ----------

model_details = mlflow.register_model(model_uri=model_uri, name=model_name)

# Wait until the model is ready
def wait_until_ready(model_name, model_version):
  client = MlflowClient()
  for _ in range(10):
    model_version_details = client.get_model_version(
      name=model_name,
      version=model_version,
    )
    status = ModelVersionStatus.from_string(model_version_details.status)
    print("Model status: %s" % ModelVersionStatus.to_string(status))
    if status == ModelVersionStatus.READY:
      break
    time.sleep(1)

wait_until_ready(model_details.name, model_details.version)

# COMMAND ----------

# MAGIC %md Now that the model is registered, move it to Production.  This will make the current model the production model, allowing you to serve this iteration of the model.

# COMMAND ----------

client.transition_model_version_stage(
  name=model_details.name,
  version=model_details.version,
  stage='Production',
)
model_version_details = client.get_model_version(
  name=model_details.name,
  version=model_details.version,
)
print("The current model stage is: '{stage}'".format(stage=model_version_details.current_stage))

# COMMAND ----------

# MAGIC %md ## Serve the Model

# COMMAND ----------

# MAGIC %md You have already created a model, but the next step will be to serve the model.  At present, the best way to do this is to select the **Models** menu option on the left-hand pane.  Note that you will only see this menu in the **Machine Learning** view.  If you are still in the **Data Science & Engineering** view, select the drop-down option from the menu and select **Machine Learning** first.
# MAGIC 
# MAGIC In the Models menu, select the **Delay Estimator** model.  Navigate to the **Serving** menu and enable serving.  This will build a Databricks cluster to allow you to perform inference.
# MAGIC 
# MAGIC Wait for both the Status indicator as well as the Production model indicator to read Ready and then copy the Model URL with "Production" in it.  This will take 5-10 minutes to complete.

# COMMAND ----------

# MAGIC %md #Test the scoring web service

# COMMAND ----------

# MAGIC %md
# MAGIC In order to test the service, create two sample rows for testing and load them into a Pandas DataFrame.

# COMMAND ----------

import json
import pandas as pd

# Create two records for testing the prediction
test_input1 = {"OriginAirportCode":"SAT","Month":5,"DayofMonth":5,"CRSDepHour":13,"DayOfWeek":7,"Carrier":"MQ","DestAirportCode":"ORD","WindSpeed":9,"SeaLevelPressure":30.03,"HourlyPrecip":0}

test_input2 = {"OriginAirportCode":"ATL","Month":2,"DayofMonth":5,"CRSDepHour":8,"DayOfWeek":4,"Carrier":"MQ","DestAirportCode":"MCO","WindSpeed":3,"SeaLevelPressure":31.03,"HourlyPrecip":0}

# package the inputs into a JSON string and test run() in local notebook
inputs = pd.DataFrame([test_input1, test_input2])

# COMMAND ----------

# MAGIC %md Fill in the values for `url` and `personal_access_token` in the function below and then run the following command to ensure that you get back results from the serving cluster.  The `url` is the model serving URL you created in the **Serve the Model** task above, and `personal_access_token` is the PAT you created in the hands-on lab.

# COMMAND ----------

import os
import requests
import numpy as np

def create_tf_serving_json(data):
  return {'inputs': {name: data[name].tolist() for name in data.keys()} if isinstance(data, dict) else data.tolist()}

def score_model(dataset):
  url = '<databricks url>/model/Delay%20Estimator/Production/invocations' # Enter your URL here
  personal_access_token = '<presonal access token>' # Enter your Personal Access Token here
  headers = {'Authorization': f'Bearer {personal_access_token}'}
  data_json = dataset.to_dict(orient='split') if isinstance(dataset, pd.DataFrame) else create_tf_serving_json(dataset)
  response = requests.request(method='POST', headers=headers, url=url, json=data_json)
  if response.status_code != 200:
    raise Exception(f'Request failed with status {response.status_code}, {response.text}')
  return response.json()

score_model(inputs)

# COMMAND ----------

# MAGIC %md # You are done!

# COMMAND ----------

# MAGIC %md Congratulations, you have completed this team challenge!
# MAGIC 
# MAGIC Please continue on to Exercise 3 in the hands-on lab document.
