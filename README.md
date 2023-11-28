# ClarifaiPySpark


## Introduction

This readme provides overview of the Software Development Kit (SDK) under development for integrating Clarifai with Databricks. The primary use case for this SDK is to facilitate the interaction between Databricks and Clarifai for tasks related to uploading client datasets, annotating data, and exporting and storing annotations in Spark DataFrames or Delta tables.

![Screenshot 2023-11-17 at 5 21 04 PM](https://github.com/Clarifai/clarifai-pyspark/assets/143642606/7b6bfc6a-19b9-48d7-8013-24e79fc5aacf)

The initial use case for this SDK revolves around three main objectives:

### Uploading Client Datasets into Clarifai App:
  The SDK should enable the seamless upload of datasets into the Clarifai application, simplifying the process of data transfer from Databricks to Clarifai.

### Annotate the Data:
  It should provide features for data annotation, making it easier for users to add labels and metadata to their datasets within the Clarifai platform.

### Export Annotations to Spark DataFrames/Delta Tables:
  The SDK should offer functionality to export annotations and store them in Spark DataFrames or Delta tables, facilitating further data analysis within Databricks.

## Requirements:
  * Databricks : Runtime 13.3 or later
  * Clarifai : ``` pip install clarifai ```
  * Create your [Clarifai account](https://clarifai.com/login)
  * Follow the instructions to get your own [Clarifai PAT](https://docs.clarifai.com/clarifai-basics/authentication/personal-access-tokens)
  * Protocol Buffers : version 4.24.2 `pip install protobuf==4.24.2 `

## Setup:

Install the package and initialize the clarifaipyspark class to begin.
```bash
pip install clarifai-pyspark
```
## Getting Started:
``` python
from clarifaipyspark.client import ClarifaiPySpark
```

Create a Clarifai-PySpark client object to connect to your app on Clarifai. You can also choose the dataset or create one in your clarifai app to upload the data.
``` python
claps_obj = ClarifaiPySpark(user_id=USER_ID, app_id=APP_ID, pat=CLARIFAI_PAT)
dataset_obj = claps_obj.dataset(dataset_id=DATASET_ID)
```
## Examples:
Checkout these notebooks for various operations you can perform using clarifai-pyspark SDK.
| Notebook | **Description** |  GitHub |
|----------|--------|---------------- |
| ClarifaiPyspark_Example_NB | An extensive notebook which walks through the journey from data ingestion to exporting annotations | [![GitHub](https://img.shields.io/badge/GitHub-Link-blue?logo=github)]((https://github.com/Clarifai/clarifai-pyspark/blob/main/examples/ClarifaiPyspark_Example_NB.ipynb)) |
| export_to_df_demo | Explains the process of exporting annotations from clarifai app and storing it as dataframe in databricks |  [![GitHub](https://img.shields.io/badge/GitHub-Link-blue?logo=github)]((https://github.com/Clarifai/clarifai-pyspark/blob/main/examples/export_to_df_demo.ipynb)) |

##
If you want to enhance your AI journey with workflows and leveraging custom models (programmatically) our [Clarifai SDK](https://docs.clarifai.com/python-sdk/tutorial) might be good place to start with.
Please refer below resources for further references. 
* Docs - [Clarifai Docs](https://docs.clarifai.com)
* Explore our community page - [Clarifai Community](https://clarifai.com/explore)
* Fork and contribute to our SDK here ! [![GitHub](https://img.shields.io/badge/GitHub-Link-blue?logo=github)](https://github.com/Clarifai/clarifai-python)
* Reach out to us on socials [![Discord](https://img.shields.io/discord/your_server_id?label=Discord&logo=discord&style=flat-square)](https://discord.com/invite/WgUvPK4pVD) 


