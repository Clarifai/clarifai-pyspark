# Databricks workflow (job) for ingestion of unstructured data from Databricks into Clarifai 

## 1. Clone the notebook script 
 To start with setting up Databricks workflow, we will need a source script file for the job. There are several ways to set up the Databricks workflow source,
 Kindly refer this [Create and run Jobs](https://docs.databricks.com/en/workflows/jobs/create-run-jobs.html).

 * The notebook script can be found inside the below folder
   
 ```
clarifai-pyspark/
        ├── Job_script
               └── image_ingestion_clarifai_job_nb.ipynb
```
* Create a notebook inside your databricks workspace like below
  
    ![Screenshot 2024-01-03 at 11 51 50 PM](https://github.com/Clarifai/clarifai-pyspark/assets/143642606/5e9a298b-73c9-493c-b772-6aa70fcf51b5)

## 2. Create a Job from workflows

Next step we need to create a databricks workflow jobs with some parameters and packages in order to call the job from our frontend UI module for data ingestion.

   ![Screenshot 2024-01-03 at 11 57 39 PM](https://github.com/Clarifai/clarifai-pyspark/assets/143642606/e91faa3e-2c49-44ef-b6fc-bb9ea860611a)

* **Task name** :
    Give desired job name.
  
* **Type** :
    Here the type refers to the Source type of your job script, you have different options based on your preference. For the purpose of our use case we have gone with Notebook as script.
    But you can also use below types, find more information here on official [databricks documentaion](https://docs.databricks.com/en/workflows/jobs/create-run-jobs.html#task-type-options) on how to configure different type,
  
    ![Screenshot 2024-01-04 at 12 01 31 AM](https://github.com/Clarifai/clarifai-pyspark/assets/143642606/1264a150-2262-4b18-91d0-13885936f4a0)

* **Source** :
   The source where the script is located, you can either use an notebook which is in your workspace or you can reference it from remote git repo. Check out here on how to [configure remote github repo](https://docs.databricks.com/en/workflows/jobs/how-to/use-repos.html#use-a-notebook-from-a-remote-git-repository).

  
* **Path** :
  Path of the script within workspace/github .

* **Cluster** :
   Select one of the clusters for compute purpose.

* **Dependant libraries** :
    Next as an important step, we need to configure the dependant libraries which is used inside the script, for our use case select PyPi as shown below and configure the package with latest release version.
  ```
  clarifai-pyspark==0.0.3 (or latest version)
  ```

  ![Screenshot 2024-01-04 at 12 14 49 AM](https://github.com/Clarifai/clarifai-pyspark/assets/143642606/fee5828f-ace3-41f7-a36f-dc7d12f48284)

* **Parameters** :
   For the final part, we need to set some parameters for our job to run. These are the four mandatory parameters that are passed from our UI module and gets updated into job to run the function inside script.
   Add parameters as key and leave the value section blank.
   
   ![Screenshot 2024-01-04 at 12 21 26 AM](https://github.com/Clarifai/clarifai-pyspark/assets/143642606/4424b66f-dc78-4751-b05a-80b4473a5247)

## 3. Use job_id in the UI module for ingestion.

Now for the application part, we need to note the job_id that we have created using above steps and pass it in the UI module whenever we are ingesting data from databricks to clarifai.

![Screenshot 2024-01-04 at 12 24 03 AM](https://github.com/Clarifai/clarifai-pyspark/assets/143642606/489d47d5-0070-4bd8-accb-060b60620a9b)





