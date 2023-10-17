from clarifai.client.base import BaseClient
from clarifai.client.dataset import Dataset
from clarifai.client.model import Model
from clarifai.client.user import User
from clarifai.client.app import App
from clarifai.client.input import Inputs
from pyspark.sql import SparkSession
from google.protobuf.json_format import MessageToJson


class Dataset(Dataset):

  def __init__(self, user_id, app_id, dataset_id):
    self.user = User(user_id=user_id)
    self.app = App(app_id=app_id)
    #Inputs object - for listannotations
    #input_obj = User(user_id="user_id").app(app_id="app_id").inputs()
    self.user_id = user_id
    self.app_id = app_id
    self.dataset_id = dataset_id
    super().__init__(user_id=user_id, app_id=app_id, dataset_id=dataset_id)


  def upload_dataset_from_csv(self, csv_path: str,
                      input_type: str = 'text',
                      csv_type: str = None,
                      labels: bool = True,
                      chunk_size: int = 128) -> None:
    ### Accepted csv format - input, label
    ### TODO: Can input column names & extreact them to convert to our csv format
    super().upload_from_csv(csv_path=csv_path, input_type=input_type, csv_type=csv_type, labels=labels, chunk_size=chunk_size)


  def upload_from_folder(self,
                         folder_path: str,
                         input_type: str,
                         labels: bool = False,
                         chunk_size: int = 128) -> None:
    ### Can provide a volume or S3 path to the folder
    ### If label is true, then folder name is class name (label)
    super().upload_from_folder(folder_path=folder_path, input_type=input_type, labels=labels, chunk_size=chunk_size)


  def upload_dataset_from_dataloader(self,
                                    task: str,
                                    split: str,
                                    module_dir: str = None,
                                    dataset_loader: str = None,
                                    chunk_size: int = 128) -> None:
    super().upload_dataset(task, split, module_dir, dataset_loader, chunk_size)


  def upload_dataset_from_table(self,
                                table_path: str,
                                task: str,
                                split: str,
                                input_type: str,
                                table_type: str,
                                labels: bool,
                                module_dir: str = None,
                                dataset_loader = None,
                                chunk_size: int = 128) -> None:
    ### Accepted csv format - input, label
    ### TODO: dataframe dataloader template
    ### TODO: Can input column names & extreact them to convert to our csv format
    if dataset_loader:
      super().upload_dataset(task, split, module_dir, dataset_loader, chunk_size)
    else:
      # df = sqlContext.table(table_path)
      csv_path = "./"
      # df.write.option("header", True).option("delimiter",",").csv(csv_path)
      spark = SparkSession.builder.appName('Clarifai-spark').getOrCreate()
      df_delta = spark.read.format("delta").load(table_path)
      df_delta.createTempView("temporary_view")
      # convert the temporary view into a CSV file
      csv_path = table_path.replace(".delta",".csv")
      spark.sql("SELECT * FROM temporary_view").write.option("header", "true").format("csv").save(csv_path)
      super().upload_from_csv(csv_path=csv_path, input_type=input_type, csv_type=table_type, labels=labels, chunk_size=chunk_size)

                                
  def list_inputs_from_dataset(self,
                               dataset_id: str = None,
                               per_page: int = None,
                               input_type: str = None):
    input_obj = Inputs(user_id=self.user_id, app_id=self.app_id)
    return list(input_obj.list_inputs(dataset_id=self.dataset_id,input_type=input_type,per_page=per_page))


  def list_annotations(self,
                       dataset_id: str = None,
                       per_page: int = None,
                       input_type: str = None):
    ### input_ids: list of input_ids for which user wants annotations
    input_obj = Inputs(user_id=self.user_id, app_id=self.app_id)
    all_inputs = list(input_obj.list_inputs(dataset_id=self.dataset_id,input_type=input_type,per_page=per_page))
    return list(input_obj.list_annotations(batch_input=all_inputs))


  def export_annotations_to_dataframe(self):
    annotation_list = []
    spark = SparkSession.builder.appName('Clarifai-spark').getOrCreate()
    input_obj = Inputs(user_id=self.user_id,app_id=self.app_id)
    response = input_obj.list_annotations(batch_input=all_inputs)
    annotation_list.append(MessageToJson(response.annotations))
    #Need to get the details of rest of the columns (ID, URL), annotations should be appended along with the existing dataframe created while dataset upload
    df = spark.createDataFrame(data)
    return df