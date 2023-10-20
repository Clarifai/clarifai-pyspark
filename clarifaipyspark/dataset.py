import json
import uuid
from typing import List
import os
import time
import requests
from clarifai.client.app import App
from clarifai.client.dataset import Dataset
from clarifai.client.input import Inputs
from clarifai.client.user import User
from clarifai.errors import UserError
from clarifai_grpc.grpc.api.resources_pb2 import Text
from google.protobuf.json_format import MessageToJson
from google.protobuf.struct_pb2 import Struct
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession


class Dataset(Dataset):
  """Dataset class provides information about dataset of the app and it inherits from the clarifai SDK Dataset class."""

  def __init__(self, user_id: str = "", app_id: str = "", dataset_id: str = ""):
    """Initializes the Dataset object.

    Args:
        user_id (str): The clarifai user ID of the user.
        app_id (str): Clarifai App ID.
        dataset_id (str): Dataset ID of the dataset inside the clarifai App.

    Example: TODO
    """
    self.user = User(user_id=user_id)
    self.app = App(app_id=app_id)
    #Inputs object - for listannotations
    #input_obj = User(user_id="user_id").app(app_id="app_id").inputs()
    self.user_id = user_id
    self.app_id = app_id
    self.dataset_id = dataset_id
    super().__init__(user_id=user_id, app_id=app_id, dataset_id=dataset_id)

  def upload_dataset_from_csv(self,
                              csv_path: str = "",
                              input_type: str = 'text',
                              csv_type: str = None,
                              labels: bool = True,
                              chunk_size: int = 128) -> None:
    """Uploads dataset into clarifai app from the csv file path.

    Args:
        csv_path (str): CSV file path of the dataset to be uploaded into clarifai App.
        input_type (str): Input type of the dataset whether (Image, text).
        csv_type (str): Type of the csv file contents(url, raw, filepath).
        labels (bool): Give True if labels column present in dataset else False.
        chunk_size (int): chunk size of parallel uploads of inputs and annotations.

    Example: TODO

    Note:
        CSV file supports 'inputid', 'input', 'concepts', 'metadata', 'geopoints' columns.
        All the data in the CSV should be in double quotes.
        metadata should be in single quotes format. Example: "{'key': 'value'}"
        geopoints should be in "long,lat" format.

    """
    ### TODO: Can input column names & extract them to convert to our csv format
    self.upload_from_csv(
        csv_path=csv_path,
        input_type=input_type,
        csv_type=csv_type,
        labels=labels,
        chunk_size=chunk_size)

  def upload_dataset_from_folder(self,
                                 folder_path: str,
                                 input_type: str,
                                 labels: bool = False,
                                 chunk_size: int = 128) -> None:
    """Uploads dataset from folder into clarifai app.

    Args:
        folder_path (str): folder path of the dataset to be uploaded into clarifai App.
        input_type (str): Input type of the dataset whether (Image, text).
        labels (bool): Give True if labels column present in dataset else False.
        chunk_size (int): chunk size of parallel uploads of inputs and annotations.

    Example: TODO

    Note:
        Can provide a volume or S3 path to the folder
        If label is true, then folder name is class name (label)
    """

    self.upload_from_folder(
        folder_path=folder_path, input_type=input_type, labels=labels, chunk_size=chunk_size)

  def get_inputs_from_dataframe(self,
                                dataframe,
                                input_type: str,
                                df_type: str,
                                dataset_id: str = None,
                                labels: str = True) -> List[Text]:
    input_protos = []
    input_obj = Inputs(user_id=self.user_id, app_id=self.app_id)

    for row in dataframe.collect():
      if labels:
        labels_list = row["concepts"].split(',')
        labels = labels_list if len(row['concepts']) > 0 else None
      else:
        labels = None

      if 'metadata' in dataframe.columns:
        if row['metadata'] is not None and len(row['metadata']) > 0:
          metadata_str = row['metadata'].replace("'", '"')
          try:
            metadata_dict = json.loads(metadata_str)
          except json.decoder.JSONDecodeError:
            raise UserError("metadata column in CSV file should be a valid json")
          metadata = Struct()
          metadata.update(metadata_dict)
        else:
          metadata = None
      else:
        metadata = None

      if 'geopoints' in dataframe.columns:
        if row['geopoints'] is not None and len(row['geopoints']) > 0:
          geo_points = row['geopoints'].split(',')
          geo_points = [float(geo_point) for geo_point in geo_points]
          geo_info = geo_points if len(geo_points) == 2 else UserError(
              "geopoints column in CSV file should have longitude,latitude")
        else:
          geo_info = None
      else:
        geo_info = None

      input_id = row['inputid'] if 'inputid' in dataframe.columns else uuid.uuid4().hex
      text = row["input"] if input_type == 'text' else None
      image = row['input'] if input_type == 'image' else None
      video = row['input'] if input_type == 'video' else None
      audio = row['input'] if input_type == 'audio' else None

      if df_type == 'raw':
        input_protos.append(
            input_obj.get_text_input(
                input_id=input_id,
                raw_text=text,
                dataset_id=dataset_id,
                labels=labels,
                geo_info=geo_info))
      elif df_type == 'url':
        input_protos.append(
            input_obj.get_input_from_url(
                input_id=input_id,
                image_url=image,
                text_url=text,
                audio_url=audio,
                video_url=video,
                dataset_id=dataset_id,
                labels=labels,
                geo_info=geo_info))
      else:
        input_protos.append(
            input_obj.get_input_from_file(
                input_id=input_id,
                image_file=image,
                text_file=text,
                audio_file=audio,
                video_file=video,
                dataset_id=dataset_id,
                labels=labels,
                geo_info=geo_info))

    return input_protos

  def upload_dataset_from_dataframe(self,
                            dataframe,
                            input_type: str,
                            df_type: str = None,
                            labels: bool = True,
                            chunk_size: int = 128) -> None:

    if input_type not in ['image', 'text', 'video', 'audio']:
      raise UserError('Invalid input type, it should be image,text,audio or video')

    if df_type not in ['raw', 'url', 'file_path']:
      raise UserError('Invalid csv type, it should be raw, url or file_path')

    if df_type == 'raw' and input_type != 'text':
      raise UserError('Only text input type is supported for raw csv type')

    if not isinstance(dataframe, SparkDataFrame):
      raise UserError('dataframe should be a Spark DataFrame')

    chunk_size = min(128, chunk_size)
    input_obj = input_obj = Inputs(user_id=self.user_id, app_id=self.app_id)
    input_protos = self.get_inputs_from_dataframe(
        dataframe=dataframe,
        df_type=df_type,
        input_type=input_type,
        dataset_id=self.dataset_id,
        labels=labels)
    return (input_obj._bulk_upload(inputs=input_protos, chunk_size=chunk_size))

  def upload_dataset_from_dataloader(self,
                                     task: str,
                                     split: str,
                                     module_dir: str = None,
                                     dataset_loader: str = None,
                                     chunk_size: int = 128) -> None:
    """Uploads dataset using a dataloader function for custom formats.

    Args:
        task (str): task type(text_clf, visual-classification, visual_detection, visual_segmentation, visual-captioning).
        split (str): split type(train, test, val).
        module_dir (str): path to the module directory.
        dataset_loader (str): name of the dataset loader.
        chunk_size (int): chunk size for concurrent upload of inputs and annotations.

    Example: TODO
    """
    self.upload_dataset(task, split, module_dir, dataset_loader, chunk_size)

  def upload_dataset_from_table(self,
                                table_path: str,
                                task: str,
                                split: str,
                                input_type: str,
                                table_type: str,
                                labels: bool,
                                module_dir: str = None,
                                dataset_loader=None,
                                chunk_size: int = 128) -> None:
    """upload dataset into clarifai app from spark tables.

    Args:
        table_path (str): path of the table to be uploaded.
        task (str):
        split (str):
        input_type (str): Input type of the dataset whether (Image, text).
        table_type (str): Type of the table contents (url, raw, filepath).
        labels (bool): Give True if labels column present in dataset else False.
        module_dir (str): path to the module directory.
        dataset_loader (str): name of the dataset loader.
        chunk_size (int): chunk size for concurrent upload of inputs and annotations.
    Note:
        Accepted csv format - input, label
        TODO: dataframe dataloader template
        TODO: Can input column names & extreact them to convert to our csv format
    """

    if dataset_loader:
      self.upload_dataset(task, split, module_dir, dataset_loader, chunk_size)

    else:
      # df = sqlContext.table(table_path)
      csv_path = "./"
      # df.write.option("header", True).option("delimiter",",").csv(csv_path)
      spark = SparkSession.builder.appName('Clarifai-spark').getOrCreate()
      df_delta = spark.read.format("delta").load(table_path)
      df_delta.createTempView("temporary_view")
      # convert the temporary view into a CSV file
      csv_path = table_path.replace(".delta", ".csv")
      spark.sql("SELECT * FROM temporary_view").write.option("header",
                                                             "true").format("csv").save(csv_path)
      self.upload_from_csv(
          csv_path=csv_path,
          input_type=input_type,
          csv_type=table_type,
          labels=labels,
          chunk_size=chunk_size)

  def list_inputs_from_dataset(self, per_page: int = None, input_type: str = None):
    """Lists all the inputs from the app.

    Args:
        dataset_id (str): dataset_id of which the inputs needs to be listed.
        per_page (str): No of response of inputs per page.
        input_type (str): Input type that needs to be displayed (text,image)
        TODO: Do we need input_type ?, since in our case it is image, so probably we can go with default value of "image".

    Examples:
        TODO

    Returns:
        list of inputs.
        """
    input_obj = Inputs(user_id=self.user_id, app_id=self.app_id)
    return list(
        input_obj.list_inputs(
            dataset_id=self.dataset_id, input_type=input_type, per_page=per_page))

  def list_annotations(self, per_page: int = None, input_type: str = None):
    """Lists all the annotations for the inputs in the dataset of a clarifai app.

    Args:
        dataset_id (str): dataset_id of which the inputs needs to be listed.
        per_page (str): No of response of inputs per page.
        input_type (str): Input type that needs to be displayed (text,image)
        TODO: Do we need input_type ?, since in our case it is image, so probably we can go with default value of "image".

    Examples:
        TODO

    Returns:
        list of annotations.
    """
    ### input_ids: list of input_ids for which user wants annotations
    input_obj = Inputs(user_id=self.user_id, app_id=self.app_id)
    all_inputs = list(
        input_obj.list_inputs(
            dataset_id=self.dataset_id, input_type=input_type, per_page=per_page))
    return list(input_obj.list_annotations(batch_input=all_inputs))

  def export_annotations_to_dataframe(self):
    """Export all the annotations from clarifai App into spark dataframe.

    Examples:
        TODO

    Returns:
        spark dataframe with annotations"""

    annotation_list = []
    spark = SparkSession.builder.appName('Clarifai-spark').getOrCreate()
    input_obj = Inputs(user_id=self.user_id, app_id=self.app_id)
    all_inputs = list(input_obj.list_inputs(dataset_id=self.dataset_id))
    response = list(input_obj.list_annotations(batch_input=all_inputs))
    for an in response:
      temp = {}
      temp['annotation'] = json.loads(MessageToJson(an.data))
      if not temp['annotation'] or temp['annotation'] == '{}' or temp['annotation'] == {}:
        continue
      temp['id'] = an.id
      temp['user_id'] = an.user_id
      temp['input_id'] = an.input_id
      created_at = float(f"{an.created_at.seconds}.{an.created_at.nanos}")
      temp['created_at'] = time.strftime('%m/%d/% %H:%M:%5', time.gmtime(created_at))
      modified_at = float(f"{an.modified_at.seconds}.{an.modified_at.nanos}")
      temp['modified_at'] = time.strftime('%m/%d/% %H:%M:%5', time.gmtime(modified_at))
      annotation_list.append(temp)
    df = spark.createDataFrame(annotation_list)
    return df


  def export_images_to_volume(self, path, input_response):
    for resp in input_response:
        imgid = resp.id
        ext = resp.data.image.image_info.format
        url = resp.data.image.url
        img_name = path+'/'+imgid+'.'+ext.lower()
        headers = {
            "Authorization": f"Bearer {os.environ['CLARIFAI_PAT']}"
        }
        response = requests.get(url, headers=headers)
        with open(img_name, "wb") as f:
            f.write(response.content)


  def export_text_to_volume(self, path, input_response):
    for resp in input_response:
        textid = resp.id
        url = resp.data.text.url
        file_name = path+'/'+textid+'.txt'
        enc = resp.data.text.text_info.encoding
        headers = {
            "Authorization": f"Bearer {os.environ['CLARIFAI_PAT']}"
        }
        response = requests.get(url, headers=headers)
        with open(file_name, "a", encoding=enc) as f:
            f.write(response.content.decode())
