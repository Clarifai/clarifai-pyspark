from clarifai.client.base import BaseClient
from clarifai.client.app import App

from clarifaipyspark.dataset import Dataset


class ClarifaiPySpark(BaseClient):
  """
  ClarifaiPySpark inherits the BaseClient class from the clarifai SDK and
  it initializes the client.
  """

  def __init__(self, user_id: str, app_id: str, pat: str = None):
    """Initializes clarifai client object.

    Args:
      - user_id (str): A user ID for authentication.
      - app_id (str): An app ID for the application to interact with.
      - pat (str): A personal access token for authentication. Can be set as env var CLARIFAI_PAT
    """
    self.user_id = user_id
    self.app_id = app_id
    self.app = App(user_id=user_id, app_id=app_id, pat=pat)
    super().__init__(user_id=user_id, app_id=app_id, pat=pat)

  def dataset(self, dataset_id):
    """Initializes the dataset method with dataset_id.

    Args:
      dataset_id: The dataset_id within the user app.

    Returns:
      Dataset object for the dataset_id.
    """

    try:
      self.app.dataset(dataset_id=dataset_id)
    except:
      print("Creating a new dataset")
      self.app.create_dataset(dataset_id=dataset_id)

    return Dataset(dataset_id=dataset_id, user_id=self.user_id, app_id=self.app_id, pat=self.pat)
