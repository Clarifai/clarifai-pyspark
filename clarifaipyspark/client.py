from clarifai.client.base import BaseClient
from clarifai.client.user import User
from clarifai.client.app import App


class ClarifaiPySpark(BaseClient):

  def __init__(self, user_id, app_id):
    self.user = User(user_id=user_id)
    self.app = App(app_id=app_id)
    #Inputs object - for listannotations
    #input_obj = User(user_id="user_id").app(app_id="app_id").inputs()
    self.user_id = user_id
    self.app_id = app_id
    super().__init__(user_id=user_id, app_id=app_id)


  def dataset(self, dataset_id):
    self.dataset_id = dataset_id
    try:
      self.dataset =  self.app.dataset(dataset_id=dataset_id)
    except:
      print("Creating a new dataset")
      self.dataset = self.app.create_dataset(dataset_id=dataset_id)
    return self.dataset