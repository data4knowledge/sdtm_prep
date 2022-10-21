import requests
from utility.service_environment import ServiceEnvironment

class BCService():

  def __init__(self):
    self.__api_url = ServiceEnvironment().get("BC_SERVER_URL")
  
  def biomedical_concept(self, name):
    return self.api_get("v1/biomedicalConcepts?filter=%s" % (name))

  def api_get(self, url):
    headers =  {"Content-Type":"application/json"}
    full_url = "%s%s" % (self.__api_url, url)
    response = requests.get(full_url, headers=headers)
    if response.status_code == 200:
      return response.json()
    else:
      return None