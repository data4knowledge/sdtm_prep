import requests
from utility.service_environment import ServiceEnvironment

class CTService():

  def __init__(self):
    self.__api_url = ServiceEnvironment().get("CT_SERVER_URL")
  
  def term_reference(self, cl, cli):
    return self.api_get("v1/terms?codelist=%s&term=%s" % (cl, cli))

  def api_get(self, url):
    headers =  {"Content-Type":"application/json"}
    full_url = "%s%s" % (self.__api_url, url)
    response = requests.get(full_url, headers=headers)
    if response.status_code == 200:
      return response.json()
    else:
      return None