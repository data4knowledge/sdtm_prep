import requests
from utility.service_environment import ServiceEnvironment

class CTService():

  def __init__(self):
    self.__api_url = ServiceEnvironment().get("CT_SERVER_URL")
  
  def get_cl(self, cl):
    return self.api_get("v1/codeLists?identifier=%s" % (cl))

  def api_get(self, url):
    headers =  {"Content-Type":"application/json"}
    full_url = "%s%s" % (self.__api_url, url)
    #print(f"CT_SERVICE: {full_url}")
    response = requests.get(full_url, headers=headers)
    if response.status_code == 200:
      return response.json()
    else:
      return None