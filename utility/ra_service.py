import requests
from utility.service_environment import ServiceEnvironment

class RaService():

  def __init__(self):
    self.__api_url = ServiceEnvironment().get("RA_SERVER_URL")
  
  def namespace_by_name(self, name):
    return self.api_get("v1/namespaces?name=%s" % (name))

  def registration_authority_by_namespace_uuid(self, uuid):
    return self.api_get("v1/registrationAuthorities?namespace=%s" % (uuid))

  def api_get(self, url):
    headers =  {"Content-Type":"application/json"}
    response = requests.get("%s%s" % (self.__api_url, url), headers=headers)
    return response.json()