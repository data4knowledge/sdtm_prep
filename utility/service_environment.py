from dotenv import load_dotenv
import os

class ServiceEnvironment():
  
  def __init__(self):
    self.load()

  def environment(self):
    if 'PYTHON_ENVIRONMENT' in os.environ:
      return os.environ['PYTHON_ENVIRONMENT']
    else:
      return "development"

  def get(self, name):
    full_name = self.build_full_name(name)
    if full_name in os.environ:
      return os.environ[full_name]
    else:
      print("ENV GET: Missing")
      return ""

  def build_full_name(self, name):
    return "%s_%s" % (self.environment().upper(), name)

  def load(self):
    load_dotenv(".%s_env" % self.environment())
