from dotenv import load_dotenv
import os

class ServiceEnvironment():
  
  load_dotenv('.development_test_env')

  @classmethod
  def environment(cls):
    if 'PYTHON_ENVIRONMENT' in os.environ:
      return os.environ['PYTHON_ENVIRONMENT']
    else:
      return "development"

  @classmethod
  def get(cls, name):
    full_name = cls.build_full_name(name)
    if full_name in os.environ:
      return os.environ[full_name]
    else:
      return ""

  @classmethod
  def build_full_name(cls, name):
    return "%s_%s" % (cls.environment().upper(), name)