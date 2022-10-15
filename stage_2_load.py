from neo4j import GraphDatabase
import os
import glob
from stringcase import pascalcase, snakecase
from utility.service_environment import ServiceEnvironment

def file_load(driver, database):
  project_root = os.path.abspath(os.path.dirname(__file__))
  load_files = []
  for filename in glob.glob("load_data/*.csv"):
    parts = filename.replace("load_data/", "").split("-")
    file_path = os.path.join(project_root, filename)
    print(file_path)
    if parts[0] == "node":
      load_files.append({ "label": pascalcase(parts[1]), "filename": file_path })
    else:
      load_files.append({ "type": parts[1].upper(), "filename": file_path })
  result = None
  session = driver.session(database=database)
  nodes = []
  relationships = []
  for file_item in load_files:
    if "label" in file_item:
      nodes.append("{ fileName: '%s', labels: ['%s'] }" % (file_item["filename"], file_item["label"]) )
    else:
      relationships.append("{ fileName: '%s', type: '%s' }" % (file_item["filename"], file_item["type"]) )
  query = """CALL apoc.import.csv( [%s], [%s], {stringIds: false})""" % (", ".join(nodes), ", ".join(relationships))
  result = session.run(query)
  for record in result:
    #print(record)
    return_value = {'nodes': record['nodes'], 'relationships': record['relationships'], 'time': record['time']}
  query = """
    MATCH (n:Domain)-[]->(m:Variable)
    set n.ordinal = toInteger(n.ordinal)
    set m.ordinal = toInteger(m.ordinal)
  """
  result = session.run(query)
  driver.close()
  return return_value

def clear(tx):
  tx.run("CALL apoc.periodic.iterate('MATCH (n) WHERE NOT n:`_Neodash_Dashboard` RETURN n', 'DETACH DELETE n', {batchSize:1000})")

def clear_neo4j(driver, database):
  with driver.session(database=database) as session:
    session.write_transaction(clear)
  driver.close()

db_name = ServiceEnvironment().get('NEO4J_DB_NAME')
url = ServiceEnvironment().get('NEO4J_URL')
usr = ServiceEnvironment().get('NEO4J_USER')
pwd = ServiceEnvironment().get('NEO4J_PWD')
driver = GraphDatabase.driver(url, auth=(usr, pwd))

print("Deleting database ...")
clear_neo4j(driver, db_name)
print("Database deleted. Load new data ...")
result = file_load(driver, db_name)
print("Load complete. %s nodes and %s relationships loaded in %s milliseconds." % (result['nodes'], result['relationships'], result['time']))

