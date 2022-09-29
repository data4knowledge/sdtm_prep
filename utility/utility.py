import csv
import os
from os import walk
import pathlib
from uuid import uuid4

uri_and_uuid_to_id = {}
id_number = 1

def format_name(name):
    name = name.lower()
    name = name.replace(" ", "_")
    return name

def extend_uri(base, ext):
  if base[-1] == '/':
    return "%s%s" % (base, ext)
  else:
    return "%s/%s" % (base, ext)

def template_uri(base_uri, name):
  uri_name = format_name(name)
  return extend_uri(base_uri, "bc_template/%s" % (uri_name))
      
def instance_uri(base_uri, name):
  uri_name = format_name(name)
  return extend_uri(base_uri, "bc_instance/%s" % (uri_name)), uri_name

def delete_dir(target_dir):
  files = os.listdir(target_dir)
  for f in files:
    os.remove("%s/%s" % (target_dir, f))
    print("Deleted %s" % (f))

def files_in_dir(target_path):
  result = []
  for (dir_path, dir_names, filenames) in walk(target_path):
    for filename in filenames:
      if pathlib.Path(filename).suffix == ".yaml":
        result.append("%s/%s" % (dir_path, filename))
  #print(result)
  return result

def write_nodes(the_data, csv_filename, id_field="id:ID"):

  global uri_and_uuid_to_id
  global id_number

  if len(the_data) == 0:
    return 
  with open(csv_filename, mode='w', newline='') as csv_file:
    fields = list(the_data[0].keys())
    fieldnames = [id_field] + fields
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, lineterminator="\n")
    writer.writeheader()
    for row in the_data:
      row[id_field] = id_number
      if 'uri' in row:
        uri_and_uuid_to_id[row["uri"]] = id_number
      if 'uuid' in row:
        uri_and_uuid_to_id[row["uuid"]] = id_number
      id_number += 1
      writer.writerow(row)

def write_relationships(the_data, csv_filename, id_field="id:ID"):

  global uuid_to_id

  if len(the_data) == 0:
    return 
  global id_number
  with open(csv_filename, mode='w', newline='') as csv_file:
    fieldnames = [ ":START_ID", ":END_ID" ]
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, lineterminator="\n")
    writer.writeheader()
    for row in the_data:
      #print("File:", csv_filename)
      #print("F:", uri_and_uuid_to_id[row["from"]])
      #print("T:", uri_and_uuid_to_id[row["to"]])
      new_row = { ":START_ID": uri_and_uuid_to_id[row["from"]], ":END_ID": uri_and_uuid_to_id[row["to"]] }
      writer.writerow(new_row)

def add_identifier_and_status(item_uri, identifier, date, ns_uri, ra_uri, nodes, relationships):
  si = { 'version': 1, 'version_label': "1", 'identifier': identifier, 'semantic_version': '1.0.0', 'uuid': str(uuid4()) }
  ns = { 'uri': ns_uri , 'uuid': str(uuid4()) }
  rs = { 'registration_status': "Draft", 'effective_date': date, 'until_date': "", 'uuid': str(uuid4()) }
  ra = { 'uri': ra_uri , 'uuid': str(uuid4()) }
  nodes['ScopedIdentifier'].append(si)
  nodes['Namespace'].append(ns)
  nodes['RegistrationStatus'].append(rs)
  nodes['RegistrationAuthority'].append(ra)
  relationships["IDENTIFIED_BY"].append({"from": item_uri, "to": si['uuid']})
  relationships["HAS_STATUS"].append({"from": item_uri, "to": rs['uuid']})
  relationships["SCOPED_BY"].append({"from": si['uuid'], "to": ns['uuid']})
  relationships["MANAGED_BY"].append({"from": rs['uuid'], "to": ra['uuid']})
