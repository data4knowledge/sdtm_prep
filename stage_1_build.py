import os
import requests
from utility.utility import *
from utility.ra_service import *
from utility.crm_service import *
from utility.ct_service import *
from utility.bc_service import *
from stringcase import pascalcase, snakecase
import yaml

nodes = { 
  "ImplementationGuide": [], "Domain": [], "Variable": [], 
  'ScopedIdentifier': [], 'Namespace': [], 'RegistrationStatus': [], 'RegistrationAuthority': [], 'BiomedicalConceptRef': [] 
}
relationships = { 
  "HAS_DOMAIN": [], "HAS_VARIABLE": [], "USING_BC": [], "BC_RESTRICTION": [],
  "IDENTIFIED_BY": [], "HAS_STATUS": [], "SCOPED_BY": [], "MANAGED_BY": [],
}

bc_domain_map = {}
bc_variable_map = {}
bc_set = {}

delete_dir("load_data")

bc_service = BCService()
with open("source_data//bc_crm.yaml") as file:
  root = yaml.load(file, Loader=yaml.FullLoader)
  print("ROOT:", root)
  for item in root["root"]:
    print("ITEM:", item)
    if "domain" in item:
      domain = item['domain']
      if not domain in bc_domain_map:
        bc_domain_map[domain] = []
      if not domain in bc_variable_map:
        bc_variable_map[domain] = {}
      for name in item["bcs"]:
        bc = bc_service.biomedical_concept(name)
        bc_domain_map[domain].append(name)
        bc_set[name] = bc['items'][0]['uri']
      for variable in item["variables"]:
        bc_name = variable['bc']
        variable_name = variable['name']
        if bc_name in bc_set:
          bc_variable_map[domain][variable_name] = bc_name
        else:
          print("***** Missing BC reference %s for %s *****" % (bc_name, variable_name))

    #elif "class" in item:
print("BC DOMAIN MAP:", bc_domain_map)
print("BC VARIABLE MAP:", bc_variable_map)
print("BC SET:", bc_set)
for name, uri in bc_set.items():
  nodes["BiomedicalConceptRef"].append({"name": name, "uri_reference": uri, "uuid": uuid4() })

# Namespace and RA
ns_s_json = RaService().namespace_by_name("d4k SDTM namespace")
ra_s_json = RaService().registration_authority_by_namespace_uuid(ns_s_json['uuid'])

# CT Service API
ct_service = CTService()

# Get API key. Uses an environment variable.
API_KEY = os.getenv('CDISC_API_KEY')
headers =  {"Content-Type":"application/json", "api-key": API_KEY}

# Get SDTM IG 3.4
api_url = "https://api.library.cdisc.org/api/mdr/sdtmig/3-4/datasets?expand=false"
response = requests.get(api_url, headers=headers)
ig_body = response.json()

# Process IG
base_uri = ns_s_json['value']
print("BASE_URI", base_uri)
ig_uri = extend_uri(base_uri, "ig")
nodes["ImplementationGuide"].append({"name": ig_body['label'], "uri": ig_uri, "uuid": uuid4() })
add_identifier_and_status(ig_uri, "SDTM IG", "2022-09-01", ns_s_json['uri'], ra_s_json['uri'], nodes, relationships)

# Process Domains
for ds in ig_body['_links']['datasets']:
  try:
    
    # Get the href to get the dataset API call.
    api_url = "https://api.library.cdisc.org/api%s" % (ds['href'])
    response = requests.get(api_url, headers=headers)
    ds_body = response.json()
    domain = ds_body['name']
    print("Domain:", domain)

    # Process the dataset
    domain_uri = extend_uri(ig_uri, domain)
    record = { 
      'uri': domain_uri,
      'uuid': uuid4(),
      'label': ds_body['label'], 
      'name': ds_body['name'], 
      'structure': ds_body['datasetStructure'], 
      'ordinal': ds_body['ordinal'] 
    }
    if domain in bc_domain_map:
      for bc in bc_domain_map[domain]:
        uri = bc_set[bc]
        relationships["USING_BC"].append({"from": domain_uri, "to": uri})
    if 'description' in ds:
      record['description'] = ds_body['description']
    else:
      record['description'] = ds_body['label']
    nodes["Domain"].append(record)
    add_identifier_and_status(domain_uri, "SDTM %s" % (domain), "2022-09-01", ns_s_json['uri'], ra_s_json['uri'], nodes, relationships)
    relationships["HAS_DOMAIN"].append({"from": ig_uri, "to": domain_uri})
    for item in ds_body['datasetVariables']:
      try:
        variable_name = item['name']
        variable_uri = extend_uri(domain_uri, item['name'])
        record = { 
          'uri': variable_uri,
          'uuid': uuid4(),
          'label': item['label'], 
          'name': variable_name, 
          'ordinal': int(item['ordinal']),
          'description': item['description'],
          'data_type': item['simpleDatatype'], 
          'role': item['role'], 
          'core': item['core'],
          'value_domain': '',
          'code_list': '',
          'code_list_uri': '',
        }
        if 'describedValueDomain' in item:
          record['value_domain'] = item['describedValueDomain']
        if 'codelist' in item['_links']:
          parts = item['_links']['codelist'][0]['href'].split('/')
          identifier = parts[-1]
          cl = ct_service.get_cl(identifier)
          record['code_list'] = identifier
          record['code_list_uri'] = cl['uri']
        if domain in bc_variable_map:
          if variable_name in bc_variable_map[domain]:
            bc_name = bc_variable_map[domain][variable_name]
            if bc_name in bc_set:
              uri = bc_set[bc_name]
              relationships["BC_RESTRICTION"].append({"from": variable_uri, "to": uri})
        nodes["Variable"].append(record)
        relationships["HAS_VARIABLE"].append({"from": domain_uri, "to": variable_uri})

      except:
        print("********** VARIABLE ***********")
        print(item)
        raise

  except:
    print("********** DATASET ***********")
    print(ds)
    raise

for k, v in nodes.items():
  csv_filename = "load_data/node-%s-1.csv" % (snakecase(k))
  write_nodes(v, csv_filename)

for k, v in relationships.items():
  csv_filename = "load_data/relationship-%s-1.csv" % (k.lower())
  write_relationships(v, csv_filename)

