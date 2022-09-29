import yaml
from utility.utility import *
from utility.ra_service import *
from utility.crm_service import *
from utility.ct_service import *
from stringcase import pascalcase, snakecase
from utility.fhir import add_data_type

nodes = { 
  "Template": [], "Instance": [], "TemplateItem": [], "InstanceItem": [], "DataType": [], "DataTypeProperty": [], "ValueSet": [],
  'ScopedIdentifier': [], 'Namespace': [], 'RegistrationStatus': [], 'RegistrationAuthority': [] 
}
relationships = { 
  "BASED_ON": [], "HAS_ITEM": [], "HAS_IDENTIFIER": [], "HAS_QUALIFIER": [], "BC_NARROWER": [], "HAS_DATA_TYPE": [], "HAS_PROPERTY": [], "HAS_RESPONSE": [],
  "IDENTIFIED_BY": [], "HAS_STATUS": [], "SCOPED_BY": [], "MANAGED_BY": [],
}
bc_uri = {}
crm_paths = {}

def process_templates(the_instance_uri, ns_uri, ra_uri):
  with open("source_data/templates/templates.yaml") as file:
    templates = yaml.load(file, Loader=yaml.FullLoader)
    for template in templates:
      the_template_uri = template_uri(the_instance_uri, template["name"])
      #print("Template:", template["name"], the_template_uri)
      nodes["Template"].append({"name": template["name"], "uri": the_template_uri, "uuid": uuid4() })
      add_identifier_and_status(the_template_uri, template["name"].upper(), "2022-09-01", ns_uri, ra_uri, nodes, relationships)
      name = format_name(template["identified_by"]["name"])
      item_uri = "%s/%s" % (the_template_uri, name)
      record = {
        "name": template["identified_by"]["name"], 
        "mandatory": template["identified_by"]["mandatory"],
        "enabled": template["identified_by"]["enabled"],
        "uri": item_uri,
        "uuid": uuid4(),
        "canonical": ""
      }
      if "canonical" in template["identified_by"]:
        record["canonical"] = template["identified_by"]["canonical"]
        #crm_server = CRMServer()
        #result = crm_server.crm_node_data_types(template["identified_by"]["canonical"])
        #crm_paths[template["identified_by"]["canonical"]] = result
      nodes["TemplateItem"].append(record)
      relationships["HAS_ITEM"].append({"from": the_template_uri, "to": item_uri})
      relationships["HAS_IDENTIFIER"].append({"from": the_template_uri, "to": item_uri})

      parent_uri = item_uri
      name = format_name(template["identified_by"]["data_type"][0]["name"])
      item_uri = "%s/%s" % (parent_uri, name)
      record = {
        "name": template["identified_by"]["data_type"][0]["name"],
        "uri": item_uri,
        "uuid": uuid4()
      }
      nodes["DataType"].append(record)
      relationships["HAS_DATA_TYPE"].append({"from": parent_uri, "to": item_uri})

      # Now all the items
      for item in template["has_items"]: 
        name = format_name(item["name"])
        item_uri = "%s/%s" % (the_instance_uri, name)
        record = {
          "name": item["name"], 
          "mandatory": item["mandatory"],
          "enabled": item["enabled"],
          "canonical": "",
          "uri": item_uri,
          "uuid": uuid4()
        }
        if "canonical" in item:
          record["canonical"] = item["canonical"]
          #crm_server = CRMServer()
          #result = crm_server.crm_node_data_types(item["canonical"])
          #print(result)
          #crm_paths[template["identified_by"]["canonical"]] = result
        nodes["TemplateItem"].append(record)
        relationships["HAS_ITEM"].append({"from": the_template_uri, "to": item_uri})
        parent_uri = item_uri
        for data_type in item["data_type"]: 
          name = format_name(data_type["name"])
          item_uri = "%s/%s" % (parent_uri, name)
          record = {
            "name": data_type["name"],
            "uri": item_uri,
            "uuid": uuid4()
          }
          nodes["DataType"].append(record)
          relationships["HAS_DATA_TYPE"].append({"from": parent_uri, "to": item_uri})

def process_instances(base_uri, ns_uri, ra_uri):
  ct_server = CTService()
  files = files_in_dir('source_data/instances')
  for filename in files:
    with open(filename) as file:
      narrower = {}
      instances = yaml.load(file, Loader=yaml.FullLoader)
      for instance in instances:
        #print(instance)
        #print("instance:", instance["name"])
        based_on_uri = template_uri(base_uri, instance["based_on"])
        #print("based on:", based_on_uri)
        the_instance_uri, uri_name = instance_uri(base_uri, instance["name"])
        nodes["Instance"].append({ "name": instance["name"], "uri": the_instance_uri, "uuid": uuid4() })           
        relationships["BASED_ON"].append({"from": the_instance_uri, "to": based_on_uri})
        add_identifier_and_status(the_instance_uri, instance["name"].upper(), "2022-09-01", ns_uri, ra_uri, nodes, relationships)
        bc_uri[uri_name] = the_instance_uri
        narrower[the_instance_uri] = []
        if "narrower" in instance:
          for children in instance["narrower"]:
            narrower[the_instance_uri].append(format_name(children))
        # Identifier Node and Associated Data Type
        item = instance["identified_by"]
        qualifier_item = ""
        if "qualified_by" in item:
          qualifier_item = item["qualified_by"]
        name = format_name(item["name"])
        item_uri = "%s/%s" % (the_instance_uri, name)
        identifier_uri = item_uri
        collect = False
        if "collect" in item:
          collect = item["collect"]
        record = {
          "name": item["name"], 
          "collect": collect,
          "enabled": item["enabled"],
          "uri": item_uri,
          "uuid": uuid4()
        }
        nodes["InstanceItem"].append(record)
        relationships["HAS_ITEM"].append({"from": the_instance_uri, "to": item_uri})
        relationships["HAS_IDENTIFIER"].append({"from": the_instance_uri, "to": item_uri})
        if "data_type" in item:
          for data_type in item["data_type"]: 
            dt_uri = add_data_type(item_uri, data_type["name"], nodes, relationships)
            relationships["HAS_DATA_TYPE"].append({"from": item_uri, "to": dt_uri})
            if "value_set" in data_type:
              #print(data_type["value_set"])
              for term in data_type["value_set"]: 
                #print(term)
                cl = term["cl"]
                cli = term["cli"]
                result = ct_server.term_reference(cl, cli)
                if result == None:
                  result = { 'uri': "", 'notation': "", 'pref_label': "" }
                record = {
                  "uuid": str(uuid4()),
                  "cl": cl,
                  "cli": cli,
                  "term_uri": result['uri'],
                  "notation": result['notation'],
                  "pref_label": result['pref_label']
                }
                nodes["ValueSet"].append(record)
                relationships["HAS_RESPONSE"].append({"from": dt_uri, "to": record['uuid']})

        # Now all the items
        for item in instance["has_items"]: 
          #print("Enabled:", item["enabled"])
          if not item["enabled"]:
            continue
          name = format_name(item["name"])
          collect = False
          if "collect" in item:
            collect = item["collect"]
          item_uri = "%s/%s" % (the_instance_uri, name)
          record = {
            "name": item["name"], 
            "collect": collect,
            "enabled": item["enabled"],
            "uri": item_uri,
            "uuid": uuid4()
          }
          nodes["InstanceItem"].append(record)
          relationships["HAS_ITEM"].append({"from": the_instance_uri, "to": item_uri})
          #print("Rel: [from: %s, to: %s]" % (the_instance_uri, item_uri))
          if qualifier_item == item["name"]:
            relationships["HAS_QUALIFIER"].append({"from": identifier_uri, "to": item_uri})
          if "data_type" in item:
            for data_type in item["data_type"]: 
              dt_uri = add_data_type(item_uri, data_type["name"], nodes, relationships)
              relationships["HAS_DATA_TYPE"].append({"from": item_uri, "to": dt_uri})
              if "value_set" in data_type:
                #print(data_type["value_set"])
                for term in data_type["value_set"]: 
                  #print(term)
                  cl = term["cl"]
                  cli = term["cli"]
                  result = ct_server.term_reference(cl, cli)
                  if result == None:
                    result = { 'uri': "", 'notation': "", 'pref_label': "" }
                  record = {
                    "uuid": str(uuid4()),
                    "cl": cl,
                    "cli": cli,
                    "term_uri": result['uri'],
                    "notation": result['notation'],
                    "pref_label": result['pref_label']
                  }
                  nodes["ValueSet"].append(record)
                  relationships["HAS_RESPONSE"].append({"from": dt_uri, "to": record['uuid']})

    for k, v in narrower.items():
      if len(v) > 0:
        from_uri = k
        for bc in v:
          to_uri = bc_uri[bc]
          #print("Narrower from %s to %s" % (from_uri, to_uri))
          relationships["BC_NARROWER"].append({"from": from_uri, "to": to_uri})

delete_dir("load_data")

ns_s_json = RaService().namespace_by_name("d4k BC namespace")
#print(ns_s_json)
ra_s_json = RaService().registration_authority_by_namespace_uuid(ns_s_json['uuid'])
#print(ra_s_json)

process_templates(ns_s_json['value'], ns_s_json['uri'], ra_s_json['uri'])
process_instances(ns_s_json['value'], ns_s_json['uri'], ra_s_json['uri'])

for k, v in nodes.items():
  csv_filename = "load_data/node-%s-1.csv" % (snakecase(k))
  write_nodes(v, csv_filename)

for k, v in relationships.items():
  csv_filename = "load_data/relationship-%s-1.csv" % (k.lower())
  write_relationships(v, csv_filename)


# # Simple program to download the latest version of the CDISC SDTM and
# # convert into a semantic form.

# import os
# from xml.dom.minidom import Identified
# from pyparsing import identbodychars
# import requests
# from statistics import variance
# from beautifultable import BeautifulTable
# from rdflib import RDFS, Graph, URIRef, Literal
# from rdflib.namespace import RDF, DC, DCTERMS

# # Get API key. Uses an environment variable.
# API_KEY = os.getenv('CDISC_API_KEY')
# headers =  {"Content-Type":"application/json", "api-key": API_KEY}

# # Get list of SDTM IGs on offer from the API
# # ==========================================
# # 
# # Get the set of SDTM IG datasets
# # CDISC API ref: https://www.cdisc.org/cdisc-library/api-documentation#/default/api.products.products.get_product_datatabulation
# api_url = "https://api.library.cdisc.org/api/mdr/sdtmig/3-4/datasets?expand=false"
# response = requests.get(api_url, headers=headers)
# ig_body = response.json()

# print('')
# print('SDTM IG Information')
# print('')
# print("Description:", ig_body['description'])
# print("Effective Date:", ig_body['effectiveDate'])
# print("Label:", ig_body['label'])
# print("Name:", ig_body['name'])
# print("Registration Status:", ig_body['registrationStatus'])
# print("Source:", ig_body['source'])
# print("Version:", ig_body['version'])
# print('')

# # Load SDTM CT
# ct = Graph()
# ct.parse('data/cdisc/sdtm_ct.ttl', format='ttl')

# # Setup schema and instance namespaces
# schema_ns = "http://ontologies.gsk.com/cdisc/sdtm-ig#"
# instance_ns = "http://id.gsk.com/dataset/cdisc/sdtm-ig/v3-4/"

# # Setup the graph        
# g = Graph()
# g.bind("gsk", URIRef(schema_ns))

# # Build the IG header subject
# ig = URIRef(instance_ns)
# g.add((ig, RDF.type, URIRef("%ssdtmig" % (schema_ns))))
# g.add((ig, RDFS.label, Literal(ig_body['label'])))
# g.add((ig, DC.description, Literal(ig_body['description'])))
# g.add((ig, DC.identifier, Literal(ig_body['name'])))
# g.add((ig, DC.source, Literal(ig_body['source'])))
# g.add((ig, DCTERMS.valid, Literal(ig_body['effectiveDate'])))

# # Process the datasets
# for ds in ig_body['_links']['datasets']:
#     try:
        
#         # Get the href to get the dataset API call.
#         api_url = "https://api.library.cdisc.org/api%s" % (ds['href'])
#         response = requests.get(api_url, headers=headers)
#         ds_body = response.json()
#         domain = ds_body['name']

#         # Process the dataset
#         dataset = URIRef(instance_ns + domain)
#         g.add((dataset, RDF.type, URIRef("%sdataset" % (schema_ns))))
#         g.add((dataset, RDFS.label, Literal(ds_body['label'])))
#         g.add((dataset, DC.identifier, Literal(ds_body['name'])))
#         if 'description' in ds:
#             g.add((dataset, DC.description, Literal(ds_body['description'])))
#         else:
#             # No description, use the label.
#             g.add((dataset, DC.description, Literal(ds_body['label'])))
#         g.add((dataset, URIRef("%sstructure" % (schema_ns)), Literal(ds_body['datasetStructure'])))
#         g.add((dataset, URIRef("%sordinal" % (schema_ns)), Literal(ds_body['ordinal'])))
#         g.add((ig, URIRef("%sdefinesDataset" % (schema_ns)), dataset))    
#         for item in ds_body['datasetVariables']:
#             try:
#                 variable = URIRef("%s%s/%s" % (instance_ns, domain, item['name']))
#                 g.add((variable, RDF.type, URIRef("%scolumn" % (schema_ns))))
#                 g.add((variable, DC.identifier, Literal(item['name'])))
#                 g.add((variable, URIRef("%sordinal" % (schema_ns)), Literal(item['ordinal'])))
#                 g.add((variable, RDFS.label, Literal(item['label'])))
#                 g.add((variable, DC.description, Literal(item['description'])))
#                 g.add((variable, URIRef("%ssimpleDatatype" % (schema_ns)), Literal(item['simpleDatatype'])))
#                 g.add((variable, URIRef("%srole" % (schema_ns)), Literal(item['role'])))
#                 g.add((variable, URIRef("%score" % (schema_ns)), Literal(item['core'])))
#                 if 'codelist' in item['_links']:
#                     #print(item['_links']['codelist'][0]['href'])
                    
#                     # Horrid nasty fix to save time. Don't query the API to get the identifier of the linked codelist.
#                     # Assume the URL last part is the C code. Terrible I know but life it too short.
#                     parts = item['_links']['codelist'][0]['href'].split('/')
#                     identifier = parts[-1]

#                     # The proper way to get the identifier, two API calls
#                     #api_url = "https://api.library.cdisc.org/api%s" % (item['_links']['codelist'][0]['href'])
#                     #cl_response = requests.get(api_url, headers=headers)
#                     #cl_body = cl_response.json()
#                     #api_url = "https://api.library.cdisc.org/api%s" % (cl_body['_links']['versioschema_ns'][-1]['href'])
#                     #clv_response = requests.get(api_url, headers=headers)
#                     #clv_body = clv_response.json()
#                     #identifier = clv_body['conceptId']

#                     # Now link the code list in if we can find it.
#                     #print(identifier)
#                     for triple in ct.triples((None, DC.identifier, Literal(identifier))):
#                         #print(triple)
#                         g.add((variable, URIRef("%scodeList" % (schema_ns)), triple[0]))
                
#                 # Set the value domain. This is for those ISO8601 and other references
#                 if 'describedValueDomain' in item:
#                     g.add((variable, URIRef("%svalueDomain" % (schema_ns)), Literal(item['describedValueDomain'])))
#                 g.add((dataset, URIRef("%sdefinesVariable" % (schema_ns)), variable))
                
#                 # Print just to show things are working.
#                 print(variable)
#             except:
#                 print("********** VARIABLE ***********")
#                 print(item)
#                 raise
#     except:
#         print("********** DATASET ***********")
#         print(ds)
#         raise
# g.serialize(destination="data/cdisc/sdtm_ig.ttl")

