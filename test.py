__author__ = 'nolan'

from bulbs.rexster import Graph, Config

from interface import Interface, Config

# configure for rexster database
config = Config('http://50.112.248.236:8182/graphs/xcede-dm')

# initialize the interface and establish proxy classes
interface = Interface(config)

# parse an example provenance.json file
bundle = interface.parse_prov('workflow_provenance.json')

# bundle has full access to the provpy api
records = bundle.get_records()
entity = records[0]

# processed the parsed bundle stored at <interface._bundle>
# maps provpy to provbulb.model and pushes data to rexster
response = interface.process_bundle()

# interface can be used to query for model objects
for i in interface.entities.get_all(): print i

# request for the registered keys for a model
interface.entities.get_property_keys()

# and query a model object by their properties
identifier='http://nipy.org/nipype/terms/0.6/ag1'
for i in interface.agents.index.lookup(identifier=identifier):
    print i




