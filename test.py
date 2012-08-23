__author__ = 'nolan'

from bulbs.rexster import Graph, Config

from interface import Interface, Config

config = Config('http://50.112.248.236:8182/graphs/xcede-dm')


interface = Interface(config)

bundle = interface.parse_prov('workflow_provenance.json')

records = bundle.get_records()

entity = records[0]

response = interface.process_bundle()


