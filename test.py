__author__ = 'nolan'

from interface import Interface, Config

config = Config('http://50.112.248.236:8182/graphs/xcede-dm')

interface = Interface(config)

bundle = interface.parse_prov('workflow_provenance.json')

interface.process_bundle()


