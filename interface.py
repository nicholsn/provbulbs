"""
Provides i/o for persisting and retrieving provenance.json files using provpy and bulbs
"""
__author__ = 'nolan'

from model import *

#ProvEntity, ProvActivity, ProvAgent, ProvAssociation, ProvGeneration, ProvUsage

from bulbs.rexster import Graph, Config
import prov


class Interface(object):
    """
    Proxy interface to to the provbulbs model

    Example:

    >>> from xcede.interface import Interface, Config
    >>> config = Config("http://<your-rexster-graph-url>")
    >>> interface = Interface(config)
    >>> interface.parse_prov("provenance.json")
    """
    def __init__(self, config):
        self._graph = Graph(config)
        self._set_entity_proxy()
        self._set_activity_proxy()
        self._set_generation_proxy()
        self._set_usage_proxy()
        self._set_communication_proxy()
        self._set_start_proxy()
        self._set_end_proxy()
        self._set_invalidation_proxy()
        self._set_derivation_proxy()
        self._set_agent_proxy()
        self._set_attribution_proxy()
        self._set_association_proxy()
        self._set_delegation_proxy()
        self._set_influence_proxy()
        self._set_specialization_proxy()
        self._set_alternate_proxy()
        self._set_mention_proxy()
        self._set_membership_proxy()
        self._set_bundle_proxy()

    def parse_prov (self, prov_json):
        self.bundle = prov.json.load(open(prov_json), cls = prov.ProvBundle.JSONDecoder)
        return self.bundle

    def process_bundle(self):
        prov_types = prov.PROV_RECORD_IDS_MAP.keys()
        for record in self.bundle.get_records():
            if record in prov_types:
                print 'prov:type', record
            type = record.get_prov_type() #prov.PROV_N_MAP[record.get_type()]
            print type
            asserted_types = [type.get_uri() for type in record.get_asserted_types()]
            print asserted_types
            attributes = []
        return



    # initilize proxies to each of the prov structures

    def _set_entity_proxy(self):
        self._graph.add_proxy("entities", ProvEntity)
        self.entities = self._graph.entities

    def _set_activity_proxy(self):
        self._graph.add_proxy("activities", ProvActivity)
        self.activities = self._graph.activities

    def _set_generation_proxy(self):
        self._graph.add_proxy("wasGeneratedBy", ProvGeneration)
        self.wasGeneratedBy = self._graph.wasGeneratedBy

    def _set_usage_proxy(self):
        self._graph.add_proxy("used", ProvUsage)
        self.used = self._graph.used

    def _set_communication_proxy(self):
        self._graph.add_proxy("wasInformedBy", ProvCommunication)
        self.wasInformedBy = self._graph.wasInformedBy

    def _set_start_proxy(self):
        self._graph.add_proxy("wasStartedBy", ProvStart)
        self.wasStartedBy = self._graph.wasStartedBy

    def _set_end_proxy(self):
        self._graph.add_proxy("wasEndedBy", ProvEnd)
        self.wasEndedBy = self._graph.wasEndedBy

    def _set_invalidation_proxy(self):
        self._graph.add_proxy("wasInvalidatedBy", ProvInvalidation)
        self.wasInvalidatedBy = self._graph.wasInvalidatedBy

### Component 2: Derivations

    def _set_derivation_proxy(self):
        self._graph.add_proxy("wasDerivedFrom", ProvDerivation)
        self.wasDerivedFrom = self._graph.wasDerivedFrom


### Component 3: Agents, Responsibility, and Influence

    def _set_agent_proxy(self):
        self._graph.add_proxy("agents", ProvAgent)
        self.agents = self._graph.agents

    def _set_attribution_proxy(self):
        self._graph.add_proxy("wasAttributedTo", ProvAttribution)
        self.wasAttributedTo = self._graph.wasAttributedTo

    def _set_association_proxy(self):
        self._graph.add_proxy("wasAssociatedWith", ProvAssociation)
        self.wasAssociatedWith = self._graph.wasAssociatedWith

    def _set_delegation_proxy(self):
        self._graph.add_proxy("actedOnBehalfOf", ProvDelegation)
        self.actedOnBehalfOf = self._graph.actedOnBehalfOf

    def _set_influence_proxy(self):
        self._graph.add_proxy("wasInfluencedBy", ProvInfluence)
        self.wasInfluencedBy = self._graph.wasInfluencedBy

### Component 4: Bundles

# See below

### Component 5: Alternate Entities

    def _set_specialization_proxy(self):
        self._graph.add_proxy("specializationOf", ProvSpecialization)
        self.specializationOf = self._graph.specializationOf

    def _set_alternate_proxy(self):
        self._graph.add_proxy("alternateOf", ProvAlternate)
        self.alternateOf = self._graph.alternateOf

    def _set_mention_proxy(self):
        self._graph.add_proxy("mentionOf", ProvMention)
        self.mentionOf = self._graph.mentionOf

### Component 6: Collections

    def _set_membership_proxy(self):
        self._graph.add_proxy("hadMember", ProvMembership)
        self.hadMember = self._graph.hadMember

    def _set_bundle_proxy(self):
        self._graph.add_proxy("bundles", ProvBundle)
        self.bundles = self._graph.bundles

