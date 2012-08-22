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
        """
        parse a prov.json file using provpy

        persist the files in a graphdb
        """
        self.bundle = prov.json.load(open(prov_json), cls = prov.ProvBundle.JSONDecoder)
        return self.bundle

    def process_bundle(self):
        print prov.PROV_N_MAP
        self.response = []
        for record in self.bundle.get_records():
            if record.is_element():
                if isinstance(record, prov.ProvEntity):
                    response = self._upload_entity(record)
                    self.response.append(response)
                elif isinstance(record, prov.ProvActivity):
                    response = self._upload_activity(record)
                    self.response.append(response)
                elif isinstance(record, prov.ProvAgent):
                    response = self._upload_agent(record)
                    self.response.append(response)
            elif record.is_relation():
                print 'is_relation'
                if isinstance(record,prov.ProvGeneration):
                    print 'is_generation'
                    response = self._upload_generation(record)
                    self.response.append(response)
        return self.response

### Component 1: Entities and Activities

    def _set_entity_proxy(self):
        """
        initialize entity proxy
        """
        self._graph.add_proxy("entities", ProvEntity)
        self.entities = self._graph.entities

    def _upload_entity(self, record):
        """
        parse entity
        """

        identifier = record.get_identifier().get_uri()
        asserted_types = [type.get_uri() for type in record.get_asserted_types()]
        attributes = record.get_attributes()
        provn = record.get_provn()
        response = self.entities.create(identifier=identifier,
            asserted_types=asserted_types,
            attributes=attributes[1],
            provn=provn
        )
        print 'entity:',response.eid
        return response

    def _set_activity_proxy(self):
        self._graph.add_proxy("activities", ProvActivity)
        self.activities = self._graph.activities

    def _upload_activity(self, record):
        """
        parse activity
        """

        identifier = record.get_identifier().get_uri()
        asserted_types = [type.get_uri() for type in record.get_asserted_types()]
        attributes = record.get_attributes()
        provn = record.get_provn()
        response = self.activities.create(identifier=identifier,
            asserted_types=asserted_types,
            attributes=attributes[1],
            provn=provn,
            start_time=record.get_attributes()[0].values()[0],
            end_time=record.get_attributes()[0].values()[1]
        )
        print 'activity:', response.eid
        return response

    def _set_generation_proxy(self):
        self._graph.add_proxy("wasGeneratedBy", ProvGeneration)
        self.wasGeneratedBy = self._graph.wasGeneratedBy

    def _upload_generation(self,record):
        entity = record.get_attributes()[0][1]
        print entity
        activity = record.get_attributes()[0][2]
        print activity
        provn = record.get_provn()
        response = self.wasGeneratedBy.create(entity=entity,
            activity=activity,
            provn=provn
        )
        print 'generation:', response.eid
        return response

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

    def _upload_agent(self, record):
        """
        parse activity
        """

        identifier = record.get_identifier().get_uri()
        asserted_types = [type.get_uri() for type in record.get_asserted_types()]
        attributes = record.get_attributes()
        provn = record.get_provn()
        response = self.agents.create(identifier=identifier,
            asserted_types=asserted_types,
            attributes=attributes[1],
            provn=provn,
        )
        print 'agent:', response.eid
        return response

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

