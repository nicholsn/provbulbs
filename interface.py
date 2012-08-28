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
    >>> interface.process_bundle()
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

        cache as an attribute of interface

        return parsed bundle
        """
        self._bundle = prov.json.load(open(prov_json), cls = prov.ProvBundle.JSONDecoder)
        return self._bundle

    def _upload_lookup(self, proxy_type):
        """
        method to lookup the upload function for a given structure
        """
        return getattr(self, '_upload_' + proxy_type, None)

    def process_bundle(self, bundle=None):
        bundle = bundle
        structures = {'element': [],
                     'relation': []}
        responses = {}
        try:
            if not bundle:
                bundle = self._bundle
            for record in bundle.get_records():
                if record.is_element():
                    # TODO logic for processing nested bundles
                    if isinstance(record,prov.ProvBundle):
                        structures['element'].append(record)
                        self.process_bundle(bundle=record)
                    else:
                        structures['element'].append(record)
                elif record.is_relation():
                    structures['relation'].append(record)
                else:
                    pass
        except AttributeError:
            print "self._bundle is None. Did you run interface.parse_prov(<prov.json>)?"

        # first process all the elements
        for element in structures['element']:
            element_type = prov.PROV_N_MAP[element.get_type()]
            element_uri = element.get_identifier().get_uri()
            responses[element_uri] = self._upload_lookup(element_type)(element)

        # relations require that elements are created first
        for relation in structures['relation']:
            relation_type = prov.PROV_N_MAP[relation.get_type()]
            relation_id = relation.get_identifier()
            if relation_id:
                relation_uri = relation_id.get_uri()
                responses[relation_uri] = self._upload_lookup(relation_type)(relation)
            else:
                relation_uri = 'rel_' + str(hash(relation))
                responses[relation_uri] = self._upload_lookup(relation_type)(relation)

        return responses

    def _process_attributes(self, record):
        attributes = record.get_attributes()
        optional_attributes = attributes[0]
        extra_attributes = attributes[1]
        results = {'optional':{},
                   'extra':{}}

        if optional_attributes:
            for attribute in optional_attributes:
                optional = {}
                for k,v in attribute:

                    if isinstance(k, prov.QName):
                        if isinstance(v, prov.QName):
                            optional[k.get_uri()] = v.get_uri()
                        else:
                            optional[k.get_uri()] = v
                    else:
                        if isinstance(v, prov.QName):
                            optional[k] = v.get_uri()
                        else:
                            optional[k] = v
                    results['optional'] = optional

        if extra_attributes:
            for k,v in extra_attributes:
                extra = {}

                if isinstance(k, prov.QName):
                    if isinstance(v, prov.QName):
                        key = k.get_namespace().get_prefix() + '_' + k.get_localpart()
                        extra[key] = v.get_uri()
                    else:
                        key = k.get_namespace().get_prefix() + '_' + k.get_localpart()
                        extra[key] = v
                else:
                    if isinstance(v, prov.QName):
                        extra[k] = v.get_localpart()
                    else:
                        extra[k] = v
                results['extra'] = extra
        return results


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
        attributes = self._process_attributes(record)
        optional_attributes = attributes['optional']
        extra_attributes = attributes['extra']

        data = dict(
            identifier = record.get_identifier().get_uri(),
            provn = record.get_provn(),
            asserted_types = [type.get_uri() for type in record.get_asserted_types()],
            optional_attributes = optional_attributes.keys(),
            extra_attributes = extra_attributes.keys()
            )
        response = self.entities.create(data)

        data_update = response.data()

        for optional in optional_attributes.iterkeys():
            prov_type = prov.PROV_ID_ATTRIBUTES_MAP[optional].split(':')[-1]
            data_update[prov_type] = optional_attributes[optional]

        for extra in extra_attributes.iterkeys():
            # bulbs reserves the 'label' keyword
            prov_type = extra
            data_update[prov_type] = extra_attributes[extra]

        if record.is_element:
            self._graph.vertices.update(response.eid,data_update)

        elif record.is_relation:
            self._graph.edges.update(response.eid,data_update)

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

    def _upload_wasGeneratedBy(self,record):
        provn = record.get_provn()
        entity_id = record.get_attributes()[0][1].get_identifier().get_uri()
        entity = list(self.entities.index.lookup(identifier=entity_id))[0]
        activity_id = record.get_attributes()[0][2].get_identifier().get_uri()
        activity = list(self.activities.index.lookup(identifier=activity_id))[0]
        response = self.wasGeneratedBy.create(entity, activity,
            entity=entity,
            activity=activity,
            provn=provn
        )
        print 'generation:', response.eid
        return response

    def _set_usage_proxy(self):
        self._graph.add_proxy("used", ProvUsage)
        self.used = self._graph.used

    def _upload_used(self, record):
        provn = record.get_provn()
        activity_id = record.get_attributes()[0][2].get_identifier().get_uri()
        activity = list(self.activities.index.lookup(identifier=activity_id))[0]
        entity_id = record.get_attributes()[0][1].get_identifier().get_uri()
        entity = list(self.entities.index.lookup(identifier=entity_id))[0]
        response = self.used.create(activity, entity,
            activity=activity,
            entity=entity,
            provn=provn
        )
        print 'used:', response.eid
        return response

    def _set_communication_proxy(self):
        self._graph.add_proxy("wasInformedBy", ProvCommunication)
        self.wasInformedBy = self._graph.wasInformedBy

    def _set_start_proxy(self):
        self._graph.add_proxy("wasStartedBy", ProvStart)
        self.wasStartedBy = self._graph.wasStartedBy

    def _upload_wasStartedBy(self,record):
        provn = record.get_provn()

        attributes = record.get_attributes()

        activity_id = attributes[0][2].get_identifier().get_uri()
        activity = list(self.activities.index.lookup(identifier=activity_id))[0]
        starter_id = attributes[0][6].get_identifier().get_uri()
        starter = list(self.activities.index.lookup(identifier=starter_id))[0]
        response = self.wasStartedBy.create(activity, starter,
            activity=activity,
            starter_activity=starter,
            provn=provn
        )
        print 'wasStartedBy:', response.eid
        return response

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

    def _upload_wasAssociatedWith(self,record):
        provn = record.get_provn()

        attributes = record.get_attributes()

        activity_id = attributes[0][2].get_identifier().get_uri()
        activity = list(self.activities.index.lookup(identifier=activity_id))[0]
        agent_id = attributes[0][8].get_identifier().get_uri()
        agent = list(self.agents.index.lookup(identifier=agent_id))[0]
        response = self.wasAssociatedWith.create(activity, agent,
            activity=activity,
            agent=agent,
            provn=provn
        )
        print 'wasAssociatedWith:', response.eid
        return response

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


    class _ElementProxy(object):
        def __init__(self):
            pass