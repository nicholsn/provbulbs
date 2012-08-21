#!/usr/local/bin/python
"""
Bulbs is an interface to a number of graph databases.

Provpy is a python implementation of the PROV-DM provenance data model.

This is an exercise in wrapping provpy and persisting it in a graph database using bulbs. The provpy.model modules was
manually extract for use, as it's current form is not easily installable.
"""
__author__ = 'Nolan Nichols'

from bulbs.model import Node, Relationship
from bulbs.property import String, Integer, List, DateTime, Dictionary
from bulbs.utils import current_datetime

class ProvElement(Node):
    """
    Base class for all Elements in PROV-DM. Implemented as a subclass of a bulbs.model.Node
    """
    element_type = 'element'

    # Required
    identifier = String(nullable=False)
    created = DateTime(default=current_datetime, nullable=False)

    # Optional
    asserted_types = List(nullable=True)
    attributes = List(nullable=True)
    provn = String(nullable=True)

class ProvRelation(Relationship):
    """
    Base class for all Relationsips in PROV-DM. Implemented as a subclass of a bulbs.model.Relationship
    """

    label = "relation"

    # Required
    identifier = String(nullable=False)
    created = DateTime(default=current_datetime, nullable=False)

    # Optional
    asserted_types = List(nullable=True)
    attributes = List(nullable=True)
    provn = String(nullable=True)


### Component 1: Entities and Activities
# http://dvcs.w3.org/hg/prov/raw-file/default/model/prov-dm.html#component1


class ProvEntity(ProvElement):
    """
    prov:entity

    Example:

    >>> from bulbs.rexster import Graph, Config
    >>> from xcede.
    >>> config = Config("http://<your-rexster-graph-url>")
    >>> g = Graph(config)
    >>>

    """
    element_type = "entity"
    prov_type = String(default=lambda:"http://www.w3.org/ns/prov#Entity",nullable=False)

class ProvActivity(ProvElement):
    """
    prov:activity
    """
    element_type = "activity"
    prov_type = String(default=lambda:"http://www.w3.org/ns/prov#Activity",nullable=False)

    # Optional
    start_time = DateTime(nullable=True)
    end_time = DateTime(nullable=True)

class ProvGeneration(ProvRelation):
    """
    prov:generation
    """
    label = "wasGeneratedBy"
    prov_type = String(default=lambda:"http://www.w3.org/ns/prov#Generation",nullable=False)
    # Required attributes
    entity = String(nullable=False)
    # Optional attributes
    activity = String(nullable=True)
    time = DateTime(nullable=True)

class ProvUsage(ProvRelation):
    """
    prov:usage
    """
    label = "used"
    prov_type = String(default=lambda:"http://www.w3.org/ns/prov#Usage",nullable=False)
    # Required attributes
    activity = String(nullable=False)
    # Optional attributes
    entity = String(nullable=True)
    time = DateTime(nullable=True)

class ProvCommunication(ProvRelation):
    """
    prov:communication
    """
    label = "wasInformedBy"
    prov_type = String(default=lambda:"http://www.w3.org/ns/prov#Communication",nullable=False)
    # Required attributes
    informed_activity = String(nullable=False)
    informant_activity = String(nullable=False)

class ProvStart(ProvRelation):
    """
    prov:start
    """
    label = "wasStartedBy"
    prov_type = String(default=lambda:"http://www.w3.org/ns/prov#Start",nullable=False)
    # Required attributes
    activity = String(nullable=False)
    # Optional attributes
    trigger_entity = String(nullable=True)
    starter_activity = String(nullable=True)
    time = DateTime(nullable=True)

class ProvEnd(ProvRelation):
    """
    prov:end
    """
    label = "wasEndedBy"
    prov_type = String(default=lambda:"http://www.w3.org/ns/prov#End",nullable=False)
    # Required attributes
    activity = String(nullable=False)
    # Optional attributes
    trigger_entity = String(nullable=True)
    ender_activity = String(nullable=True)
    time = DateTime(nullable=True)

class ProvInvalidation(ProvRelation):
    """
    prov:invalidation
    """
    label = "wasInvalidatedBy"
    prov_type = String(default=lambda:"http://www.w3.org/ns/prov#Invalidation",nullable=False)
    # Required attributes
    entity = String(nullable=False)
    # Optional attributes
    activity = String(nullable=True)
    time = DateTime(nullable=True)

### Component 2: Derivations

class ProvDerivation(ProvRelation):
    """
    prov:derivation
    """
    label = "wasDerivedFrom"
    prov_type = String(default=lambda:"http://www.w3.org/ns/prov#Derivation",nullable=False)
    # Required attributes
    generated_entity = String(nullable=False)
    used_entity = String(nullable=False)
    # Optional attributes
    activity = String(nullable=True)
    generation = String(nullable=True)
    usage = String(nullable=True)

### Component 3: Agents, Responsibility, and Influence

class ProvAgent(ProvElement):
    """
    prov:agent
    """
    element_type = "agent"
    prov_type = String(default=lambda:"http://www.w3.org/ns/prov#Agent",nullable=False)

class ProvAttribution(ProvRelation):
    """
    prov:attribution
    """
    label = "wasAttributedTo"
    prov_type = String(default=lambda:"http://www.w3.org/ns/prov#Attribution",nullable=False)
    # Required attributes
    entity = String(nullable=False)
    agent = String(nullable=False)

class ProvAssociation(ProvRelation):
    """
    prov:association
    """
    label = "wasAssociatedWith"
    prov_type = String(default=lambda:"http://www.w3.org/ns/prov#Association",nullable=False)
    # Required attributes
    activity = String(nullable=False)
    # Optional attributes
    agent = String(nullable=False)
    plan = String(nullable=False)

class ProvDelegation(ProvRelation):
    """
    prov:delegation
    """
    label = "wasAssociatedWith"
    prov_type = String(default=lambda:"http://www.w3.org/ns/prov#Delegation",nullable=False)
    # Required attributes
    delegate_entity = String(nullable=False)
    responsible_entity = String(nullable=False)
    # Optional attributes
    activity = String(nullable=False)

class ProvInfluence(ProvRelation):
    """
    prov:influence
    """
    label = "wasInfluencedBy"
    prov_type = String(default=lambda:"http://www.w3.org/ns/prov#Influence",nullable=False)
    # Required attributes
    influencee_entity = String(nullable=False)
    influencer_entity = String(nullable=False)
    # Optional attributes
    activity = String(nullable=False)

### Component 4: Bundles

# See below

### Component 5: Alternate Entities

class ProvSpecialization(ProvRelation):
    """
    prov:specialization
    """
    label = "specializationOf"
    prov_type = String(default=lambda:"http://www.w3.org/ns/prov#Specialization",nullable=False)
    # Required attributes
    specific_entity = String(nullable=False)
    general_entity = String(nullable=False)

class ProvAlternate(ProvRelation):
    """
    prov:alternate
    """
    label = "alternateOf"
    prov_type = String(default=lambda:"http://www.w3.org/ns/prov#Alternate",nullable=False)
    # Required attributes
    alternate1_entity = String(nullable=False)
    alternate2_entity = String(nullable=False)

class ProvMention(ProvSpecialization):
    """
    prov:mention
    """
    label = "mentionOf"
    #prov_type = "http://www.w3.org/ns/prov#Mention"
    # Required attributes
    collection = String(nullable=False)
    bundle_entity = String(nullable=False)

### Component 6: Collections

class ProvMembership(ProvRelation):
    """
    prov:membership
    """
    label = "hadMember"
    prov_type = String(default=lambda:"http://www.w3.org/ns/prov#Membership",nullable=False)
    # Required attributes
    colection_entity = String(nullable=False)
    entity = String(nullable=False)

class ProvBundle(ProvEntity):
    """
    prov:bundle
    """
    element_type = "bundle"
    #prov_type = String(default=lambda:"http://www.w3.org/ns/prov#Bundle",nullable=False)
    # TODO how to set different defaults for subclasses - just reimplement or set defaults via interface?

    record = List(nullable=False)