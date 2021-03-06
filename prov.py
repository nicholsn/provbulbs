'''Python implemetation of the W3C Provenance Data Model (PROV-DM)

Support for PROV-JSON import/export

References:

PROV-DM: http://www.w3.org/TR/prov-dm/

@author: Trung Dong Huynh <trungdong@donggiang.com>
@copyright: University of Southampton 2012
'''

import logging
import datetime
import json
import re
import collections
from collections import OrderedDict, defaultdict
logger = logging.getLogger(__name__)

## PROV record constants - PROV-DM LC
# C1. Entities/Activities
PROV_REC_ENTITY                 = 1
PROV_REC_ACTIVITY               = 2
PROV_REC_GENERATION             = 11
PROV_REC_USAGE                  = 12
PROV_REC_COMMUNICATION          = 13
PROV_REC_START                  = 14
PROV_REC_END                    = 15
PROV_REC_INVALIDATION           = 16

# C2. Derivations
PROV_REC_DERIVATION             = 21

# C3. Agents/Responsibility
PROV_REC_AGENT                  = 3
PROV_REC_ATTRIBUTION            = 31
PROV_REC_ASSOCIATION            = 32
PROV_REC_DELEGATION             = 33
PROV_REC_INFLUENCE              = 34
# C4. Bundles
PROV_REC_BUNDLE                 = 4 # This is the lowest value, so bundle(s) in JSON will be decoded first
# C5. Alternate
PROV_REC_ALTERNATE              = 51
PROV_REC_SPECIALIZATION         = 52
PROV_REC_MENTION                = 53
# C6. Collections
#PROV_REC_COLLECTION             = 6
PROV_REC_MEMBERSHIP             = 61

PROV_RECORD_TYPES = (
    (PROV_REC_ENTITY,               u'Entity'),
    (PROV_REC_ACTIVITY,             u'Activity'),
    (PROV_REC_GENERATION,           u'Generation'),
    (PROV_REC_USAGE,                u'Usage'),
    (PROV_REC_COMMUNICATION,        u'Communication'),
    (PROV_REC_START,                u'Start'),
    (PROV_REC_END,                  u'End'),
    (PROV_REC_INVALIDATION,         u'Invalidation'),
    (PROV_REC_DERIVATION,           u'Derivation'),
    (PROV_REC_AGENT,                u'Agent'),
    (PROV_REC_ATTRIBUTION,          u'Attribution'),
    (PROV_REC_ASSOCIATION,          u'Association'),
    (PROV_REC_DELEGATION,           u'Delegation'),
    (PROV_REC_BUNDLE,               u'Bundle'),
    (PROV_REC_ALTERNATE,            u'Alternate'),
    (PROV_REC_SPECIALIZATION,       u'Specialization'),
    (PROV_REC_MENTION,              u'Mention'),
    #    (PROV_REC_COLLECTION,           u'Collection'),
    (PROV_REC_MEMBERSHIP,           u'Membership'),
    (PROV_REC_BUNDLE,               u'Bundle'),
    )

PROV_N_MAP = {
    PROV_REC_ENTITY:               u'entity',
    PROV_REC_ACTIVITY:             u'activity',
    PROV_REC_GENERATION:           u'wasGeneratedBy',
    PROV_REC_USAGE:                u'used',
    PROV_REC_COMMUNICATION:        u'wasInformedBy',
    PROV_REC_START:                u'wasStartedBy',
    PROV_REC_END:                  u'wasEndedBy',
    PROV_REC_INVALIDATION:         u'wasInvalidatedBy',
    PROV_REC_DERIVATION:           u'wasDerivedFrom',
    PROV_REC_AGENT:                u'agent',
    PROV_REC_ATTRIBUTION:          u'wasAttributedTo',
    PROV_REC_ASSOCIATION:          u'wasAssociatedWith',
    PROV_REC_DELEGATION:           u'actedOnBehalfOf',
    PROV_REC_ALTERNATE:            u'alternateOf',
    PROV_REC_SPECIALIZATION:       u'specializationOf',
    PROV_REC_MENTION:              u'mentionOf',
    #    PROV_REC_COLLECTION:           u'Collection',
    PROV_REC_MEMBERSHIP:           u'memberOf',
    PROV_REC_BUNDLE:               u'bundle',
    }

## Identifiers for PROV's attributes
PROV_ATTR_ENTITY                = 1
PROV_ATTR_ACTIVITY              = 2
PROV_ATTR_TRIGGER               = 3
PROV_ATTR_INFORMED              = 4
PROV_ATTR_INFORMANT             = 5
PROV_ATTR_STARTER               = 6
PROV_ATTR_ENDER                 = 7
PROV_ATTR_AGENT                 = 8
PROV_ATTR_PLAN                  = 9
PROV_ATTR_DELEGATE              = 10
PROV_ATTR_RESPONSIBLE           = 11
PROV_ATTR_GENERATED_ENTITY      = 12
PROV_ATTR_USED_ENTITY           = 13
PROV_ATTR_GENERATION            = 14
PROV_ATTR_USAGE                 = 15
PROV_ATTR_SPECIFIC_ENTITY       = 16
PROV_ATTR_GENERAL_ENTITY        = 17
PROV_ATTR_ALTERNATE1            = 18
PROV_ATTR_ALTERNATE2            = 19
PROV_ATTR_BUNDLE                = 20
PROV_ATTR_INFLUENCEE            = 21
PROV_ATTR_INFLUENCER            = 22
PROV_ATTR_COLLECTION            = 23

# Literal properties
PROV_ATTR_TIME                  = 100
PROV_ATTR_STARTTIME             = 101
PROV_ATTR_ENDTIME               = 102

PROV_RECORD_ATTRIBUTES = (
    # Relations properties
    (PROV_ATTR_ENTITY, u'prov:entity'),
    (PROV_ATTR_ACTIVITY, u'prov:activity'),
    (PROV_ATTR_TRIGGER, u'prov:trigger'),
    (PROV_ATTR_INFORMED, u'prov:informed'),
    (PROV_ATTR_INFORMANT, u'prov:informant'),
    (PROV_ATTR_STARTER, u'prov:starter'),
    (PROV_ATTR_ENDER, u'prov:ender'),
    (PROV_ATTR_AGENT, u'prov:agent'),
    (PROV_ATTR_PLAN, u'prov:plan'),
    (PROV_ATTR_DELEGATE, u'prov:delegate'),
    (PROV_ATTR_RESPONSIBLE, u'prov:responsible'),
    (PROV_ATTR_GENERATED_ENTITY, u'prov:generatedEntity'),
    (PROV_ATTR_USED_ENTITY, u'prov:usedEntity'),
    (PROV_ATTR_GENERATION, u'prov:generation'),
    (PROV_ATTR_USAGE, u'prov:usage'),
    (PROV_ATTR_SPECIFIC_ENTITY, u'prov:specificEntity'),
    (PROV_ATTR_GENERAL_ENTITY, u'prov:generalEntity'),
    (PROV_ATTR_ALTERNATE1, u'prov:alternate1'),
    (PROV_ATTR_ALTERNATE2, u'prov:alternate2'),
    (PROV_ATTR_BUNDLE, u'prov:bundle'),
    (PROV_ATTR_INFLUENCEE, u'prov:influencee'),
    (PROV_ATTR_INFLUENCER, u'prov:influencer'),
    # Literal properties
    (PROV_ATTR_TIME, u'prov:time'),
    (PROV_ATTR_STARTTIME, u'prov:startTime'),
    (PROV_ATTR_ENDTIME, u'prov:endTime'),
    )

PROV_ATTRIBUTE_LITERALS = set([PROV_ATTR_TIME, PROV_ATTR_STARTTIME, PROV_ATTR_ENDTIME])

PROV_RECORD_IDS_MAP = dict((PROV_N_MAP[rec_type_id], rec_type_id) for rec_type_id in PROV_N_MAP)
PROV_ID_ATTRIBUTES_MAP = dict((prov_id, attribute) for (prov_id, attribute) in PROV_RECORD_ATTRIBUTES)
PROV_ATTRIBUTES_ID_MAP = dict((attribute, prov_id) for (prov_id, attribute) in PROV_RECORD_ATTRIBUTES)

_r_xsd_dateTime = re.compile(""" ^
    (?P<year>-?[0-9]{4}) - (?P<month>[0-9]{2}) - (?P<day>[0-9]{2})
    T (?P<hour>[0-9]{2}) : (?P<minute>[0-9]{2}) : (?P<second>[0-9]{2})
    (?P<microsecond>\.[0-9]{1,6})?
    (?P<tz>
      Z | (?P<tz_hr>[-+][0-9]{2}) : (?P<tz_min>[0-9]{2})
    )?
    $ """, re.X)
_r_typed_literal_uri = re.compile(r'^"(?P<value>[^"\\]*(?:\\.[^"\\]*)*)"\^\^<(?P<datatype>[^>\\]*(?:\\.[^>\\]*)*)>$', re.X)
_r_typed_literal_qname = re.compile(r'^"(?P<value>[^"\\]*(?:\\.[^"\\]*)*)"\^\^(?P<datatype>[^>\\]*(?:\\.[^>\\]*)*)$', re.X)

# Datatypes
def _parse_xsd_dateTime(s):
    """Returns datetime or None."""
    m = _r_xsd_dateTime.match(s)
    if m is not None:
        values = m.groupdict()
    if values["microsecond"] is None:
        values["microsecond"] = 0
    else:
        values["microsecond"] = values["microsecond"][1:]
        values["microsecond"] += "0" * (6 - len(values["microsecond"]))
    values = dict((k, int(v)) for k, v in values.iteritems()
        if not k.startswith("tz"))

    return datetime.datetime(**values)

def _ensure_datetime(time):
    if isinstance(time, basestring):
        return _parse_xsd_dateTime(time);
    else:
        return time

def parse_xsd_dateTime(s):
    try:
        return _parse_xsd_dateTime(s)
    except ValueError:
        pass
    return None

DATATYPE_PARSERS = {
    datetime.datetime: parse_xsd_dateTime,
    }

def parse_datatype(value, datatype):
    if datatype in DATATYPE_PARSERS:
        # found the required parser
        return DATATYPE_PARSERS[datatype](value)
    else:
        # No parser found for the given data type
        raise Exception(u'No parser found for the data type <%s>' % str(datatype))

class Literal(object):
    def __init__(self, value, datatype):
        self._value = value
        self._datatype = datatype

    def __str__(self):
        return self.provn_representation()

    def get_value(self):
        return self._value

    def get_datatype(self):
        return self._datatype

    def provn_representation(self):
        return u'%s %%%% %s' % (str(self._value), str(self._datatype))

    def json_representation(self):
        if isinstance(self._datatype, QName):
            return u'"%s"^^%s' % (str(self._value), str(self._datatype))
        else:
            # Assuming it is a valid identifier
            return u'"%s"^^<%s>' % (str(self._value), self._datatype.get_uri())

class Identifier(object):
    def __init__(self, uri):
        self._uri = uri

    def get_uri(self):
        return self._uri

    def __str__(self):
        return self._uri

    def __eq__(self, other):
        return self.get_uri() == other.get_uri() if isinstance(other, Identifier) else False

    def __hash__(self):
        return hash(self.get_uri())

    def provn_representation(self):
        return self._uri + u' %% xsd:anyURI'

    def json_representation(self):
        return u'"%s"^^%s' % (self._uri, u'xsd:anyURI')


class QName(Identifier):
    def __init__(self, namespace, localpart):
        self._namespace = namespace
        self._localpart = localpart
        self._str = ':'.join([namespace._prefix, localpart]) if namespace._prefix else localpart

    def get_namespace(self):
        return self._namespace

    def get_localpart(self):
        return self._localpart

    def get_uri(self):
        return ''.join([self._namespace._uri, self._localpart])

    def __str__(self):
        return self._str

    def provn_representation(self):
        return u"'%s'" % self._str

    def json_representation(self):
        return u'"%s"^^%s' % (str(self), u'xsd:QName')


class Namespace(object):
    def __init__(self, prefix, uri):
        self._prefix = prefix
        self._uri = uri
        self._cache = dict()

    def get_prefix(self):
        return self._prefix

    def get_uri(self):
        return self._uri

    def contains(self, identifier):
        uri = identifier if isinstance(identifier, (str, unicode)) else (identifier.get_uri() if isinstance(identifier, Identifier) else None)
        return uri.startswith(self._uri) if uri else False

    def qname(self, identifier):
        uri = identifier if isinstance(identifier, (str, unicode)) else (identifier.get_uri() if isinstance(identifier, Identifier) else None)
        if uri and uri.startswith(self._uri):
            return QName(self, uri[len(self._uri):])
        else:
            return None

    def __eq__(self, other):
        return (self._uri == other._uri and self._prefix == other._prefix) if isinstance(other, Namespace) else False

    def __getitem__(self, localpart):
        if localpart in self._cache:
            return self._cache[localpart]
        else:
            qname = QName(self, localpart)
            self._cache[localpart] = qname
            return qname

XSD = Namespace("xsd",'http://www.w3.org/2001/XMLSchema-datatypes#')
PROV = Namespace("prov",'http://www.w3.org/ns/prov#')

### Exceptions

class ProvException(Exception):
    """Base class for exceptions in this module."""
    pass

class ProvExceptionMissingRequiredAttribute(ProvException):
    def __init__(self, record_type, attribute_id):
        self.record_type = record_type
        self.attribute_id = attribute_id

class ProvExceptionNotValidAttribute(ProvException):
    def __init__(self, record_type, attribute, attribute_types):
        self.record_type = record_type
        self.attribute = attribute
        self.attribute_types = attribute_types

class ProvExceptionContraint(ProvException):
    def __init__(self, record_type, attribute1, attribute2, msg):
        self.record_type = record_type
        self.attribute1 = attribute1
        self.attribute2 = attribute2
        self.msg = msg


# PROV records
class ProvRecord(object):
    """Base class for PROV _records."""
    def __init__(self, bundle, identifier, attributes=None, other_attributes=None):
        self._bundle = bundle
        self._identifier = identifier
        self._attributes = None
        self._extra_attributes = None
        if attributes or other_attributes:
            self.add_attributes(attributes, other_attributes)

    def get_type(self):
        pass

    def get_prov_type(self):
        pass

    def get_asserted_types(self):
        if self._extra_attributes:
            prov_type = PROV['type']
            return set([value for attr, value in self._extra_attributes if attr == prov_type])
        return set()

    def add_asserted_type(self, type_identifier):
        asserted_types = self.get_asserted_types()
        if type_identifier not in asserted_types:
            if self._extra_attributes is None:
                self._extra_attributes = []
            self._extra_attributes.append((PROV['type'], type_identifier))

    def get_identifier(self):
        return self._identifier

    def add_extra_attributes(self, extra_attributes):
        if extra_attributes:
            if self._extra_attributes is None:
                self._extra_attributes = []
            try:
                # This will only work if extra_attributes is a dictionary
                # Converting the dictionary into a list of tuples (i.e. attribute-value pairs)
                extra_attributes = extra_attributes.items()
            except:
                # Do nothing if it did not work, expect the variable is already a list
                pass
            attr_list = ((self._bundle.valid_identifier(attribute), value) for attribute, value in extra_attributes)
            # Check attributes for valid qualified names
            self._extra_attributes.extend(attr_list)

    def add_attributes(self, attributes, extra_attributes):
        if attributes:
            if self._attributes is None:
                self._attributes = attributes
            else:
                self._attributes.update(attributes)
        self.add_extra_attributes(extra_attributes)

    def get_attributes(self):
        return (self._attributes, self._extra_attributes)

    def get_bundle(self):
        return self._bundle

    def _parse_identifier(self, value):
        try:
            return value.get_identifier()
        except:
            return self._bundle.valid_identifier(value)

    def _parse_record(self, attribute, attribute_types):
        # check to see if there is an existing record matching the attribute (as the record's identifier)
        existing_record = self._bundle.get_record(attribute)
        if existing_record is None:
            existing_record = self._bundle.get_bundle(attribute)
        if existing_record and isinstance(existing_record, attribute_types):
            return existing_record
        else:
            return None

    def _parse_attribute(self, attribute, attribute_types):
        if attribute_types is Identifier:
            return self._parse_identifier(attribute)
            # attempt to find an existing record
        record = self._parse_record(attribute, attribute_types)
        if record:
            return record
        else:
            # It is not a record, try to parse it with known datatype parsers
            if isinstance(attribute_types, collections.Iterable):
                for datatype in attribute_types:
                    data = parse_datatype(attribute, datatype)
                    if data:
                        return data
            else:
                # only one datatype provided
                datatype = attribute_types
                try:
                    return parse_datatype(attribute, datatype)
                except:
                    return None
        return None

    def _validate_attribute(self, attribute, attribute_types):
        if isinstance(attribute, attribute_types):
            # The attribute is of a required type
            # Return it
            return attribute
        else:
            # The attribute is not of a valid type
            # Attempt to parse it
            parsed_value = self._parse_attribute(attribute, attribute_types)
            if parsed_value is None:
                raise ProvExceptionNotValidAttribute(self.get_type(), attribute, attribute_types)
            return parsed_value


    def required_attribute(self, attributes, attribute_id, attribute_types):
        if attribute_id not in attributes:
            # Raise an exception about the missing attribute
            raise ProvExceptionMissingRequiredAttribute(self.get_type(), attribute_id)
            # Found the required attribute
        attribute = attributes.get(attribute_id)
        return self._validate_attribute(attribute, attribute_types)

    def optional_attribute(self, attributes, attribute_id, attribute_types):
        if attribute_id not in attributes:
            # Because this is optional, return nothing
            return None
            # Found the optional attribute
        attribute = attributes.get(attribute_id)
        if attribute is None:
            return None
            # Validate its type
        return self._validate_attribute(attribute, attribute_types)


    def __eq__(self, other):
        if self.__class__ <> other.__class__:
            return False
        if self._identifier and not (self._identifier == other._identifier):
            return False
        if self._attributes and other._attributes:
            if len(self._attributes) <> len(other._attributes):
                return False
            for attr, value_a in self._attributes.items():
                value_b = other._attributes[attr]
                if isinstance(value_a, ProvRecord) and value_a._identifier:
                    if not (value_a._identifier == value_b._identifier):
                        return False
                elif not (value_a == value_b):
                    return False
        elif self._attributes <> other._attributes:
            return False
        if self._extra_attributes <> other._extra_attributes:
            return False
        return True

    def __str__(self):
        return self.get_provn()

    def get_provn(self, _indent_level=0):
        items = []
        if self._identifier:
            items.append(str(self._identifier))
        if self._attributes:
            for (attr, value) in self._attributes.items():
                if value is None:
                    items.append('-')
                else:
                    if isinstance(value, ProvRecord):
                        record_id = value.get_identifier()
                        items.append(str(record_id))
                    else:
                        # Assuming this is a datetime or QName value
                        items.append(value.isoformat() if isinstance(value, datetime.datetime) else str(value))

        if self._extra_attributes:
            extra = []
            for (attr, value) in self._extra_attributes:
                try:
                    provn_represenation = value.provn_representation()
                except:
                    if isinstance(value, basestring):
                        provn_represenation = '"%s"' % value
                    else:
                        provn_represenation = '"%s %%%% xsd:dateTime"' % value.isoformat() if isinstance(value, datetime.datetime) else str(value)
                extra.append('%s=%s' % (str(attr), provn_represenation))
            if extra:
                items.append('[%s]' % ', '.join(extra))

        return '%s(%s)' % (PROV_N_MAP[self.get_type()], ', '.join(items))

    def is_element(self):
        return False

    def is_relation(self):
        return False

class ProvElement(ProvRecord):
    def is_element(self):
        return True

class ProvRelation(ProvRecord):
    def is_relation(self):
        return True


### Component 1: Entities and Activities

class ProvEntity(ProvElement):
    def get_type(self):
        return PROV_REC_ENTITY

    def get_prov_type(self):
        return PROV['Entity']


class ProvActivity(ProvElement):
    def get_type(self):
        return PROV_REC_ACTIVITY

    def get_prov_type(self):
        return PROV['Activity']

    def add_attributes(self, attributes, extra_attributes):
        startTime = self.optional_attribute(attributes, PROV_ATTR_STARTTIME, datetime.datetime)
        endTime = self.optional_attribute(attributes, PROV_ATTR_ENDTIME, datetime.datetime)
        if startTime and endTime and startTime > endTime:
            #TODO Raise logic exception here
            pass
        attributes = OrderedDict()
        attributes[PROV_ATTR_STARTTIME] = startTime
        attributes[PROV_ATTR_ENDTIME] = endTime

        ProvElement.add_attributes(self, attributes, extra_attributes)

    # Convenient methods
    def set_time(self, startTime=None, endTime=None):
        # The _attributes dict should be initialised
        self._attributes[PROV_ATTR_STARTTIME] = startTime
        self._attributes[PROV_ATTR_ENDTIME] = endTime

class ProvGeneration(ProvRelation):
    def get_type(self):
        return PROV_REC_GENERATION

    def get_prov_type(self):
        return PROV['Generation']

    def add_attributes(self, attributes, extra_attributes):
        # Required attributes
        entity = self.required_attribute(attributes, PROV_ATTR_ENTITY, ProvEntity)
        # Optional attributes
        activity = self.optional_attribute(attributes, PROV_ATTR_ACTIVITY, ProvActivity)
        time = self.optional_attribute(attributes, PROV_ATTR_TIME, datetime.datetime)

        attributes = OrderedDict()
        attributes[PROV_ATTR_ENTITY] = entity
        attributes[PROV_ATTR_ACTIVITY] = activity
        attributes[PROV_ATTR_TIME] = time

        ProvRelation.add_attributes(self, attributes, extra_attributes)


class ProvUsage(ProvRelation):
    def get_type(self):
        return PROV_REC_USAGE

    def get_prov_type(self):
        return PROV['Usage']

    def add_attributes(self, attributes, extra_attributes):
        # Required attributes
        activity = self.required_attribute(attributes, PROV_ATTR_ACTIVITY, ProvActivity)
        # Optional attributes
        entity = self.optional_attribute(attributes, PROV_ATTR_ENTITY, ProvEntity)
        time = self.optional_attribute(attributes, PROV_ATTR_TIME, datetime.datetime)

        attributes = OrderedDict()
        attributes[PROV_ATTR_ACTIVITY] = activity
        attributes[PROV_ATTR_ENTITY] = entity
        attributes[PROV_ATTR_TIME] = time
        ProvRelation.add_attributes(self, attributes, extra_attributes)

class ProvCommunication(ProvRelation):
    def get_type(self):
        return PROV_REC_COMMUNICATION

    def get_prov_type(self):
        return PROV['Communication']

    def add_attributes(self, attributes, extra_attributes):
        # Required attributes
        informed = self.required_attribute(attributes, PROV_ATTR_INFORMED, ProvActivity)
        informant = self.required_attribute(attributes, PROV_ATTR_INFORMANT, ProvActivity)

        attributes = OrderedDict()
        attributes[PROV_ATTR_INFORMED] = informed
        attributes[PROV_ATTR_INFORMANT] = informant
        ProvRelation.add_attributes(self, attributes, extra_attributes)

class ProvStart(ProvRelation):
    def get_type(self):
        return PROV_REC_START

    def get_prov_type(self):
        return PROV['Start']

    def add_attributes(self, attributes, extra_attributes):
        # Required attributes
        activity = self.required_attribute(attributes, PROV_ATTR_ACTIVITY, ProvActivity)
        # Optional attributes
        trigger = self.optional_attribute(attributes, PROV_ATTR_TRIGGER, ProvEntity)
        starter = self.optional_attribute(attributes, PROV_ATTR_STARTER, ProvActivity)
        time = self.optional_attribute(attributes, PROV_ATTR_TIME, datetime.datetime)

        attributes = OrderedDict()
        attributes[PROV_ATTR_ACTIVITY] = activity
        attributes[PROV_ATTR_TRIGGER] = trigger
        attributes[PROV_ATTR_STARTER] = starter
        attributes[PROV_ATTR_TIME] = time
        ProvRelation.add_attributes(self, attributes, extra_attributes)

class ProvEnd(ProvRelation):
    def get_type(self):
        return PROV_REC_END

    def get_prov_type(self):
        return PROV['End']

    def add_attributes(self, attributes, extra_attributes):
        # Required attributes
        activity = self.required_attribute(attributes, PROV_ATTR_ACTIVITY, ProvActivity)
        # Optional attributes
        trigger = self.optional_attribute(attributes, PROV_ATTR_TRIGGER, ProvEntity)
        ender = self.optional_attribute(attributes, PROV_ATTR_ENDER, ProvActivity)
        time = self.optional_attribute(attributes, PROV_ATTR_TIME, datetime.datetime)

        attributes = OrderedDict()
        attributes[PROV_ATTR_ACTIVITY] = activity
        attributes[PROV_ATTR_TRIGGER] = trigger
        attributes[PROV_ATTR_ENDER] = ender
        attributes[PROV_ATTR_TIME] = time
        ProvRelation.add_attributes(self, attributes, extra_attributes)


class ProvInvalidation(ProvRelation):
    def get_type(self):
        return PROV_REC_INVALIDATION

    def get_prov_type(self):
        return PROV['Invalidation']

    def add_attributes(self, attributes, extra_attributes):
        # Required attributes
        entity = self.required_attribute(attributes, PROV_ATTR_ENTITY, ProvEntity)
        # Optional attributes
        activity = self.optional_attribute(attributes, PROV_ATTR_ACTIVITY, ProvActivity)
        time = self.optional_attribute(attributes, PROV_ATTR_TIME, datetime.datetime)
        # Constraint: activity, time, and extra_attributes cannot be missing at the same time
        if (activity is None) and (time is None) and (not extra_attributes):
            raise ProvException(u'At least one of "actitivy", "time", or "extra_attributes" must be present in an Invalidation assertion.')

        attributes = OrderedDict()
        attributes[PROV_ATTR_ENTITY] = entity
        attributes[PROV_ATTR_ACTIVITY] = activity
        attributes[PROV_ATTR_TIME] = time
        ProvRelation.add_attributes(self, attributes, extra_attributes)


### Component 2: Derivations

class ProvDerivation(ProvRelation):
    def get_type(self):
        return PROV_REC_DERIVATION

    def get_prov_type(self):
        return PROV['Derivation']

    def add_attributes(self, attributes, extra_attributes):
        # Required attributes
        generatedEntity = self.required_attribute(attributes, PROV_ATTR_GENERATED_ENTITY, ProvEntity)
        usedEntity = self.required_attribute(attributes, PROV_ATTR_USED_ENTITY, ProvEntity)
        # Optional attributes
        activity = self.optional_attribute(attributes, PROV_ATTR_ACTIVITY, ProvActivity)
        generation = self.optional_attribute(attributes, PROV_ATTR_GENERATION, ProvGeneration)
        usage = self.optional_attribute(attributes, PROV_ATTR_USAGE, ProvUsage)

        attributes = OrderedDict()
        attributes[PROV_ATTR_GENERATED_ENTITY]= generatedEntity
        attributes[PROV_ATTR_USED_ENTITY]= usedEntity
        attributes[PROV_ATTR_ACTIVITY]= activity
        attributes[PROV_ATTR_GENERATION] = generation
        attributes[PROV_ATTR_USAGE] = usage
        ProvRelation.add_attributes(self, attributes, extra_attributes)


### Component 3: Agents, Responsibility, and Influence

class ProvAgent(ProvElement):
    def get_type(self):
        return PROV_REC_AGENT

    def get_prov_type(self):
        return PROV['Agent']


class ProvAttribution(ProvRelation):
    def get_type(self):
        return PROV_REC_ATTRIBUTION

    def get_prov_type(self):
        return PROV['Attribution']

    def add_attributes(self, attributes, extra_attributes):
        # Required attributes
        entity = self.required_attribute(attributes, PROV_ATTR_ENTITY, ProvEntity)
        agent = self.required_attribute(attributes, PROV_ATTR_AGENT, (ProvAgent, ProvEntity))

        attributes = OrderedDict()
        attributes[PROV_ATTR_ENTITY] = entity
        attributes[PROV_ATTR_AGENT] = agent
        ProvRelation.add_attributes(self, attributes, extra_attributes)

class ProvAssociation(ProvRelation):
    def get_type(self):
        return PROV_REC_ASSOCIATION

    def get_prov_type(self):
        return PROV['Association']

    def add_attributes(self, attributes, extra_attributes):
        # Required attributes
        activity = self.required_attribute(attributes, PROV_ATTR_ACTIVITY, ProvActivity)
        # Optional attributes
        agent = self.optional_attribute(attributes, PROV_ATTR_AGENT, (ProvAgent, ProvEntity))
        plan = self.optional_attribute(attributes, PROV_ATTR_PLAN, ProvEntity)

        attributes = OrderedDict()
        attributes[PROV_ATTR_ACTIVITY]= activity
        attributes[PROV_ATTR_AGENT]= agent
        attributes[PROV_ATTR_PLAN]= plan
        ProvRelation.add_attributes(self, attributes, extra_attributes)

    def get_provn(self, _indent_level=0):
        items = []
        if self._attributes:
            items.append(str(self._attributes[PROV_ATTR_ACTIVITY].get_identifier()))
            agent_id = self._attributes[PROV_ATTR_AGENT].get_identifier()
            if PROV_ATTR_PLAN in self._attributes and self._attributes[PROV_ATTR_PLAN]:
                plan_id = self._attributes[PROV_ATTR_PLAN].get_identifier()
                items.append('%s @ %s' % (str(agent_id), str(plan_id)))
            else:
                items.append(str(agent_id))
        if self._extra_attributes:
            extra = []
            for (attr, value) in self._extra_attributes:
                extra.append('%s="%s"' % (str(attr), '%s %%%% xsd:dateTime' % value.isoformat() if isinstance(value, datetime.datetime) else str(value)))
            if extra:
                items.append('[%s]' % ', '.join(extra))

        return '%s(%s)' % (PROV_N_MAP[self.get_type()], ', '.join(items))


class ProvDelegation(ProvRelation):
    def get_type(self):
        return PROV_REC_DELEGATION

    def get_prov_type(self):
        return PROV['Delegation']

    def add_attributes(self, attributes, extra_attributes):
        # Required attributes
        delegate = self.required_attribute(attributes, PROV_ATTR_DELEGATE, (ProvAgent, ProvEntity))
        responsible = self.required_attribute(attributes, PROV_ATTR_RESPONSIBLE, (ProvAgent, ProvEntity))
        # Optional attributes
        activity = self.optional_attribute(attributes, PROV_ATTR_ACTIVITY, ProvActivity)

        attributes = OrderedDict()
        attributes[PROV_ATTR_DELEGATE] = delegate
        attributes[PROV_ATTR_RESPONSIBLE] = responsible
        attributes[PROV_ATTR_ACTIVITY]= activity
        ProvRelation.add_attributes(self, attributes, extra_attributes)

class ProvInfluence(ProvRelation):
    def get_type(self):
        return PROV_REC_INFLUENCE

    def get_prov_type(self):
        return PROV['Influence']

    def add_attributes(self, attributes, extra_attributes):
        # Required attributes
        influencee = self.required_attribute(attributes, PROV_ATTR_INFLUENCEE, (ProvAgent, ProvEntity))
        influencer = self.required_attribute(attributes, PROV_ATTR_INFLUENCER, (ProvAgent, ProvEntity))
        # Optional attributes
        activity = self.optional_attribute(attributes, PROV_ATTR_ACTIVITY, ProvActivity)

        attributes = OrderedDict()
        attributes[PROV_ATTR_INFLUENCEE] = influencee
        attributes[PROV_ATTR_INFLUENCER] = influencer
        attributes[PROV_ATTR_ACTIVITY]= activity
        ProvRelation.add_attributes(self, attributes, extra_attributes)


### Component 4: Bundles

# See below

### Component 5: Alternate Entities

class ProvSpecialization(ProvRelation):
    def get_type(self):
        return PROV_REC_SPECIALIZATION

    def get_prov_type(self):
        return PROV['Specialization']

    def add_attributes(self, attributes, extra_attributes):
        # Required attributes
        specificEntity = self.required_attribute(attributes, PROV_ATTR_SPECIFIC_ENTITY, ProvEntity)
        generalEntity = self.required_attribute(attributes, PROV_ATTR_GENERAL_ENTITY, ProvEntity)

        attributes = OrderedDict()
        attributes[PROV_ATTR_SPECIFIC_ENTITY]= specificEntity
        attributes[PROV_ATTR_GENERAL_ENTITY]= generalEntity
        ProvRelation.add_attributes(self, attributes, extra_attributes)


class ProvAlternate(ProvRelation):
    def get_type(self):
        return PROV_REC_ALTERNATE

    def get_prov_type(self):
        return PROV['Alternate']

    def add_attributes(self, attributes, extra_attributes):
        # Required attributes
        alternate1 = self.required_attribute(attributes, PROV_ATTR_ALTERNATE1, ProvEntity)
        alternate2 = self.required_attribute(attributes, PROV_ATTR_ALTERNATE2, ProvEntity)

        attributes = OrderedDict()
        attributes[PROV_ATTR_ALTERNATE1]= alternate1
        attributes[PROV_ATTR_ALTERNATE2]= alternate2
        ProvRelation.add_attributes(self, attributes, extra_attributes)

class ProvMention(ProvSpecialization):
    def get_type(self):
        return PROV_REC_MENTION

    def get_prov_type(self):
        return PROV['Mention']

    def add_attributes(self, attributes, extra_attributes):
        # Required attributes
        specificEntity = self.required_attribute(attributes, PROV_ATTR_SPECIFIC_ENTITY, ProvEntity)
        generalEntity = self.required_attribute(attributes, PROV_ATTR_GENERAL_ENTITY, Identifier)
        bundle = self.required_attribute(attributes, PROV_ATTR_BUNDLE, Identifier)
        #=======================================================================
        # # This is disabled so that mentionOf can refer to bundle that is not defined in the same place
        # bundle = self.required_attribute(attributes, PROV_ATTR_BUNDLE, ProvBundle)
        # # Check if generalEntity is in the bundle
        # if generalEntity.get_bundle() is not bundle:
        #    raise ProvExceptionContraint(PROV_REC_MENTION, generalEntity, bundle, 'The generalEntity must belong to the bundle')
        #=======================================================================

        attributes = OrderedDict()
        attributes[PROV_ATTR_SPECIFIC_ENTITY]= specificEntity
        attributes[PROV_ATTR_GENERAL_ENTITY]= generalEntity
        attributes[PROV_ATTR_BUNDLE]= bundle
        ProvRelation.add_attributes(self, attributes, extra_attributes)


### Component 6: Collections

class ProvMembership(ProvRelation):
    def get_type(self):
        return PROV_REC_MEMBERSHIP

    def get_prov_type(self):
        return PROV['Membership']

    def add_attributes(self, attributes, extra_attributes):
        # Required attributes
        collection = self.required_attribute(attributes, PROV_ATTR_COLLECTION, ProvEntity)
        entity = self.required_attribute(attributes, PROV_ATTR_ENTITY, ProvEntity)

        attributes = OrderedDict()
        attributes[PROV_ATTR_COLLECTION]= collection
        attributes[PROV_ATTR_ENTITY]= entity
        ProvRelation.add_attributes(self, attributes, extra_attributes)

# Class mappings from PROV record type
PROV_REC_CLS = {
    PROV_REC_ENTITY                 : ProvEntity,
    PROV_REC_ACTIVITY               : ProvActivity,
    PROV_REC_GENERATION             : ProvGeneration,
    PROV_REC_USAGE                  : ProvUsage,
    PROV_REC_COMMUNICATION          : ProvCommunication,
    PROV_REC_START                  : ProvStart,
    PROV_REC_END                    : ProvEnd,
    PROV_REC_INVALIDATION           : ProvInvalidation,
    PROV_REC_DERIVATION             : ProvDerivation,
    PROV_REC_AGENT                  : ProvAgent,
    PROV_REC_ATTRIBUTION            : ProvAttribution,
    PROV_REC_ASSOCIATION            : ProvAssociation,
    PROV_REC_DELEGATION             : ProvDelegation,
    PROV_REC_INFLUENCE              : ProvInfluence,
    PROV_REC_SPECIALIZATION         : ProvSpecialization,
    PROV_REC_ALTERNATE              : ProvAlternate,
    PROV_REC_MENTION                : ProvMention,
    PROV_REC_MEMBERSHIP             : ProvMembership,
    }


# Bundle
class NamespaceManager(dict):
    def __init__(self, default_namespaces={}, default=None):
        self._default_namespaces = {}
        self._default_namespaces.update(default_namespaces)
        self._namespaces = {}
        self.update(self._default_namespaces)
        self._default = default
        # TODO check if default is in the default namespaces
        self._anon_id_count = 0
        self._rename_map = {}

    def get_namespace(self, uri):
        for namespace in self.values():
            if uri == namespace._uri:
                return namespace
        return None

    def get_registered_namespaces(self):
        return self._namespaces.values()

    def set_default_namespace(self, uri):
        self._default = Namespace('', uri)
        self[''] = self._default

    def get_default_namespace(self):
        return self._default

    def add_namespace(self, namespace):
        if namespace in self.values():
            # no need to do anything
            return
        if namespace in self._rename_map:
            # already renamed and added
            return

        prefix = namespace.get_prefix()
        if prefix in self:
            # Conflicting prefix
            new_prefix = self._get_unused_prefix(prefix)
            new_namespace = Namespace(new_prefix, namespace.get_uri())
            self._rename_map[namespace] = new_namespace
            prefix = new_prefix
            namespace = new_namespace
        self._namespaces[prefix] = namespace
        self[prefix] = namespace

    def get_valid_identifier(self, identifier):
        if not identifier:
            return None
        if isinstance(identifier, Identifier):
            if isinstance(identifier, QName):
                # Register the namespace if it has not been registered before
                namespace = identifier.get_namespace()
                if namespace not in self.values():
                    self.add_namespace(namespace)
                    # return the original identifier
            return identifier
        elif isinstance(identifier, (str, unicode)):
            if identifier.startswith('_:'):
                return None
            elif ':' in identifier:
                # check if the identifier contains a registered prefix
                prefix, local_part = identifier.split(':', 1)
                if prefix in self:
                    # return a new QName
                    return self[prefix][local_part]
                else:
                    # treat as a URI (with the first part as its scheme)
                    # check if the URI can be compacted
                    for namespace in self.values():
                        if identifier.startswith(namespace.get_uri()):
                            # create a QName with the namespace
                            return namespace[identifier.replace(namespace.get_uri(), '')]
                            # return an Identifier with the given URI
                    return Identifier(identifier)
            elif self._default:
                # create and return an identifier in the default namespace
                return self._default[identifier]
            else:
                # TODO Should an exception raised here
                return Identifier(identifier)

    def get_anonymous_identifier(self, local_prefix='id'):
        self._anon_id_count += 1
        return Identifier('_:%s%d' % (local_prefix, self._anon_id_count))

    def _get_unused_prefix(self, original_prefix):
        if original_prefix not in self:
            return original_prefix
        count = 1
        while True:
            new_prefix = '_'.join((original_prefix, count))
            if new_prefix in self:
                count += 1
            else:
                return new_prefix


class ProvBundle(ProvEntity):
    def __init__(self, bundle=None, identifier=None, attributes=None, other_attributes=None):
        # Initializing bundle-specific attributes
        self._records = list()
        self._id_map = dict()
        self._bundles = dict()
        if bundle is None:
            self._namespaces = NamespaceManager({ PROV.get_prefix(): PROV, XSD.get_prefix(): XSD})
        else:
            self._namespaces = bundle._namespaces

        # Initializing record-specific attributes
        super(ProvBundle, self).__init__(bundle, identifier, attributes, other_attributes)

    # Bundle configurations
    def set_default_namespace(self, uri):
        self._namespaces.set_default_namespace(uri)

    def get_default_namespace(self):
        return self._namespaces.get_default_namespace()

    def add_namespace(self, namespace_or_prefix, uri=None):
        if uri is None:
            self._namespaces.add_namespace(namespace_or_prefix)
        else:
            self._namespaces.add_namespace(Namespace(namespace_or_prefix, uri))

    def get_registered_namespaces(self):
        return self._namespaces.get_registered_namespaces()

    def valid_identifier(self, identifier):
        return self._namespaces.get_valid_identifier(identifier)

    def get_anon_id(self, record):
        #TODO Implement a dict of self-generated anon ids for records without identifier
        return self._namespaces.get_anonymous_identifier()

    def get_records(self):
        return self._records

    def get_record(self, identifier):
        try:
            valid_id = self.valid_identifier(identifier)
            return self._id_map[valid_id]
        except:
            # looking up the parent bundle
            if self._bundle is not None:
                return self._bundle.get_record(valid_id)
            else:
                return None

    def get_bundle(self, identifier):
        try:
            valid_id = self.valid_identifier(identifier)
            return self._bundles[valid_id]
        except:
            # looking up the parent bundle
            if self._bundle is not None:
                return self._bundle.get_bundle(valid_id)
            else:
                return None

    # PROV-JSON serialization/deserialization
    class JSONEncoder(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, ProvBundle):
                return o._encode_JSON_container()
            else:
                # Use the default encoder instead
                return json.JSONEncoder.default(self, o)

    class JSONDecoder(json.JSONDecoder):
        def decode(self, s):
            json_container = json.JSONDecoder.decode(self, s)
            result = ProvBundle()
            result._decode_JSON_container(json_container)
            return result

    def _encode_json_representation(self, value):
        try:
            return value.json_representation()
        except AttributeError:
            if isinstance(value, datetime.datetime):
                return u'"%s"^^%s' % (value.isoformat(), u'xsd:dateTime')
            else:
                return value

    def _decode_json_representation(self, value):
        try:
            # If the value is a string
            # try matching a typed literal with uri pattern
            m = _r_typed_literal_uri.match(value)
            if m is None:
                # try matching a typed literal with qname pattern
                m = _r_typed_literal_qname.match(value)
            if m is not None:
                # found one of the typed literal patterns
                component = m.groupdict()
                value_str = component['value']
                datatype = component['datatype']
                # Check for common data types
                # TODO Add a proper XSD datatype converter to replace this
                if datatype == u'xsd:anyURI':
                    return Identifier(value_str)
                elif datatype == u'xsd:QName':
                    return self.valid_identifier(value_str)
                elif datatype == u'xsd:dateTime':
                    return parse_xsd_dateTime(value_str)
                else:
                    return Literal(value_str, self.valid_identifier(datatype))
            else:
                # cannot match the patterns, just return the string
                return value
        except:
            # not a string, just return it
            return value

    def _encode_JSON_container(self):
        container = defaultdict(dict)

        if self._bundle is None:
            # This is the top-level bundle, we need to define namespaces
            prefixes = {}
            for namespace in self._namespaces.get_registered_namespaces():
                prefixes[namespace.get_prefix()] = namespace.get_uri()
            if self._namespaces._default:
                prefixes['$'] = self._namespaces._default.get_uri()
            container[u'prefix'] = prefixes

        ids = {}
        # generating/mapping all record identifiers
        for record in self._records:
            ids[record] = record._identifier if record._identifier else self.get_anon_id(record)
        for record in self._records:
            rec_type = record.get_type()
            rec_label = PROV_N_MAP[rec_type]
            identifier = str(ids[record])

            if rec_type == PROV_REC_BUNDLE:
                # encoding the sub-bundle
                record_json = record._encode_JSON_container()
            else:
                record_json = {}
                if record._attributes:
                    for (attr, value) in record._attributes.items():
                        if isinstance(value, ProvRecord):
                            attr_record_id = ids[value]
                            record_json[PROV_ID_ATTRIBUTES_MAP[attr]] = str(attr_record_id)
                        elif value is not None:
                            # Assuming this is a datetime value
                            record_json[PROV_ID_ATTRIBUTES_MAP[attr]] = value.isoformat() if isinstance(value, datetime.datetime) else str(value)
                if record._extra_attributes:
                    for (attr, value) in record._extra_attributes:
                        attr_id = str(attr)
                        value_json = self._encode_json_representation(value)
                        if attr_id in record_json:
                            # Multi-value attribute
                            existing_value = record_json[attr_id]
                            try:
                                # Add the value to the current list of values
                                existing_value.add(value_json)
                            except:
                                # But if the existing value is not a list, it'll fail
                                # create the list for the existing value and the second value
                                record_json[attr_id] = [existing_value, value_json]
                        else:
                            record_json[attr_id] = value_json
            container[rec_label][identifier] = record_json

        return container

    def _decode_JSON_container(self, jc):
        if u'prefix' in jc:
            prefixes = jc[u'prefix']
            for prefix, uri in prefixes.items():
                if prefix <> '$':
                    self.add_namespace(Namespace(prefix, uri))
                else:
                    self.set_default_namespace(uri)
        records = sorted([(PROV_RECORD_IDS_MAP[rec_type], rec_id, jc[rec_type][rec_id])
        for rec_type in jc if rec_type <> u'prefix'
        for rec_id in jc[rec_type]],
            key=lambda tuple_rec: tuple_rec[0])

        record_map = {}
        # Create all the records before setting their attributes
        for (record_type, identifier, content) in records:
            if record_type == PROV_REC_BUNDLE:
                bundle = self.bundle(identifier)
                bundle._decode_JSON_container(content)
            else:
                record_map[identifier] = self.add_record(record_type, identifier, None, None)
        for (record_type, identifier, attributes) in records:
            if record_type <> PROV_REC_BUNDLE:
                record = record_map[identifier]
                prov_attributes = {}
                extra_attributes = []
                # Splitting PROV attributes and the others
                for attr, value in attributes.items():
                    if attr in PROV_ATTRIBUTES_ID_MAP:
                        prov_attributes[PROV_ATTRIBUTES_ID_MAP[attr]] = record_map[value] if (isinstance(value, (str, unicode)) and value in record_map) else self._decode_json_representation(value)
                    else:
                        attr_id = self.valid_identifier(attr)
                        if isinstance(value, list):
                            # Parsing multi-value attribute
                            extra_attributes.append((attr_id, self._decode_json_representation(value_single)) for value_single in value)
                        else:
                            # add the single-value attribute
                            extra_attributes.append((attr_id, self._decode_json_representation(value)))
                record.add_attributes(prov_attributes, extra_attributes)

    # Miscellaneous functions
    def get_type(self):
        return PROV_REC_BUNDLE

    def get_provn(self, _indent_level=0):
        indentation = '' +  ('  ' * _indent_level)
        newline = '\n' + ('  ' * (_indent_level + 1))
        if self._bundle is None:
            records = ['bundle']
            # print out prefixes in the top-level bundle
            records.extend(['prefix %s <%s>' % (namespace.get_prefix(), namespace.get_uri()) for namespace in self._namespaces.get_registered_namespaces()])
            # a blank line between the prefixes and the assertions
            records.append('')
        else:
            records = ['bundle %s' % self._identifier]

        records.extend([record.get_provn(_indent_level + 1) for record in self._records])
        provn_str = newline.join(records) + '\n' +\
                    indentation + 'endBundle'
        return provn_str

    def __eq__(self, other):
        try:
            other_records = set(other._records)
        except:
            # other is not a bundle
            return False
        this_records = set(self._records)
        if len(this_records) <> len(other_records):
            return False
            # check if all records for equality
        for record_a in this_records:
            if record_a._identifier:
                if record_a.get_type() == PROV_REC_BUNDLE:
                    record_b = other.get_bundle(record_a._identifier)
                else:
                    record_b = other.get_record(record_a._identifier)
                if record_b:
                    if record_a == record_b:
                        other_records.remove(record_b)
                        continue
                    else:
                        logger.debug("Inequal PROV records:")
                        logger.debug("%s" % str(record_a))
                        logger.debug("%s" % str(record_b))
                        return False
                else:
                    logger.debug("Could not find a record with this identifier: %s" % str(record_a._identifier))
                    return False
            else:
                # Manually look for the record
                found = False
                for record_b in other_records:
                    if record_a == record_b:
                        other_records.remove(record_b)
                        found = True
                        break
                if not found:
                    logger.debug("Could not find this record: %s" % str(record_a))
                    return False
        return True

    # Provenance statements
    def add_record(self, record_type, identifier, attributes=None, other_attributes=None):
        new_record = PROV_REC_CLS[record_type](self, self.valid_identifier(identifier), attributes, other_attributes)
        self._records.append(new_record)
        if new_record._identifier:
            if record_type == PROV_REC_BUNDLE:
                # Don't mix bunle ids with normal record ids.
                self._bundles[new_record._identifier] = new_record
            else:
                self._id_map[new_record._identifier] = new_record
        return new_record

    def add_element(self, record_type, identifier, attributes=None, other_attributes=None):
        return self.add_record(record_type, identifier, attributes, other_attributes)

    def entity(self, identifier, other_attributes=None):
        return self.add_element(PROV_REC_ENTITY, identifier, None, other_attributes)

    def activity(self, identifier, startTime=None, endTime=None, other_attributes=None):
        return self.add_element(PROV_REC_ACTIVITY, identifier, {PROV_ATTR_STARTTIME: _ensure_datetime(startTime), PROV_ATTR_ENDTIME: _ensure_datetime(endTime)}, other_attributes)

    def generation(self, entity, activity=None, time=None, identifier=None, other_attributes=None):
        return self.add_record(PROV_REC_GENERATION, identifier, {PROV_ATTR_ENTITY: entity, PROV_ATTR_ACTIVITY: activity, PROV_ATTR_TIME: _ensure_datetime(time)}, other_attributes)

    def usage(self, activity, entity=None, time=None, identifier=None, other_attributes=None):
        return self.add_record(PROV_REC_USAGE, identifier, {PROV_ATTR_ACTIVITY: activity, PROV_ATTR_ENTITY: entity, PROV_ATTR_TIME: _ensure_datetime(time)}, other_attributes)

    def start(self, activity, trigger=None, starter=None, time=None, identifier=None, other_attributes=None):
        return self.add_record(PROV_REC_START, identifier, {PROV_ATTR_ACTIVITY: activity, PROV_ATTR_TRIGGER: trigger, PROV_ATTR_STARTER: starter, PROV_ATTR_TIME: _ensure_datetime(time)}, other_attributes)

    def end(self, activity, trigger=None, ender=None, time=None, identifier=None, other_attributes=None):
        return self.add_record(PROV_REC_END, identifier, {PROV_ATTR_ACTIVITY: activity, PROV_ATTR_TRIGGER: trigger, PROV_ATTR_ENDER: ender, PROV_ATTR_TIME: _ensure_datetime(time)}, other_attributes)

    def invalidation(self, entity, activity=None, time=None, identifier=None, other_attributes=None):
        return self.add_record(PROV_REC_INVALIDATION, identifier, {PROV_ATTR_ENTITY: entity, PROV_ATTR_ACTIVITY: activity, PROV_ATTR_TIME: _ensure_datetime(time)}, other_attributes)

    def communication(self, informed, informant, identifier=None, other_attributes=None):
        return self.add_record(PROV_REC_COMMUNICATION, identifier, {PROV_ATTR_INFORMED: informed, PROV_ATTR_INFORMANT: informant}, other_attributes)

    def agent(self, identifier, other_attributes=None):
        return self.add_element(PROV_REC_AGENT, identifier, None, other_attributes)

    def attribution(self, entity, agent, identifier=None, other_attributes=None):
        return self.add_record(PROV_REC_ATTRIBUTION, identifier, {PROV_ATTR_ENTITY: entity, PROV_ATTR_AGENT: agent}, other_attributes)

    def association(self, activity, agent=None, plan=None, identifier=None, other_attributes=None):
        return self.add_record(PROV_REC_ASSOCIATION, identifier, {PROV_ATTR_ACTIVITY: activity, PROV_ATTR_AGENT: agent, PROV_ATTR_PLAN: plan}, other_attributes)

    def delegation(self, delegate, responsible, activity=None, identifier=None, other_attributes=None):
        return self.add_record(PROV_REC_DELEGATION, identifier, {PROV_ATTR_DELEGATE: delegate, PROV_ATTR_RESPONSIBLE: responsible, PROV_ATTR_ACTIVITY: activity}, other_attributes)

    def derivation(self, generatedEntity, usedEntity, activity=None, generation=None, usage=None, time=None, identifier=None, other_attributes=None):
        attributes = {PROV_ATTR_GENERATED_ENTITY: generatedEntity,
                      PROV_ATTR_USED_ENTITY: usedEntity,
                      PROV_ATTR_ACTIVITY: activity,
                      PROV_ATTR_GENERATION: generation,
                      PROV_ATTR_USAGE: usage}
        return self.add_record(PROV_REC_DERIVATION, identifier, attributes, other_attributes)

    def revision(self, generatedEntity, usedEntity, activity=None, generation=None, usage=None, time=None, identifier=None, other_attributes=None):
        record = self.derivation(generatedEntity, usedEntity, activity, generation, usage, time, identifier, other_attributes)
        record.add_asserted_type(PROV['Revision'])
        return record

    def quotation(self, generatedEntity, usedEntity, activity=None, generation=None, usage=None, time=None, identifier=None, other_attributes=None):
        record = self.derivation(generatedEntity, usedEntity, activity, generation, usage, time, identifier, other_attributes)
        record.add_asserted_type(PROV['Quotation'])
        return record

    def primary_source(self, generatedEntity, usedEntity, activity=None, generation=None, usage=None, time=None, identifier=None, other_attributes=None):
        record = self.derivation(generatedEntity, usedEntity, activity, generation, usage, time, identifier, other_attributes)
        record.add_asserted_type(PROV['PrimarySource'])
        return record

    def specialization(self, specificEntity, generalEntity, identifier=None, other_attributes=None):
        return self.add_record(PROV_REC_SPECIALIZATION, identifier, {PROV_ATTR_SPECIFIC_ENTITY: specificEntity, PROV_ATTR_GENERAL_ENTITY: generalEntity}, other_attributes)

    def alternate(self, alternate1, alternate2, identifier=None, other_attributes=None):
        return self.add_record(PROV_REC_ALTERNATE, identifier, {PROV_ATTR_ALTERNATE1: alternate1, PROV_ATTR_ALTERNATE2: alternate2}, other_attributes)

    def mention(self, specificEntity, generalEntity, bundle, identifier=None, other_attributes=None):
        # Special case for mention: if generalEntity is not already a ProvRecord (e.g. a string identifier),
        # then it might need to be looked up in the bundle provided, not the current bundle
        if isinstance(generalEntity, basestring):
            # try to look for an existing record first:
            existing_record = self.get_record(generalEntity)
            if existing_record:
                # stop looking
                generalEntity = existing_record
            else:
                # looking in the target bundle
                if isinstance(bundle, basestring):
                    bundle = self.get_bundle(bundle)
                generalEntity = bundle.get_record(generalEntity)
        return self.add_record(PROV_REC_MENTION, identifier, {PROV_ATTR_SPECIFIC_ENTITY: specificEntity, PROV_ATTR_GENERAL_ENTITY: generalEntity, PROV_ATTR_BUNDLE: bundle}, other_attributes)

    def membership(self, collection, entity, identifier=None, other_attributes=None):
        return self.add_record(PROV_REC_MEMBERSHIP, identifier, {PROV_ATTR_COLLECTION: collection, PROV_ATTR_ENTITY: entity}, other_attributes)

    def bundle(self, identifier, other_attributes=None):
        return self.add_element(PROV_REC_BUNDLE, identifier, None, other_attributes)

    # Aliases
    wasGeneratedBy = generation
    used = usage
    wasStartedBy = start
    wasEndedBy = end
    wasInvalidatedBy = invalidation
    wasInformedBy = communication
    wasAttributedTo = attribution
    wasAssociatedWith = association
    actedOnBehalfOf = delegation
    wasDerivedFrom = derivation
    wasRevisionOf = revision
    wasQuotedFrom = quotation
    hadPrimarySource = primary_source
    alternateOf = alternate
    specializationOf = specialization
    mentionOf = mention
    hadMember = membership

# Add the newly defined ProvBundle into the PROV class dictionary
PROV_REC_CLS[PROV_REC_BUNDLE] = ProvBundle