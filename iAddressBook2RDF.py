#!/usr/bin/python

# iAddressBook2RDF - convert contacts from an iOS AddressBook to N-Triples
#
# Extract contact information from an iOS AddressBook SQLite database and output
# RDF data in N-Triples format to a file or to the standard output. The output
# RDF graph uses the vCard Ontology <http://www.w3.org/2006/vcard/ns#>
# vocabulary when possible.
#
# @copyright: Copyright (c) 2015 Robert Zavalczki, distributed
# under the terms and conditions of the Lesser GNU General
# Public License version 2.1

import argparse
import datetime
import rdflib
import sqlite3
import sys

from _functools import partial
from rdflib.namespace import Namespace, NamespaceManager
from PhoneNumberUtils import normalize_phone_number

# global variables section
_rdf = Namespace('http://www.w3.org/1999/02/22-rdf-syntax-ns#')
_rdfs = Namespace('http://www.w3.org/2000/01/rdf-schema#')
_abp = Namespace('http://www.apple.com/ABPerson#')
_vcard = Namespace('http://www.w3.org/2006/vcard/ns#')
_gn = Namespace('http://www.geonames.org/ontology#')

_namespace_manager = NamespaceManager(rdflib.Graph())
_namespace_manager.bind('rdf', _rdf)
_namespace_manager.bind('rdfs', _rdfs)
_namespace_manager.bind('abp', _abp)
_namespace_manager.bind('vcard', _vcard)
_namespace_manager.bind('gn', _gn)


# convert Apple date fields to ISO 8601 date representation
def apple_date_to_iso_8601(adt, is_timestamp=True):
    if is_timestamp:
        return datetime.datetime.fromtimestamp(
                    978307200 + float(adt)).strftime("%Y-%m-%d %H:%M:%S")

    dt = datetime.datetime.fromtimestamp(978307200 + float(adt))
    return '--%02d-%02d' % (dt.month, dt.day)


""" map ABPerson table columns to RDF predicates """
ab_person_column_map = {
    'ROWID' : None,
    'First' : _vcard['given-name'],
    'Last' : _vcard['family-name'],
    'Middle' : _vcard['additional-name'],
    'FirstPhonetic' : _abp.FirstPhonetic,
    'MiddlePhonetic' : _abp.MiddlePhonetic,
    'LastPhonetic' : _abp.LastPhonetic,
    'Organization' : _vcard['organization-name'],
    'Department' : _vcard['organizational-unit'],
    'Note' : _vcard.note,
    'Kind' : _abp.Kind,
    'Birthday' : _vcard.bday,
    'JobTitle' : _vcard.title,
    'Nickname' : _vcard.nickname,
    'Prefix' : _vcard['honorific-prefix'],
    'Suffix' : _vcard['honorific-suffix'],
    'FirstSort' : None,
    'LastSort' : None,
    'CreationDate' : None,
    'ModificationDate' : _vcard.rev,
    'CompositeNameFallback' : _abp.CompositeNameFallback,
    'ExternalIdentifier' : _abp.ExternalIdentifier,
    'ExternalModificationTag' : None,
    'ExternalUUID' : _abp.ExternalUUID,
    'StoreID' : _abp.StoreID,
    'DisplayName' : _abp.DisplayName,
    'ExternalRepresentation' : _abp.ExternalRepresentation,
    'FirstSortSection' : None,
    'LastSortSection' : None,
    'FirstSortLanguageIndex' : None,
    'LastSortLanguageIndex' : None,
    'PersonLink' : _abp.PersonLink,
    'ImageURI' : _vcard.hasPhoto,
    'IsPreferredName' : None,
    'guid' : _abp['x-abuid'],
    'PhonemeData' : _abp.PhonemeData,
    'AlternateBirthday' : _abp.AlternateBirthday,
    'MapsData' : _abp.MapsData,
    'FirstPronunciation' : _abp.FirstPronunciation,
    'MiddlePronunciation' : _abp.MiddlePronunciation,
    'LastPronunciation' : _abp.LastPronunciation,
    'OrganizationPhonetic' : _abp.OrganizationPhonetic,
    'OrganizationPronunciation' : _abp.OrganizationPronunciation,
    'PreviousFamilyName' : _abp.PreviousFamilyName,
    'PreferredLikenessSource' : _abp.PreferredLikenessSource,
    'PreferredPersonaIdentifier' : _abp.PreferredPersonaIdentifier
}


""" map ABPerson.Kind values vcard:Kind subclasses """
ab_object_kind_map = {
    0 : _vcard.Individual,
    1 : _vcard.Organization
    # 2? : _vcard.Group,
    # 3? : _vcard.Location
}


def get_vcard_object_kind(nkey):
    try:
        return ab_object_kind_map[nkey]
    except KeyError:
        return _vcard.Kind


def _to_telephone_uri(phoneNo):
    return rdflib.URIRef('tel:%s' % (normalize_phone_number(phoneNo)))


def _process_has_telephone(objPropDict, val, _):
    # process phone number
    objPropDict[_vcard.hasValue] = _to_telephone_uri(val)


def _process_has_email(objPropDict, val, _):
    # e-mail address
    objPropDict[_vcard.hasValue] = rdflib.URIRef('mailto:%s' % (val))


def _process_url(objPropDict, val, _):
    objPropDict[_vcard.hasValue] = rdflib.URIRef(val)


def _process_literal(objPropDict, val, _):
    objPropDict[_vcard.hasValue] = rdflib.Literal(val)


def _process_literal_date(objPropDict, val, _):
    objPropDict[_vcard.hasValue] = rdflib.Literal(apple_date_to_iso_8601(val, False))


def _process_multi_value_entry(objPropDict, val, mve_relation):
    assert(mve_relation)
    if mve_relation == _vcard.url:
        objPropDict[mve_relation] = rdflib.URIRef(val)
    else:
        objPropDict[mve_relation] = rdflib.Literal(val)


def _process_unknown_multi_value_entry(rel1, objPropDict, val, _):
    objPropDict[rel1] = rdflib.Literal(val)


def _get_mv_property_type_info(mv_property):
    prop_type_map = {
        3 : (_vcard.hasTelephone, _process_has_telephone),
        4 : (_vcard.hasEmail, _process_has_email),
        5 : (_vcard.hasAddress, _process_multi_value_entry),
        16 : (_vcard.sound, _process_literal),
        22 : (_vcard.url, _process_url),
        23 : (_abp.relatedName, _process_literal),
        12 : (_abp.relatedDate, _process_literal_date),
        46 : (_abp.socialProfile, _process_multi_value_entry),
        13 : (_vcard.hasInstantMessage, _process_multi_value_entry)
    }

    try:
        return prop_type_map[mv_property]
    except KeyError:
        rel1 = _abp['x-prop_%s' % (mv_property)]
        return (rel1, partial(_process_unknown_multi_value_entry, rel1))


def translate_category_label(categ_label):
    # category label could be like: _$!<Mobile>!$_, _$!<Work>!$_, etc.
    # category labels like: iPhone, Twitter, Facebook etc. are not translated
    if not categ_label:
        return []
    if categ_label.startswith('_$!<') and categ_label.endswith('>!$_'):
        categ_label = categ_label[4:-4]

    types_categories = []
    if categ_label == "Mobile":
        types_categories.append((_rdf.type, _vcard.Cell))
    elif categ_label == "Home":
        types_categories.append((_rdf.type, _vcard.Home))
    elif categ_label == "WorkFAX":
        types_categories.append((_rdf.type, _vcard.Work))
        types_categories.append((_rdf.type, _vcard.Fax))
    elif categ_label == "iPhone":
        types_categories.append((_rdf.type, _vcard.Cell))
        types_categories.append((_vcard.category, rdflib.Literal(categ_label)))
    elif categ_label == "Pager":
        types_categories.append((_rdf.type, _vcard.Pager))
    elif categ_label == "Work":
        types_categories.append((_rdf.type, _vcard.Work))
    else:
        types_categories.append((_vcard.category, rdflib.Literal(categ_label)))

    return types_categories


class ABPerson(object):
    def __init__(self, person_id=None):
        self.id = person_id
        self.name = {} # name components: family name, given name, title, etc.
        self.values = {} # single values (birthday, formatted name, organization, etc.)
        self.multivalues = {} # multi values (phone#s, emails, addresses, etc.)

    def __str__(self, *args, **kwargs):
        return object.__str__(self, *args, **kwargs)

    def generate_formatted_name(self):
        fn = ''
        if _vcard['given-name'] in self.name:
            fn = self.name[_vcard['given-name']]
            if fn:
                fn += ' '

        if _vcard['additional-name'] in self.name:
            fn += self.name[_vcard['additional-name']]
            if fn:
                fn += ' '

        if _vcard['family-name'] in self.name:
            fn += self.name[_vcard['family-name']]

        if not fn and _vcard['organization-name'] in self.values:
            fn += self.values[_vcard['organization-name']]

        fn = fn.strip()
        if fn:
            self.values[_vcard.fn] = rdflib.Literal(fn)

    def output_triples(self, rdfGraph):
        person_bnode = rdflib.BNode()
        for k in self.values:
            rdfGraph.add((person_bnode, k, self.values[k]))

        if self.name:
            name_bnode = rdflib.BNode()
            rdfGraph.add((person_bnode, _vcard.hasName, name_bnode))
            for k in self.name:
                rdfGraph.add((name_bnode, k, self.name[k]))

        for k in self.multivalues:
            _, mvrel = k
            mvprops = self.multivalues[k]
            if not mvprops:
                continue
            mvbnode = rdflib.BNode()
            rdfGraph.add((person_bnode, mvrel, mvbnode))
            for m in mvprops:
                rdfGraph.add((mvbnode, m, mvprops[m]))


class ABPersonToRDF(object):
    def __init__(self, db_name, output_file_name=None):
        self.db_connection = sqlite3.connect(db_name)
        self.ab_multi_value_entry_map = self._build_multi_value_entry_map(
                                                            self.db_connection)
        if output_file_name is not None:
            self.out = open(output_file_name, 'wb')
        else:
            try:
                self.out = sys.stdout.buffer
            except AttributeError:
                self.out = sys.stdout

    """ process Address Book records """
    def process_ab_records(self):
        cur = self.db_connection.cursor()
        cur.execute('select * from ABPerson')
        col_names = [k[0] for k in cur.description]

        rdfGraph = rdflib.Graph()
        rdfGraph.namespace_manager = _namespace_manager

        for row in cur:
            person_id = int(row[0])
            person = ABPerson(person_id)

            for i, col in enumerate(col_names):
                self._process_ab_person_column(col, row[i], person)

            self._process_person_multi_values(person)
            person.generate_formatted_name()
            person.output_triples(rdfGraph)

        rdfGraph.serialize(self.out, format='n3', encoding="UTF-8")

    """ process the field of an ABPerson record, store results in person  """
    @staticmethod
    def _process_ab_person_column(col_name, col_val, person):
        if col_val is None:
            return

        try:
            relation = ab_person_column_map[col_name]
        except KeyError:
            return
        if relation is None:
            return
        if relation == _abp.Kind:
            person.values[_rdf.type] = get_vcard_object_kind(col_val)
            return

        if relation == _abp.StoreID and col_val == 0:
            return

        if relation == _abp.PersonLink:
            # col_val points to an entry in the ABPersonLink table
            if col_val == -1:
                return

        # process a name component
        if relation in [_vcard['given-name'], _vcard['family-name'],
                        _vcard['additional-name'], _vcard.title,
                        _vcard['honorific-prefix'], _vcard['honorific-suffix'],
                        _abp.PreviousFamilyName]:
            person.name[relation] = rdflib.Literal(col_val)
            return

        # convert Apple date fields to ISO 8601 date representation
        if relation == _vcard.rev:
            col_val = apple_date_to_iso_8601(col_val, True)
        elif relation == _vcard.bday:
            col_val = apple_date_to_iso_8601(col_val, False)

        if relation in [_vcard.hasPhoto]:
            person.values[relation] = rdflib.URIRef(col_val)
            return

        # set the property
        person.values[relation] = rdflib.Literal(col_val)

    def _build_multi_value_entry_map(self, db_connection):
        query = 'select ROWID, value from ABMultiValueEntryKey'
        cur = db_connection.cursor()
        remap = {
            'Street': _vcard['street-address'],
            'Country': _vcard['country-name'],
            'ZIP': _vcard['postal-code'],
            'City': _vcard.locality,
            'State': _vcard.region,
            'CountryCode': _gn.countryCode,
            'username': _abp.username,
            'service': _abp.service,
            'url': _vcard.url
        }

        mve_map = {}
        for row in cur.execute(query):
            try:
                mve_map[row[0]] = remap[row[1]]
            except KeyError:
                mve_map[row[0]] = _abp[row[1]]
        return mve_map

    def _get_multi_value_entry_rel(self, nkey):
        try:
            return self.ab_multi_value_entry_map[nkey]
        except KeyError:
            return None

    def _process_person_multi_values(self, person):
        # mv.property: 3/phone number 16/ringer 5/address 4/e-mail address
        # 13/instant message, 46/social profile, 22/web-page etc.
        query = """
            SELECT mv.UID as mvid, mv.property as mvtype,
                   mv.label as mvlabel, mv.value as mval,
                   mvl.value as mvlabel2,
                   mve.value as mv_subval, mve.key as mve_key,
                   mvek.value as mve_key2,
                   mv.record_id as person_id
            FROM ABMultiValue mv
            LEFT JOIN ABMultiValueLabel mvl ON
                mvl.ROWID = mv.label
            LEFT JOIN ABMultiValueEntry mve ON
                mve.parent_id = mv.UID
            LEFT JOIN ABMultiValueEntryKey mvek ON
                mvek.ROWID = mve.key
            WHERE mv.record_id=?
            ORDER BY 1;
        """

        last_mv_uid = None
        current_prop_relation = None
        current_prop_func = None
        curPropDict = None

        cur = self.db_connection.cursor()

        for row in cur.execute(query, (person.id,)):
            uid = row[0]
            isNewValue = uid != last_mv_uid

            prop_type = row[1]

            if isNewValue:
                current_prop_relation, current_prop_func = _get_mv_property_type_info(prop_type)
                if not current_prop_relation:
                    curPropDict = None
                    continue

                curPropDict = {}
                person.multivalues[(uid, current_prop_relation)] = curPropDict

                prop_category_label = row[4]
                for pred1, obj1 in translate_category_label(prop_category_label):
                    curPropDict[pred1] = obj1

            elif not current_prop_relation:
                continue

            mval = row[3]
            mv_subval = row[5]
            assert(mval or mv_subval)

            if mv_subval:
                assert(row[6])
                mve_relation = self._get_multi_value_entry_rel(row[6])
            else:
                mve_relation = None

            # call the custom function to set this property
            current_prop_func(curPropDict, mval or mv_subval, mve_relation)

            last_mv_uid = uid


def find_default_database():
    from glob import glob
    search_dir = None
    if sys.platform.startswith('win'):
        try:
            from win32com.shell import shellcon, shell
            search_dir = "%s/%s" % (
                shell.SHGetFolderPath(0, shellcon.CSIDL_APPDATA, 0, 0),
                'Apple Computer/MobileSync/Backup')
        except ImportError:
            return None
    elif sys.platform.startswith('darwin'):
        import os
        home = os.getenv('HOME')
        if not home:
            return None
        search_dir = "%s/%s" % (home,
                'Library/Application Support/MobileSync/Backup')

    if not search_dir:
        return None

    folder_glob = '%s/*/%s' % (search_dir,
                               '31bb7ba8914766d4ba40d6dfb6113c8b614be442')
    gl = glob(folder_glob)
    if not gl:
        folder_glob = '%s/*/31/%s' % (search_dir,
                                      '31bb7ba8914766d4ba40d6dfb6113c8b614be442')

    return gl[0] if gl else None


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                description='Convert contacts from an iOS AddressBook to N-Triples.')
    parser.add_argument('-i', '--input', dest='input',
                help='input iOS AddressBook SQLite 3 database file (default is the iTunes backup if found)')
    parser.add_argument('-o', '--output', dest='output',
                help='output N-Triples (.nt) file name (default console)')

    args = parser.parse_args()
    if not args.input:
        args.input = find_default_database()
        if not args.input:
            parser.error('Missing input file!')

    converter = ABPersonToRDF(args.input, args.output)
    converter.process_ab_records()
