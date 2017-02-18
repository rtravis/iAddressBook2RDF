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
import sqlite3
import sys


qnames_prefix_map = {
    'rdf' : 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
    'rdfs' : 'http://www.w3.org/2000/01/rdf-schema#',
    'abp' : 'http://www.apple.com/ABPerson#',
    'vcard' : 'http://www.w3.org/2006/vcard/ns#',
    'gn' : 'http://www.geonames.org/ontology#'
}

def qname_to_uri(rel):
    p = rel.partition(':')
    if p[1] == ':' and p[0] in qnames_prefix_map:
        return '<%s%s>' % (qnames_prefix_map[p[0]], p[2])
    if not rel.startswith('<'):
        rel = '<' + rel
    if not rel.endswith('>'):
        rel = rel + '>'
    return rel


def format_literal(s):
    if s is None:
        return ''
    s = s.replace("\\", "\\\\")
    s = s.replace("\"", "\\\"")
    s = s.replace("\n", "\\n")
    return '"%s"' % (s)

""" map ABPerson table columns to RDF predicates """
ab_person_column_map = {
    'ROWID' : None,
    'First' : 'vcard:given-name',
    'Last' : 'vcard:family-name',
    'Middle' : 'vcard:additional-name',
    'FirstPhonetic' : 'abp:FirstPhonetic',
    'MiddlePhonetic' : 'abp:MiddlePhonetic',
    'LastPhonetic' : 'abp:LastPhonetic',
    'Organization' : 'vcard:organization-name',
    'Department' : 'vcard:organizational-unit',
    'Note' : 'vcard:note',
    'Kind' : 'abp:Kind',
    'Birthday' : 'vcard:bday',
    'JobTitle' : 'vcard:title',
    'Nickname' : 'vcard:nickname',
    'Prefix' : 'vcard:honorific-prefix',
    'Suffix' : 'vcard:honorific-suffix',
    'FirstSort' : None,
    'LastSort' : None,
    'CreationDate' : None,
    'ModificationDate' : 'vcard:rev',
    'CompositeNameFallback' : 'abp:CompositeNameFallback',
    'ExternalIdentifier' : 'abp:ExternalIdentifier',
    'ExternalModificationTag' : 'abp:ExternalModificationTag',
    'ExternalUUID' : 'abp:ExternalUUID',
    'StoreID' : 'abp:StoreID',
    'DisplayName' : 'abp:DisplayName',
    'ExternalRepresentation' : 'abp:ExternalRepresentation',
    'FirstSortSection' : None,
    'LastSortSection' : None,
    'FirstSortLanguageIndex' : None,
    'LastSortLanguageIndex' : None,
    'PersonLink' : 'vcard:hasURL',
    'ImageURI' : 'vcard:hasPhoto',
    'IsPreferredName' : None,
    'guid' : 'abp:x-abuid',
    'PhonemeData' : 'abp:PhonemeData'
}

""" map ABMultiValueEntry.key values to property names """
ab_multi_value_entry_map = {
    1 : 'vcard:street-address',
    2 : 'vcard:country-name',
    3 : 'vcard:postal-code',
    4 : 'vcard:locality',
    5 : 'vcard:region',
    6 : 'gn:countryCode'
}

def get_multi_value_entry_qname(nkey):
    try:
        return ab_multi_value_entry_map[nkey]
    except KeyError:
        return None

""" map ABPerson.Kind values vcard:Kind subclasses """
ab_object_kind_map = {
    0 : 'vcard:Individual',
    1 : 'vcard:Organization'
    # 2? : 'vcard:Group',
    # 3? : 'vcard:Location'
}

def get_object_kind_qname(nkey):
    try:
        return ab_object_kind_map[nkey]
    except KeyError:
        return 'vcard:Kind'


def output_triple(out, subj, pred, obj):
    if obj.startswith('<') and obj.endswith('>'):
        pass
    elif obj.startswith('"') and obj.endswith('"'):
        pass
    elif obj.startswith('_:'):
        pass
    else:
        obj = qname_to_uri(obj)

    triple = '%s %s %s .' % (
                            subj,
                            qname_to_uri(pred),
                            obj)

    # hack for Python 2 / 3 compatibility
    try:
        out.write(('%s\n' % (triple)).encode('UTF-8'))
    except TypeError:
        out.write('%s\n' % (triple))


class ABPerson(dict):
    def __init__(self, person_id=None):
        super(ABPerson, self).__init__()
        self.id = person_id

    def __str__(self, *args, **kwargs):
        return object.__str__(self, *args, **kwargs)

    def generate_full_name(self):
        fn = ''
        if 'vcard:given-name' in self:
            fn += self['vcard:given-name'].strip('"')
        if fn:
            fn += ' '
        if 'vcard:family-name' in self:
            fn += self['vcard:family-name'].strip('"')
        if not fn and 'vcard:organization-name' in self:
            fn += self['vcard:organization-name'].strip('"')
        fn = fn.strip()
        if fn:
            self['vcard:fn'] = format_literal(fn)

    def output_ntriples(self, out, bnode_tag):
        bnode = '_:%sp%d' % (bnode_tag, self.id)
        for k in self:
            if k.startswith('multival:'):
                mv = self[k]
                rel = mv['mv_relation_name']
                bnode2 = bnode + 'm' + k[len('multival:'):]
                output_triple(out, bnode, rel, bnode2)
                for k2 in mv:
                    if k2 == 'mv_relation_name':
                        continue
                    output_triple(out, bnode2, k2, mv[k2])
            else:
                output_triple(out, bnode, k, self[k])

def multi_value_prop_class_qname(class_name, class_label):
    # class label could be like: _$!<Mobile>!$_, _$!<Work>!$_, etc.
    if class_label and len(class_label) > 8:
        class_label = class_label[4:-4]
        return 'abp:' + class_label


class ABPersonToRDF(object):
    def __init__(self, db_name, output_file_name=None):
        self.db_connection = sqlite3.connect(db_name)
        self.rand_tag = ('%x' % (abs(hash(db_name))))[:8]
        if output_file_name is not None:
            self.out = open(output_file_name, 'wb')
        else:
            self.out = sys.stdout

    """ process Address Book records """
    def process_ab_records(self):
        cur = self.db_connection.cursor()
        cur.execute('select * from ABPerson')
        col_names = [k[0] for k in cur.description]

        for row in cur:
            person_id = int(row[0])
            person = ABPerson(person_id)

            for i, col in enumerate(col_names):
                self._process_ab_person_column(col, row[i], person)

            self._process_person_multi_values(person)
            person.generate_full_name()

            person.output_ntriples(self.out, self.rand_tag)

    """ process the field of an ABPerson record, store results in person  """
    def _process_ab_person_column(self, col_name, col_val, person):
        if col_val is None:
            return

        try:
            relation = ab_person_column_map[col_name]
        except KeyError:
            return
        if relation is None:
            return

        if relation == 'abp:Kind':
            person['rdf:type'] = get_object_kind_qname(col_val)
            return

        if relation == 'abp:StoreID' and col_val == 0:
            return

        if relation == 'vcard:hasURL' and col_val == -1:
            return

        # convert Apple date fields to ISO 8601 date representation
        if relation == 'vcard:rev':
            col_val = datetime.datetime.fromtimestamp(
                        978307200 + float(col_val)).strftime("%Y-%m-%d %H:%M:%S")
        elif relation == 'vcard:bday':
            dt = datetime.datetime.fromtimestamp(978307200 + float(col_val))
            col_val = '--%02d-%02d' % (dt.month, dt.day)

        # set the property
        person[relation] = format_literal(col_val)


    def _line_out(self, s):
        # hack for Python 2 / 3 compatibility
        try:
            self.out.write(('%s\n' % (s)).encode('UTF-8'))
        except TypeError:
            self.out.write('%s\n' % (s))

    def _to_telephone_uri(self, phoneNo):
        phoneNo = phoneNo.replace(' ', '');
        if isinstance(phoneNo, bytes):
            phoneNo = phoneNo.replace(u'\xc2\xa0', '');
        else:
            phoneNo = phoneNo.replace(u'\xa0', '');
        return '<tel:%s>' % (phoneNo)


    def _process_person_multi_values(self, person):
        # mv.property: 3/phone number 16/ringer 5/address 4/e-mail address
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
        cur = self.db_connection.cursor()

        for row in cur.execute(query, (person.id,)):
            uid = row[0]
            prop_type = row[1]
            if prop_type is None:
                continue

            prop_val = row[3] if row[3] else row[5]
            if not prop_val:
                continue

            prop_class_number = row[2]
            prop_class_label = row[4]
            mv_class = multi_value_prop_class_qname(prop_class_number,
                                                    prop_class_label)

            mv_key = 'multival:' + str(uid)
            if mv_key not in person:
                person[mv_key] = {}

            mv_obj = person[mv_key]

            if prop_type == 3:
                # process phone number
                phone_uri = self._to_telephone_uri(prop_val)

                mv_obj['mv_relation_name'] = 'vcard:hasTelephone'
                mv_obj['vcard:hasValue'] = phone_uri
                mv_obj['rdfs:label'] = format_literal(prop_val)
                if mv_class:
                    mv_obj['rdf:type'] = mv_class

            elif prop_type == 4:
                # e-mail address
                email_uri = '<mailto:%s>' % (prop_val)
                mv_obj['mv_relation_name'] = 'vcard:hasEmail'
                mv_obj['vcard:hasValue'] = email_uri
                if mv_class:
                    mv_obj['rdf:type'] = mv_class

            elif prop_type == 5:
                # street address
                if uid != last_mv_uid:
                    mv_obj['mv_relation_name'] = 'vcard:hasAddress'
                    if mv_class:
                        mv_obj['rdf:type'] = mv_class

                qn = get_multi_value_entry_qname(row[6])
                if qn:
                    mv_obj[qn] = format_literal(prop_val)
            else:
                # unknown property
                if uid != last_mv_uid:
                    prop_name = 'abp:prop_%s' % (prop_type)
                    mv_obj['mv_relation_name'] = prop_name
                    if mv_class:
                        mv_obj['rdf:type'] = mv_class
                mv_obj['vcard:hasValue'] = format_literal(prop_val)

            last_mv_uid = uid


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                description='Convert contacts from an iOS AddressBook to N-Triples.')
    parser.add_argument('-i', '--input', dest='input',
                help='input iOS AddressBook SQLite 3 database file (default is the iTunes backup if found)')
    parser.add_argument('-o', '--output', dest='output',
                help='output N-Triples (.nt) file name (default console)')

    args = parser.parse_args()
    if not args.input:
        if sys.platform.startswith('win'):
            try:
                from win32com.shell import shellcon, shell
                from glob import glob
                folder_glob = '%s/%s/*/%s' % (
                    shell.SHGetFolderPath(0, shellcon.CSIDL_APPDATA, 0, 0),
                    'Apple Computer/MobileSync/Backup',
                    '31bb7ba8914766d4ba40d6dfb6113c8b614be442')
                gl = glob(folder_glob)
                args.input = gl[0] if gl else None
            except ImportError:
                pass
        elif sys.platform.startswith('darwin'):
            import os
            home = os.getenv('HOME')
            if home:
                from glob import glob
                folder_glob = '%s/%s/*/%s' % (
                    home,
                    'Library/Application Support/MobileSync/Backup',
                    '31bb7ba8914766d4ba40d6dfb6113c8b614be442')
                gl = glob(folder_glob)
                args.input = gl[0] if gl else None

    if not args.input:
        parser.error('Missing input file!')

    converter = ABPersonToRDF(args.input, args.output)
    converter.process_ab_records()
