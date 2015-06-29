#!/usr/bin/python

# iAddressBook2RDF - convert contacts from an iOS AddressBook to N-Triples
#
# Extract contact information from an iOS AddressBook SQLite database and output
# RDF data in N-Triples format to a file or to the standard output. The output
# RDF graph tries to use the FOAF <http://xmlns.com/foaf/0.1/> vocabulary when
# possible.
#
# @copyright: Copyright (c) 2015 Robert Zavalczki, distributed
# under the terms and conditions of the Lesser GNU General
# Public License version 2.1

import argparse
import sqlite3
import sys

qnames_prefix_map = {
    'rdf' : 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
    'foaf' : 'http://xmlns.com/foaf/0.1/',
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

def escape_literal(s):
    if s is None:
        return ''
    try:
        s = unicode(s)
    except NameError:
        s = str(s)

    s = s.replace("\\", "\\\\")
    s = s.replace("\"", "\\\"")
    s = s.replace("\n", "\\n")
    return s

ab_person_column_map = {
    'ROWID' : None,
    'First' : 'foaf:firstName',
    'Last' : 'foaf:lastName',
    'Middle' : 'foaf:middleName',
    'FirstPhonetic' : 'abp:FirstPhonetic',
    'MiddlePhonetic' : 'abp:MiddlePhonetic',
    'LastPhonetic' : 'abp:LastPhonetic',
    'Organization' : 'abp:Organization',
    'Department' : 'abp:Department',
    'Note' : 'abp:Note',
    'Kind' : 'abp:Kind',
    'Birthday' : 'foaf:birthday',
    'JobTitle' : 'foaf:title',
    'Nickname' : 'foaf:nick',
    'Prefix' : 'abp:Prefix',
    'Suffix' : 'abp:Suffix',
    'FirstSort' : None,
    'LastSort' : None,
    'CreationDate' : 'abp:CreationDate',
    'ModificationDate' : 'abp:ModificationDate',
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
    'PersonLink' : 'abp:PersonLink',
    'ImageURI' : 'foaf:img',
    'IsPreferredName' : None,
    'guid' : 'abp:guid',
    'PhonemeData' : 'abp:PhonemeData'
}

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

class ABPersonToRDF(object):
    def __init__(self, db_name, output_file_name=None):
        self.db_connection = sqlite3.connect(db_name)
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
            blank_node = '_:p%d' % (person_id)
            for i, col in enumerate(col_names):
                qname = self._get_ab_person_column_relation_qname(col, row[i])
                if qname is None:
                    continue
                tripple = '%s %s "%s" .' % (
                                    blank_node,
                                    qname_to_uri(qname),
                                    escape_literal(row[i]))
                self._line_out(tripple)
            self._process_person_multi_values(person_id)

    """ return None if we're not interested in the column value """
    def _get_ab_person_column_relation_qname(self, col_name, col_val):
        if col_val is None:
            return None
        try:
            relation = ab_person_column_map[col_name]
        except KeyError:
            return None
        if relation is None:
            return None
        if relation in [ 'abp:StoreID', 'abp:Kind' ] and col_val == 0:
            return None
        if relation in [ 'abp:PersonLink' ] and col_val == -1:
            return None
        return relation

    def _line_out(self, s):
        # hack for Python 2 / 3 compatibility
        try:
            self.out.write(('%s\n' % (s)).encode('UTF-8'))
        except TypeError:
            self.out.write('%s\n' % (s))

    def _process_person_multi_values(self, person_id):
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
            WHERE mv.record_id=?;
        """

        person_blank = '_:p%d' % (person_id)
        cur = self.db_connection.cursor()
        for row in cur.execute(query, (person_id,)):
            uid = row[0]
            prop_type = row[1]
            if prop_type is None:
                continue
            prop_val = row[3] if row[3] else row[5]
            if not prop_val:
                continue

            if prop_type == 3:
                # process phone number
                self._process_phone_number(person_blank, prop_val, row[4])
            elif prop_type == 4:
                # e-mail address
                trip = '%s %s <mailto:%s> .' % (person_blank,
                                     qname_to_uri('foaf:mbox'),
                                     prop_val)
                self._line_out(trip)
            elif prop_type == 5:
                # street address
                address_blank = '_:p%dad%d' % (person_id, uid)
                trip = '%s %s %s .' % (person_blank,
                                     qname_to_uri('vcard:address'),
                                     address_blank)
                self._line_out(trip)
                qn = get_multi_value_entry_qname(row[6])
                if qn:
                    trip = '%s %s "%s" .' % (address_blank,
                                             qname_to_uri(qn),
                                             escape_literal(prop_val))
                    self._line_out(trip)
            else:
                # unknown property
                prop_name = 'abp:prop_%s' % (prop_type)
                trip = '%s %s "%s" .' % (person_blank,
                                     qname_to_uri(prop_name),
                                     escape_literal(prop_val))
                self._line_out(trip)

    def _process_phone_number(self, person_blank, phone_number, phone_type):
        # process phone number
        phone_uri = '<tel:%s>' % (phone_number.replace(' ', ''))
        trip = '%s %s %s .' % (person_blank,
                             qname_to_uri('foaf:phone'),
                             phone_uri)
        self._line_out(trip)
        # phone type could be like: _$!<Mobile>!$_, _$!<Work>!$_, etc.
        if phone_type and len(phone_type) > 8:
            phone_type = phone_type[4:-4]
            trip = '%s %s %s .' % (phone_uri,
                                 qname_to_uri('rdf:type'),
                                 qname_to_uri('abp:' + phone_type))
            self._line_out(trip)


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
                args.input = gl[-1] if gl else None
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
                args.input = gl[-1] if gl else None

    if not args.input:
        parser.error('Missing input file!')

    converter = ABPersonToRDF(args.input, args.output)
    converter.process_ab_records()
