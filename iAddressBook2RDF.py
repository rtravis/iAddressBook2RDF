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


try:
    unicode_constructor = unicode
except NameError:
    unicode_constructor = str

def format_literal(s):
    if s is None:
        return ''
    s = unicode_constructor(s).replace("\\", "\\\\")
    s = s.replace("\"", "\\\"")
    s = s.replace("\n", "\\n")
    return '"%s"' % (s)


def format_uri(s):
    return '<' + s + '>'


# convert Apple date fields to ISO 8601 date representation
def apple_date_to_iso_8601(adt, is_timestamp=True):
    if is_timestamp:
        return datetime.datetime.fromtimestamp(
                    978307200 + float(adt)).strftime("%Y-%m-%d %H:%M:%S")

    dt = datetime.datetime.fromtimestamp(978307200 + float(adt))
    return '--%02d-%02d' % (dt.month, dt.day)


def get_local_trunk_prefix_for_country_code(code):
    if not code:
        return ""

    if code[0] == "1":
        # North American Numbering Plan
        return "1"
    elif code[0] == "7":
        # Russian world
        return "8"
    elif code == "36":
        # Hungary
        return "06"
    elif code in ["420", "45", "372", "30", "39", "371", "352", "356", "377",
                  "977", "47", "968", "48", "351", "378", "34", "3906698" ]:
        # no trunk prefix
        return ""
    else:
        # most of Africa, Asia and Europe
        return "0"


def normalize_phone_number(phoneNo, countryCallingCode=None):
    phoneNo = unicode_constructor(phoneNo).lower()
    phoneNo = phoneNo.translate({ord(c) : None for c in u"<>()-\u2011 \t\n\u00a0\u202a\u202c"})
    if phoneNo.startswith("tel:"):
        phoneNo = phoneNo[4:]

    isSpecial = len(phoneNo) < 5 or "*" in phoneNo or "#" in phoneNo

    if not phoneNo.translate({ord(c) : None for c in u"+*#"}).isdigit():
        return None

    isInternational = False

    calling_prefixes = ['0011', '000', '001', '010', '011', '00', '+']
    for i in calling_prefixes:
        if phoneNo.startswith(i):
            phoneNo = phoneNo[len(i):]
            isInternational = True

    if not isInternational:
        isInternational = len(phoneNo) >= 11

    if not isInternational and not isSpecial and countryCallingCode:
        trunk = get_local_trunk_prefix_for_country_code(countryCallingCode)
        if phoneNo.startswith(trunk):
            phoneNo = countryCallingCode + phoneNo[len(trunk):]
            isInternational = True

    if isInternational and not isSpecial:
        return "+" + phoneNo
    else:
        return phoneNo


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
    'ExternalModificationTag' : None,
    'ExternalUUID' : 'abp:ExternalUUID',
    'StoreID' : 'abp:StoreID',
    'DisplayName' : 'abp:DisplayName',
    'ExternalRepresentation' : 'abp:ExternalRepresentation',
    'FirstSortSection' : None,
    'LastSortSection' : None,
    'FirstSortLanguageIndex' : None,
    'LastSortLanguageIndex' : None,
    'PersonLink' : 'abp:PersonLink',
    'ImageURI' : 'vcard:hasPhoto',
    'IsPreferredName' : None,
    'guid' : 'abp:x-abuid',
    'PhonemeData' : 'abp:PhonemeData',
    'AlternateBirthday' : 'abp:AlternateBirthday',
    'MapsData' : 'abp:MapsData',
    'FirstPronunciation' : 'abp:FirstPronunciation',
    'MiddlePronunciation' : 'abp:MiddlePronunciation',
    'LastPronunciation' : 'abp:LastPronunciation',
    'OrganizationPhonetic' : 'abp:OrganizationPhonetic',
    'OrganizationPronunciation' : 'abp:OrganizationPronunciation',
    'PreviousFamilyName' : 'abp:PreviousFamilyName',
    'PreferredLikenessSource' : 'abp:PreferredLikenessSource',
    'PreferredPersonaIdentifier' : 'abp:PreferredPersonaIdentifier'
}


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


def _to_telephone_uri(phoneNo):
    return '<tel:%s>' % (normalize_phone_number(phoneNo))


def _process_has_telephone(objPropDict, val, _):
    # process phone number
    objPropDict['vcard:hasValue'] = _to_telephone_uri(val)
    # objPropDict['rdfs:label'] = format_literal(val)


def _process_has_email(objPropDict, val, _):
    # e-mail address
    objPropDict['vcard:hasValue'] = '<mailto:%s>' % (val)


def _process_url(objPropDict, val, _):
    objPropDict['vcard:hasValue'] = format_uri(val)


def _process_literal(objPropDict, val, _):
    objPropDict['vcard:hasValue'] = format_literal(val)


def _process_literal_date(objPropDict, val, _):
    objPropDict['vcard:hasValue'] = format_literal(apple_date_to_iso_8601(val, False))


def _process_multi_value_entry(objPropDict, val, mve_relation):
    assert(mve_relation)
    if mve_relation == 'vcard:url':
        objPropDict[mve_relation] = format_uri(val)
    else:
        objPropDict[mve_relation] = format_literal(val)


def _get_mv_property_type_qname(mv_property):
    prop_type_map = {
        3 : ('vcard:hasTelephone', _process_has_telephone),
        4 : ('vcard:hasEmail', _process_has_email),
        5 : ('vcard:hasAddress', _process_multi_value_entry),
        16 : ('vcard:sound', _process_literal),
        22 : ('vcard:url', _process_url),
        23 : ('abp:relatedName', _process_literal),
        12 : ('abp:relatedDate', _process_literal_date),
        46 : ('abp:socialProfile', _process_multi_value_entry),
        13 : ('vcard:hasInstantMessage', _process_multi_value_entry)
    }

    try:
        return prop_type_map[mv_property]
    except KeyError:
        return 'abp:x-prop_%s' % (mv_property)


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


def translate_category_label(categ_label):
    # category label could be like: _$!<Mobile>!$_, _$!<Work>!$_, etc.
    # category labels like: iPhone, Twitter, Facebook etc. are not translated
    if not categ_label:
        return None
    if len(categ_label) > 8 and categ_label.startswith('_$!<'):
        categ_label = categ_label[4:-4]

    return '"' + categ_label + '"'



class ABPerson(object):
    def __init__(self, person_id=None):
        self.id = person_id
        self.values = {} # single values (birthday, first name, organization, etc.)
        self.multivalues = {} # multi values (phone#s, emails, addresses, etc.)

    def __str__(self, *args, **kwargs):
        return object.__str__(self, *args, **kwargs)

    def generate_formatted_name(self):
        fn = ''
        if 'vcard:given-name' in self.values:
            fn = self.values['vcard:given-name'].strip('"')
        if fn:
            fn += ' '

        if 'vcard:additional-name' in self.values:
            fn += self.values['vcard:additional-name'].strip('"')
        if fn:
            fn += ' '

        if 'vcard:family-name' in self.values:
            fn += self.values['vcard:family-name'].strip('"')

        if not fn and 'vcard:organization-name' in self.values:
            fn += self.values['vcard:organization-name'].strip('"')

        fn = fn.strip()
        if fn:
            self.values['vcard:fn'] = format_literal(fn)

    def output_ntriples(self, out, bnode_tag):
        person_bnode = '_:%sp%d' % (bnode_tag, self.id)
        for k in self.values:
            output_triple(out, person_bnode, k, self.values[k])

        for k in self.multivalues:
            mvid, mvrel = k
            mvprops = self.multivalues[k]
            if not mvprops:
                continue
            mvbnode = person_bnode + 'm' + str(mvid)
            output_triple(out, person_bnode, mvrel, mvbnode)
            for m in mvprops:
                output_triple(out, mvbnode, m, mvprops[m])


class ABPersonToRDF(object):
    def __init__(self, db_name, output_file_name=None):
        self.db_connection = sqlite3.connect(db_name)
        self.ab_multi_value_entry_map = self._build_multi_value_entry_map(
                                                            self.db_connection)
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
            person.generate_formatted_name()

            person.output_ntriples(self.out, self.rand_tag)

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

        if relation == 'abp:Kind':
            person.values['rdf:type'] = get_object_kind_qname(col_val)
            return

        if relation == 'abp:StoreID' and col_val == 0:
            return

        if relation == 'abp:PersonLink':
            # col_val points to an entry in the ABPersonLink table
            if col_val == -1:
                return

        # convert Apple date fields to ISO 8601 date representation
        if relation == 'vcard:rev':
            col_val = apple_date_to_iso_8601(col_val, True)
        elif relation == 'vcard:bday':
            col_val = apple_date_to_iso_8601(col_val, False)

        if relation in ['vcard:hasPhoto']:
            person.values[relation] = format_uri(col_val)
            return

        # set the property
        person.values[relation] = format_literal(col_val)


    def _line_out(self, s):
        # hack for Python 2 / 3 compatibility
        try:
            self.out.write(('%s\n' % (s)).encode('UTF-8'))
        except TypeError:
            self.out.write('%s\n' % (s))

    def _build_multi_value_entry_map(self, db_connection):
        query = 'select ROWID, value from ABMultiValueEntryKey'
        cur = db_connection.cursor()
        remap = {
            'Street': 'vcard:street-address',
            'Country': 'vcard:country-name',
            'ZIP': 'vcard:postal-code',
            'City': 'vcard:locality',
            'State': 'vcard:region',
            'CountryCode': 'gn:countryCode',
            'username': 'abp:username',
            'service': 'abp:service',
            'url': 'vcard:url'
        }

        mve_map = {}
        for row in cur.execute(query):
            try:
                mve_map[row[0]] = remap[row[1]]
            except KeyError:
                mve_map[row[0]] = 'abp:' + row[1]
        return mve_map

    def _get_multi_value_entry_qname(self, nkey):
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
        vcard_categ = None
        curPropDict = None

        cur = self.db_connection.cursor()

        for row in cur.execute(query, (person.id,)):
            uid = row[0]
            isNewValue = uid != last_mv_uid

            prop_type = row[1]

            if isNewValue:
                current_prop_relation, current_prop_func = _get_mv_property_type_qname(prop_type)
                if not current_prop_relation:
                    curPropDict = None
                    continue

                curPropDict = {}
                person.multivalues[(uid, current_prop_relation)] = curPropDict

                prop_category_label = row[4]
                vcard_categ = translate_category_label(prop_category_label)
                if vcard_categ:
                    curPropDict['vcard:category'] = vcard_categ

            elif not current_prop_relation:
                continue

            mval = row[3]
            mv_subval = row[5]
            assert(mval or mv_subval)

            if mv_subval:
                assert(row[6])
                mve_relation = self._get_multi_value_entry_qname(row[6])
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
