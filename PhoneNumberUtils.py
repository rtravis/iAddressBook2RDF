#!/usr/bin/python

# PhoneNumberUtils - utility functions to normalize phone numbers
#
# @copyright: Copyright (c) 2017 Robert Zavalczki, distributed
# under the terms and conditions of the Lesser GNU General
# Public License version 2.1

try:
    unicode_constructor = unicode
except NameError:
    unicode_constructor = str


""" return the local trunk prefix for a given phone country code """
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

""" Normalize a phone number be removing formatting characters

Returns None if the phone number given as input is invalid and
validateDigitsOnly is true.

Phone numbers identified as international will be prefixed by a '+'. If the
country code parameter is specified it will be prepended to phone numbers
identified as local, after removing the local trunk prefix if it exists.
"""
def normalize_phone_number(phoneNo, countryCallingCode=None,
                           validateDigitsOnly=False):
    # remove spaces and formatting characters
    phoneNo = unicode_constructor(phoneNo).lower().translate(
        { ord(c) : None for c in u"<>()-\u2011 \t\n\u00a0\u202a\u202c" })
    # check if a phone URI
    if phoneNo.startswith("tel:"):
        phoneNo = phoneNo[4:]

    # check if this is a special or extension number
    isSpecial = len(phoneNo) < 5 or "*" in phoneNo or "#" in phoneNo

    if validateDigitsOnly:
        if not phoneNo.translate({ ord(c) : None for c in u"+*#" }).isdigit():
            return None

    isInternational = False

    calling_prefixes = ['0011', '000', '001', '010', '011', '00', '+']
    for i in calling_prefixes:
        if phoneNo.startswith(i):
            phoneNo = phoneNo[len(i):]
            isInternational = True
            break

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

if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2:
        print('Usage: PhoneNumberUtils.py <pnone-number> [<country-calling-code>]')
        sys.exit(1)
    phoneNo = sys.argv[1]
    countryCode = sys.argv[2] if len(sys.argv) > 2 else None
    print(normalize_phone_number(phoneNo, countryCode))

