__author__ = 'Ammar Akhtar'

"""
This file creates set of regex which are used by the DiffEngine class to exclude bad rows
"""

import re

re_date_search = r"\b(2011|2012|2013|2014|2015)(\/\d{2}){2}\b"

re_bad_whitespace = re.compile(r'\s+')

re_bad_narratives = [
    "^(.{1,}PBZ)$|^(\d+)$",
    "^(\s)*\?(\s)*$",
    "(\W\D){2,}",
    "^[0-9]{1,6}$",
    "^[0-9]{1,}\-[A-Za-z]{3}$'",
    "^(\d){2,10}([^\w\d\s][\w]+)+",
    "^[a-zA-z]{3}\ \d{4}\-\d{2}\-\d{2}",
    "^[0-9]{1,6}\ [0-9]{1,2}[A-Za-z]{3}(\ |)[0-9]{4,}$",
    "^[0-9]{1,2}[A-Za-z]{1,3}(\ |-|)[A-Za-z]\/[A-Za-z]\ \d{1,}$",
    "^[0-9]{1,2}[A-Za-z]{3}(\ |-|)[A-Za-z]{3}(|(\ |)[0-9]{4,})$",
    "^\w{2}\d{6}\w{1}",
    "E\+\d+$",
    "^\w{2}\- \d+ \-\d+$",
    "^(\d){2,10} (\d){2}(\w){3}(\d){2}",
    "(\w{3}\d\w{2}){2}\w",
    "\w{2}\-\d{4}\s+\-\d{2,}",
    "\d{8,}\w{1}",
    "(\d{2}\w{2}\d{8,})|([a-zA-Z]{4,}\d?(\w{4,}|\d{5,}))",
    "([a-zA-Z]{3}\.[a-zA-Z]{4}\d+)",
    "\b[a-zA-Z]{1,4}\d{4}\b",
    "WS\-\d{4}",
]

re_dead_narratives = [
        "\w{2}\-\d{4}\s+\-\d{2,}",
        "\d{8,}\w{1}",
        "(\d{2}\w{2}\d{8,})|([a-zA-Z]{4,}\d?(\w{4,}|\d{5,}))",
        "([a-zA-Z]{3}\.[a-zA-Z]{4}\d+)",
        "\b[a-zA-Z]{1,4}\d{4}\b",
        "WS\-\d{4}",
    ]

re_bad_narratives = re.compile("(" + ")|(".join(re_bad_narratives) + ")")
re_dead_narratives  = re.compile("(" + ")|(".join(re_dead_narratives ) + ")")

re_bad_seed = ["(^\d+$)",
               "(\d{1,2}(\/\d{1,2})+)",
               "(\w{1}\[\'\+\/\=]{2,})",
               "(\w{2}\d{3,}\w{1})",
               "(\w{3}\d{4,10})",
               "(\d{3,}\w{0,1})",
               "(\d{2}(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{2,4})?)",
               "(\b(POS)\b)",
               "((\$|\U+20A4|U+20AC)\d+\.\d+)",
               "REF\.NO",
               "NO\:",
               "\d\w{4}\S{4}\d{3}\w{1}",
               "^\w{1}$"
]

re_bad_seed = re.compile("(" + ")|(".join(re_bad_seed) + ")")