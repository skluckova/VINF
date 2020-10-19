import json
import re
import xml.etree.ElementTree as ET
from dateutil.parser import *
from datetime import *

if __name__ == '__main__':
    dictionary_list = []
    for event, elem in ET.iterparse("data_short.xml", events=("start", "end")):
        if "text" in elem.tag and event == "end":
            birth_re = re.search('\| birth_date.*\{\{(.*)\}\}', elem.text).group(1)
            birth_date = re.search('\|[0-9]*\|[0-9]*\|[0-9]*', birth_re).group(0).replace('|', ' ')
            birth_date_parsed = str(parse(birth_date).date())

            death_re = re.search('\| death_date.*\{\{(.*)\}\}', elem.text)
            if death_re is not None:
                death_re = death_re.group(1)
                death_date = re.search('\|[0-9]*\|[0-9]*\|[0-9]*', death_re)
                if death_date is not None:
                    death_date = death_date.group(0).replace('|', ' ')
                    death_date_parsed = str(parse(death_date).date())
                else:
                    death_date_parsed = None
            else:
                death_date_parsed = None

            name_re = re.search('\| name.*=.*', elem.text).group(0)
            name_split = name_re.split('=')
            name = name_split[1].strip()

            dictionary_list.append({'name': name, 'birth_date': birth_date_parsed, 'death_date': death_date_parsed})

            print(name)
            print(birth_date_parsed)
            print(death_date_parsed)

            elem.clear()
    with open('output.json',"w") as outfile:
        json.dump(dictionary_list, outfile)