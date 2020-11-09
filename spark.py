import csv
import re
from dateutil.parser import *
from datetime import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas


def to_csv(dictionary_list):
    # prevod do csv
    keys = dictionary_list[0].keys()
    with open('output.csv', 'w', newline='', encoding="utf-8") as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(dictionary_list)


def get_name(text):
    name_re = re.search('\| name.*=.*', str(text))

    if name_re is not None:
        name_re = name_re.group(0)
        name_split = name_re.split('=')
        name = name_split[1].strip()
    else:
        name = ''
    # odstranenie tagov
    cleanr = re.compile('<.*?>')
    name = re.sub(cleanr, '', name)
    return re.sub(r'\W+', ' ', name).strip()


def get_year_from_century(birth_date_full):
    return re.sub('\D', '', birth_date_full) + "00"


def check_primary_regex_birth(text, date_type):
    # regex s | a {{
    remove_terms = ['C.E.','c.', 'ca.', 'Circa', 'circa', 'CE', 'BC', 'After', 'C.', 'C ', 'baptized', '<','>', 'about', '[[circa|c.]]', '[[Vedic period]]', '?', '[[Antwerp]]', '(', ')', 'AH',
                    'before','[[Av]]', 'BCE', 'late', 'prior to', 'Baptised', 'Early', 'died', 'after', 'early']
    try:
        birth_re = re.search(date_type + '.*\{\{(.*)\}\}', text)
        if birth_re is not None:
            birth_re = birth_re.group(1)
            birth_date = re.search('\|[0-9]*\|[0-9]*\|[0-9]*', birth_re)

            if birth_date is not None:
                birth_date = birth_date.group(0).replace('|', ' ')
                birth_date_parsed = str(parse(birth_date).date())
            else:
                birth_date_parsed = 'None'
        else:
            second_birth_re = re.search(date_type + '.*', text)
            if second_birth_re is not None:
                birth_date_full = second_birth_re.group(0).split('=', 1)[1]
                birth_date_full = re.split("<ref|\(age|\&lt|<\!\-|<br|, age|\(Age |aged", birth_date_full)[0]
                birth_date_full = re.sub(r'|'.join(map(re.escape, remove_terms)), '', birth_date_full)

                birth_date_full = birth_date_full.rsplit(' or ')[-1]
                birth_date_full = birth_date_full.rsplit('/')[-1]
                birth_date_full = birth_date_full.rsplit('and')[-1]
                birth_date_full = birth_date_full.rsplit('-')[-1]

                birth_date_full = birth_date_full.strip()

                if 'century'.lower() in birth_date_full.lower():
                    birth_date_full = get_year_from_century(birth_date_full)

                if not birth_date_full:
                    return 'None'
                birth_date_parsed = str(parse(birth_date_full).date())
            else:
                birth_date_parsed = 'None'

    except Exception as exception:
        # print(exception)
        date_re = re.search(date_type + '.*', text)
        if date_re is not None:
            with open(date_type + ".txt", "a") as myfile:
                myfile.write(get_name(text) + "\n")
                myfile.write(str(exception) + '\n')
                myfile.write(date_re.group(0) + "\n\n\n")
        return 'None'

    return birth_date_parsed

def map_row(row):
    text = row.revision.text
    name = get_name(text)
    if not name:
        name = row.title

    try:
        birth_date_parsed = check_primary_regex_birth(text, "birth_date")
        death_date_parsed = check_primary_regex_birth(text, "death_date")

    except Exception as exception:
        birth_date_parsed = 'None'
        death_date_parsed = 'None'
        # print(exception)

    return (name, birth_date_parsed, death_date_parsed)

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    customSchema = StructType([ \
        StructField("title", StringType(), True),
        StructField('revision', StructType([
            StructField('text', StringType(), True),
        ]))
    ])

    df = spark.read \
        .format('com.databricks.spark.xml') \
        .options(rowTag="page") \
        .load('data.xml', schema=customSchema)

    rdd = df.rdd.map(map_row)
    df2 = rdd.toDF()
    df_filtered = df2.filter(df2._2 != 'None')
    # df_filtered.show()
    df_filtered.toPandas().to_csv("outputSpark.csv")