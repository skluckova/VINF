import csv
from elasticsearch import helpers, Elasticsearch


def csv_reader(file_name, es):
    with open(file_name, 'r', encoding="utf-8") as outfile:
        reader = csv.DictReader(outfile)
        helpers.bulk(es, reader, index="wiki_index")


if __name__ == '__main__':

    mapping = {
        "mappings": {
            "properties": {
                "Name": {
                    "type": "text"
                },
                "Birth date": {
                    "type": "text"
                },
                "Death date": {
                    "type": "text"
                },
                "Birth note": {
                    "type": "text"
                },
                "Death note": {
                    "type": "text"
                }
            }
        }
    }

    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    es.indices.delete(index='wiki_index')
    response = es.indices.create(
        index="wiki_index",
        body=mapping,
        ignore=400
    )

    print('response:', response)

    csv_reader('output/outputSpark1.csv', es)
