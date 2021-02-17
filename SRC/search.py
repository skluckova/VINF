from elasticsearch import helpers, Elasticsearch


def print_names(list):
    sequence_number = 1
    for item in list:
        print(sequence_number, " - ", item["_source"]["Name"])
        sequence_number += 1


def check_dates(person_one, person_two):
    birth_date_one = person_one["Birth date"]
    death_date_one = person_one["Death date"]
    birth_date_two = person_two["Birth date"]
    death_date_two = person_two["Death date"]

    if death_date_one is "None":
        death_date_one = "2022-12-12"
        print("Death date of ", person_one["Name"] ," might not be correct.")

    if death_date_one is "None":
        death_date_one = "2022-12-12"
        print("Death date of ", person_two["Name"], " might not be correct.")

    if ((birth_date_one <= death_date_two) and (birth_date_two <= death_date_one)):
        print(person_one["Name"], " and ", person_two["Name"], " could meet. ")

    else:
        print(person_one["Name"], " and ", person_two["Name"], " could not meet. ")


def search(text, es):
    query_body = {
      "query": {
          "match": {
              "Name": text
          }
      }
    }
    results = es.search(index="wiki_index", body=query_body)
    return results["hits"]["hits"]


if __name__ == '__main__':
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

    while True:
        name = input("Enter name of first person: ")
        options_person_one = (search(name, es))
        print_names(options_person_one)
        index = input("Enter number of chosen person: ")
        person_one = (options_person_one[int(index)-1]["_source"])

        name = input("Enter name of second person: ")
        options_person_two = (search(name, es))
        print_names(options_person_two)
        index = input("Enter number of chosen person: ")
        person_two = (options_person_two[int(index)-1]["_source"])

        check_dates(person_one, person_two)
        if not input("Press c to continue ") == "c":
            break

