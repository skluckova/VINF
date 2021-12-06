import lucene
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.util import Version
from org.apache.lucene.store import ByteBuffersDirectory
from org.apache.lucene.index import IndexWriterConfig
from org.apache.lucene.document import StringField
from org.apache.lucene.document import TextField
from org.apache.lucene.index import IndexWriter
from org.apache.lucene.document import Document
from org.apache.lucene.document import Field
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.search import Query
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.store import RAMDirectory
from org.apache.lucene.store import FSDirectory
from java.nio.file import Paths
import csv


def openCSV(file_name,w):
    with open(file_name, 'r', encoding="utf-8") as outfile:
        reader = csv.DictReader(outfile)
        for dct in reader:

            #print(dct)
            addDoc(w, dct["Name"], dct["Birth date"], dct["Death date"], dct["Birth note"], dct["Death note"])

            
def addDoc(w, name,birth_date, death_date,birth_note, death_note):
    doc = Document()
    doc.add(TextField("name", name, Field.Store.YES))
    doc.add(StringField("birth_date", birth_date, Field.Store.YES))
    doc.add(StringField("death_date", death_date, Field.Store.YES))
    doc.add(StringField("birth_note", birth_note, Field.Store.YES))
    doc.add(StringField("death_note", death_note, Field.Store.YES))
    w.addDocument(doc)

    
def createIndex():
    print(lucene.VERSION)
    lucene.initVM(vmargs=['-Djava.awt.headless=true'])

    index = FSDirectory.open(Paths.get('index'))
    analyzer = StandardAnalyzer()
    config = IndexWriterConfig(analyzer)

    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    writer = IndexWriter(index, config)

    openCSV('../output/outputSpark_full_index.csv', writer)
    writer.close()


def search(querystr):
    print('lucene', lucene.VERSION)
    # lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    directory = FSDirectory.open(Paths.get("index"))
    searcher = IndexSearcher(DirectoryReader.open(directory))
    analyzer = StandardAnalyzer()

    q = QueryParser("name", analyzer).parse(querystr)

    hitsPerPage = 20
    docs = searcher.search(q, hitsPerPage)
    hits = docs.scoreDocs

    people = []
    number = 1
    for hit in hits:
        # print(hit.doc, hit.score)
        d = searcher.doc(hit.doc)
        person = {}
        print(number, d.get("name"))
        person['Name'] = (d.get("name"))
        person['Birth date'] = (d.get("birth_date"))
        person['Death date'] = (d.get("death_date"))
        person['Birth note'] = (d.get("birth_note"))
        person['Death note'] = (d.get("death_note"))
        people.append(person)
        number += 1

    return people


def check_dates(person_one, person_two):
    birth_date_one = person_one["Birth date"]
    death_date_one = person_one["Death date"]
    birth_date_two = person_two["Birth date"]
    death_date_two = person_two["Death date"]

    if death_date_one == "None":
        death_date_one = "2022-12-12"
        print("Death date of ", person_one["Name"], " might not be correct.")

    if death_date_two == "None":
        death_date_two = "2022-12-12"
        print("Death date of ", person_two["Name"], " might not be correct.")

    if ((birth_date_one <= death_date_two) and (birth_date_two <= death_date_one)):
        print(person_one["Name"], "lived:", birth_date_one, "-", death_date_one, " and ", person_two["Name"], "lived:", birth_date_two, "-", death_date_two, " could meet. ")

    else:
        print(person_one["Name"], "born:", birth_date_one, "-", death_date_one, " and ", person_two["Name"], "born:", birth_date_two, "-", death_date_two, "could not meet.")


if __name__ == '__main__':
    # createIndex()
    # print("done")

    lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    while True:
        name = input("Enter name of first person: ")
        options_person_one = (search(name))
        if(options_person_one == []):
            print("No match for ", name)
            break

        index = input("Enter number of chosen person: ")
        person_one = (options_person_one[int(index)-1])

        name = input("Enter name of second person: ")
        options_person_two = (search(name))
        if(options_person_two == []):
            print("No match for", name)
            break

        index = input("Enter number of chosen person: ")
        person_two = (options_person_two[int(index)-1])

        check_dates(person_one, person_two)
        if not input("Press c to continue ") == "c":
            break

