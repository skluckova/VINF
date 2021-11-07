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
import csv


def openCSV(file_name,w):
    with open(file_name, 'r', encoding="utf-8") as outfile:
        reader = csv.DictReader(outfile)
        for dct in reader:
            print(dct)
            addDoc(w, dct["Name"], dct["Birth date"], dct["Death date"], dct["Birth note"], dct["Death note"])

def addDoc(w, name,birth_date, death_date,birth_note, death_note):
    doc = Document()
    doc.add(TextField("name", name, Field.Store.YES))
    doc.add(StringField("birth_date", birth_date, Field.Store.YES))
    doc.add(StringField("death_date", death_date, Field.Store.YES))
    doc.add(StringField("birth_note", birth_note, Field.Store.YES))
    doc.add(StringField("death_note", death_note, Field.Store.YES))
    w.addDocument(doc)


print(lucene.VERSION)

lucene.initVM()
analyzer = StandardAnalyzer()
index = RAMDirectory()

config = IndexWriterConfig(analyzer)

w = IndexWriter(index, config)
openCSV('../output/outputSpark1.csv', w)
w.close()

querystr = "Peter"
q = QueryParser("name", analyzer).parse(querystr)


hitsPerPage = 10
reader = DirectoryReader.open(index)
searcher = IndexSearcher(reader)
docs = searcher.search(q, hitsPerPage)
hits = docs.scoreDocs

for hit in hits:
    print(hit.doc, hit.score)
    d = searcher.doc(hit.doc)
    print(d.get("name"))