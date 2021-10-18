from lupyne import engine                       # don't forget to call lucene.initVM
import lucene

indexer = engine.Indexer()                      # create an in-memory index (no filename supplied)
indexer.set('name', stored=True)                # create stored 'name' field
indexer.set('text', engine.Field.Text)          # create indexed 'text' field
indexer.add(name='sample', text='hello world')  # add a document to the index
indexer.commit()                                # commit changes; document is now searchable
hits = indexer.search('text:hello')             # run search and return sequence of documents
len(hits), hits.count                           # 1 hit retrieved (out of a total of 1)
(hit,) = hits
hit['name']                                     # hits support mapping interface for their stored fields
hit.id, hit.score                               # plus internal doc number and score
hit.dict()                                      # dict representation of the hit document