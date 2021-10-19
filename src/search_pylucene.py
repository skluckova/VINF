from lupyne import engine
import lucene

indexer = engine.Indexer()
indexer.set('name', stored=True)
indexer.set('text', engine.Field.Text)
indexer.add(name='sample', text='hello world')
indexer.commit()
hits = indexer.search('text:hello')
len(hits), hits.count
(hit,) = hits
hit['name']
hit.id, hit.score
hit.dict()