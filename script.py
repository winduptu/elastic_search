from elasticsearch import Elasticsearch, helpers
import pandas as pd

es = Elasticsearch(hosts=[{"host": "localhost", "port": 9200, "scheme": "http"}])

# Create an index with a custom Thai analyzer
index_settings = {
    "settings": {
        "analysis": {
            "analyzer": {
                "thai_analyzer": {
                    "tokenizer": "thai_tokenizer"
                }
            },
            "tokenizer": {
                "thai_tokenizer": {
                    "type": "thai"
                }
            }
        }
    }
}

es.indices.delete(index='test', ignore=[400, 404])

es.indices.create(index="test", body=index_settings)
# Update the mapping for a field to use the custom Thai analyzer
field_mapping = {
    "properties": {
        "text_field": {
            "type": "text",
            "analyzer": "thai_analyzer"
        },
        "agent": {
            "type": "text",
            "analyzer": "thai_analyzer"
        },
        "tel": {
            "type": "text",
            "analyzer": "thai_analyzer"
        },
        "time_durationclip": {
            "type": "float"
            
        },
        "datetime": {
            "type": "text",
            "analyzer": "thai_analyzer"
        }        
    }
}

es.indices.put_mapping(index="botnoi4", body=field_mapping)

dat2 = pd.read_csv('botnoivoice_audiofile_flat.csv', low_memory=False)
dat2 = dat2.fillna(0)
dat2['datetime'] = pd.to_datetime(dat2['datetime'])



doc_list = []
for index, row in dat2.iterrows():
    try:
        doc = {'time_durationclip': float(row['duration']),
               'text_field': str(row['text']),
               'agent': str(row['admin']),
               'tel': str(row['cid']),
               'audio_url': str(row['url']),
               'datetime': row['datetime']}
        doc_list.append(doc)
    except:
        print(index)
        pass

# index all documents at once
helpers.bulk(es, doc_list, index='test')