from typing import NamedTuple
from datetime import datetime


DB_CONFIGS = {
    'dwh': {
        'dbname': 'embeddings',
        'user': 'dwh',
        'password': 'asodfin2948gb9h284hf2uhg',
        'host': 'playground.cg1vrrgv96z0.eu-west-1.rds.amazonaws.com'
    }
}

DATA_SOURCES = [
    {
        'name': 'gdpr',
        'table_name': 'llm_legal_chatbot.gdpr_documents',
        'collection_name': 'llm_legal_chatbot.gdpr_embeddings',
        'content': 'src/rag_data/content/gdpr.pdf',
        'schema': 'src/rag_data/schema/gdpr.csv',
        'key_word': 'Article',
        'start_page': 32,
        'updated_at': '2016-05-04'
    },
    {
        'name': 'ai_act',
        'table_name': 'llm_legal_chatbot.ai_act_documents',
        'collection_name': 'llm_legal_chatbot.ai_act_embeddings',
        'content': 'src/rag_data/content/ai_act.pdf',
        'schema': 'src/rag_data/schema/ai_act.csv',
        'key_word': 'Art.',
        'start_page': 43,
        'updated_at': '2024-07-12'
    },
]


class DataSourceConfig(NamedTuple):
    name: str
    table_name: str
    collection_name: str
    content: str
    schema: str
    key_word: str
    start_page: int
    updated_at: datetime
