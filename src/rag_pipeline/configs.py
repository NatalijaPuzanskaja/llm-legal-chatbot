import os

from dotenv import find_dotenv, load_dotenv
from typing import NamedTuple
from datetime import datetime

load_dotenv(find_dotenv('.dev'))

DB_CONFIGS = {
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASS'),
    'host': os.getenv('DB_HOST'),
}

DB_URL = os.getenv('DB_URL')

OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
LANGCHAIN_API_KEY = os.getenv('LANGCHAIN_API_KEY')
HUGGING_FACE_API_KEY = os.getenv('HUGGING_FACE_API_KEY')

DATA_SOURCES = [
    {
        'name': 'gdpr',
        'table_name': 'llm_legal_chatbot.gdpr_documents',
        'collection_name': 'llm_legal_chatbot.gdpr_embeddings',
        'content': 'src/rag_data/content/gdpr.pdf',
        'schema': 'src/rag_data/schema/gdpr.csv',
        'key_word': 'Article',
        'start_page': 31,
        'end_page': 88,
        'updated_at': '2016-05-04'
    },
    {
        'name': 'ai_act',
        'table_name': 'llm_legal_chatbot.ai_act_documents',
        'collection_name': 'llm_legal_chatbot.ai_act_embeddings',
        'content': 'src/rag_data/content/ai_act.pdf',
        'schema': 'src/rag_data/schema/ai_act.csv',
        'key_word': 'Article',
        'start_page': 43,
        'end_page': 123,
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
    end_page: int
    updated_at: datetime


# https://openai.com/api/pricing/
# price per 1M tokens
TEXT_EMBEDDING_PRICE_CONFIGS = [
    {
        'model': 'text-embedding-3-small',
        'pricing': 0.020,
        'batch_pricing': 0.010,
    },
    {
        'model': 'text-embedding-3-large',
        'pricing': 0.130,
        'batch_pricing': 0.065,
    },
    {
        'model': 'text-embedding-ada-002',
        'pricing': 0.100,
        'batch_pricing': 0.050
    }
]

EMBEDDINGS_CHUNKS_SIZE = 512
EMBEDDINGS_MODEL = 'text-embedding-ada-002'

