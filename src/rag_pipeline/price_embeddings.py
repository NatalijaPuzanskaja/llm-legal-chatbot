import traceback
import tiktoken

from typing import List

from src.vector_storage import RemoteDocumentsStorage
from src.rag_pipeline.configs import (
    DB_CONFIGS,
    DATA_SOURCES,
    DataSourceConfig,
    TEXT_EMBEDDING_PRICE_CONFIGS
)
from src.utils.sql import PostgresDataSource, SqlEngine
from src.document import RawDocument


def num_tokens_from_string(string: str, encoding_name="cl100k_base") -> int:
    if not string:
        return 0
    # return the number of tokens in a text string
    encoding = tiktoken.get_encoding(encoding_name)
    num_tokens = len(encoding.encode(string))
    return num_tokens


def get_content_length(content):
    word_list = content.split()
    num_words = len(word_list)
    return num_words


def get_embedding_cost(num_tokens: int) -> List[dict]:
    return [
        {
            'model': price_config.get('model'),
            'tokens': num_tokens,
            'pricing': num_tokens / 1000000 * price_config.get('pricing'),
            'batch_pricing': num_tokens / 1000000 * price_config.get('batch_pricing')
        } for price_config in TEXT_EMBEDDING_PRICE_CONFIGS
    ]


def get_total_embeddings_cost(documents: List[RawDocument]) -> List[dict]:
    documents_tokens = [
        num_tokens_from_string(document.contents)
        for document in documents
    ]

    total_tokens = sum(documents_tokens)
    total_cost = get_embedding_cost(total_tokens)

    return total_cost


def fetch_and_price_embeddings() -> List[dict]:
    try:
        data_source = PostgresDataSource.from_credentials(**DB_CONFIGS)
        sql = SqlEngine(data_source)
        storage = RemoteDocumentsStorage(sql)

        result = []
        for data_source in DATA_SOURCES:
            data_source_config = DataSourceConfig(**data_source)

            documents = list(storage.list_documents(data_source_config.table_name))

            result += [
                dict(item, **{'name': data_source_config.name})
                for item in get_total_embeddings_cost(documents)
            ]

        return result

    except Exception as err:
        trace = traceback.format_exc()
        print(f'Error: {err}')
        print(trace)
        raise
