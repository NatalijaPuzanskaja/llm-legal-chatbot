import traceback

from typing import List
from openai import OpenAI

from src.document_storage import RemoteDocumentsStorage
from src.vector_storage import insert_embeddings
from src.rag_pipeline.configs import (
    DB_CONFIGS,
    DATA_SOURCES,
    DataSourceConfig,
    EMBEDDINGS_CHUNKS_SIZE,
    EMBEDDINGS_MODEL
)
from src.rag_pipeline.price_embeddings import num_tokens_from_string
from src.utils.sql import PostgresDataSource, SqlEngine
from src.document import RawDocument, EmbeddedDocument


client = OpenAI()


def get_embeddings(documents: List[EmbeddedDocument]) -> List[EmbeddedDocument]:
    embeddings_result = []
    for document in documents:
        response = client.embeddings.create(
            input=[document.contents.replace("\n", " ")],
            model=EMBEDDINGS_MODEL
        )

        embeddings_result.append(EmbeddedDocument(
           article=document.article,
           url=document.url,
           contents=document.contents,
           tokens=document.tokens,
           embedding=response.data[0].embedding
        ))

    return embeddings_result


def chunk_documents(documents: List[RawDocument]) -> List[EmbeddedDocument]:
    chunk_result = []
    for document in documents:
        token_count = num_tokens_from_string(document.contents)

        if token_count <= EMBEDDINGS_CHUNKS_SIZE:
            chunk_result.append(
                EmbeddedDocument(
                    article=document.article,
                    url=document.url,
                    contents=document.contents,
                    tokens=token_count
                )
            )

        else:
            start = 0
            size = int(EMBEDDINGS_CHUNKS_SIZE // (4/3))
            end = size

            words = document.contents.split()
            words = [item for item in words if item != ' ']

            total_words = len(words)

            chunk_count = total_words // size
            if total_words % size != 0:
                chunk_count += 1

            chunked_content = []
            for i in range(chunk_count):
                if end > total_words:
                    end = total_words
                chunked_content = words[start:end]
                chunked_content_string = ' '.join(chunked_content)
                chunked_content_token_count = num_tokens_from_string(chunked_content_string)

                if chunked_content_token_count > 0:
                    chunk_result.append(
                        EmbeddedDocument(
                            article=document.article,
                            url=document.url,
                            contents=chunked_content_string,
                            tokens=chunked_content_token_count
                        )
                    )

                start += size
                end += size

    return chunk_result


def chunk_and_create_embeddings() -> None:
    try:
        data_source = PostgresDataSource.from_credentials(**DB_CONFIGS)
        sql = SqlEngine(data_source)
        storage = RemoteDocumentsStorage(sql)

        for data_source in DATA_SOURCES:
            data_source_config = DataSourceConfig(**data_source)
            documents = list(storage.list_documents(data_source_config.table_name))

            chunked_documents = chunk_documents(documents)
            embedded_documents = get_embeddings(chunked_documents)

            insert_embeddings(
                table_name=data_source_config.collection_name,
                documents=embedded_documents,
            )

    except Exception as err:
        trace = traceback.format_exc()
        print(f'Error: {err}')
        print(trace)
        raise
