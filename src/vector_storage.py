import psycopg2

import numpy as np

from typing import Iterable, List, Optional
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import execute_values
from pgvector.psycopg2 import register_vector

from src.document import EmbeddedDocument
from src.rag_pipeline.configs import DB_CONFIGS


_pool = None


def db():
    global _pool

    _pool = None
    _pool = create_pool()

    return _pool


def create_pool():
    return psycopg2.pool.SimpleConnectionPool(
        minconn=1,
        maxconn=2,
        **DB_CONFIGS
    )


def get_pool():
    global _pool

    if _pool is None:
        db()

    return _pool


def get_conn(pool):
    return pool.getconn()


def put_conn(pool, conn):
    pool.putconn(conn)


# TODO: extend SqlEngine class
def get_similar_gdpr_documents(embedding_array: np.array) -> List[EmbeddedDocument]:
    pool = get_pool()

    with get_conn(pool) as conn:
        register_vector(conn)
        cur = conn.cursor()
        cur.execute(
            "SELECT contents FROM llm_legal_chatbot.gdpr_embeddings ORDER BY embedding <=> %s LIMIT 3",
            (embedding_array,)
        )
        similar_documents = cur.fetchall()
        cur.close()
        conn.commit()

    put_conn(pool, conn)

    return similar_documents


# TODO: extend SqlEngine class
def get_similar_ai_act_documents(embedding_array: np.array) -> List[EmbeddedDocument]:
    pool = get_pool()

    with get_conn(pool) as conn:
        register_vector(conn)
        cur = conn.cursor()
        cur.execute(
            "SELECT contents FROM llm_legal_chatbot.ai_act_embeddings ORDER BY embedding <=> %s LIMIT 3",
            (embedding_array,)
        )
        similar_documents = cur.fetchall()
        cur.close()
        conn.commit()

    put_conn(pool, conn)

    return similar_documents


# TODO: extend SqlEngine class
def insert_embeddings(table_name: str, documents: List[EmbeddedDocument]) -> None:
    pool = get_pool()

    delete_query = f"""DELETE FROM {table_name}"""
    insert_query = f"""INSERT INTO {table_name} (article, url, contents, tokens, embedding) VALUES %s"""

    documents_tuples = [
        (document.article, document.url, document.contents, document.tokens, np.array(document.embedding))
        for document in documents
    ]

    with get_conn(pool) as conn:
        register_vector(conn)
        cur = conn.cursor()

        cur.execute(delete_query)
        execute_values(cur, insert_query, documents_tuples)

        cur.close()
        conn.commit()

    put_conn(pool, conn)
