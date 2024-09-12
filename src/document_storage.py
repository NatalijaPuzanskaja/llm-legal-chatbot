import psycopg2

import numpy as np

from typing import Iterable, List, Optional
from typing_extensions import Protocol
from psycopg2.extras import execute_values
from pgvector.psycopg2 import register_vector

from src.document import RawDocument, EmbeddedDocument

from src.utils.sql import SqlEngine


class DocumentsStorage(Protocol):
    def list_documents(self, table_name: str) -> Iterable[RawDocument]:
        ...

    def upsert_documents(self, table_name: str, documents: Iterable[RawDocument]) -> None:
        ...


class RemoteDocumentsStorage(DocumentsStorage):
    sql: SqlEngine

    def __init__(self, sql: SqlEngine) -> None:
        self.sql = sql

    def list_documents(self, table_name: str) -> Iterable[RawDocument]:
        q = f"""
        SELECT 
            chapter
            , chapter_name
            , section
            , section_name
            , article
            , article_name
            , url
            , contents
            , updated_time
        FROM 
            {table_name}
        """

        with self.sql.begin_transaction() as tx:
            for row in tx.execute_query(q):
                yield RawDocument(
                    chapter=row['chapter'],
                    chapter_name=row['chapter_name'],
                    section=row['section'],
                    section_name=row['section_name'],
                    article=row['article'],
                    article_name=row['article_name'],
                    url=row['url'],
                    contents=row['contents'],
                    updated_time=row['updated_time'],
                )

    def upsert_documents(self, table_name: str, documents: Iterable[RawDocument]) -> None:
        q = f"""
        INSERT INTO 
            {table_name}(
                chapter
                , chapter_name
                , section
                , section_name
                , article
                , article_name
                , url
                , contents
                , updated_time
            )
        VALUES (
            %(chapter)s
            , %(chapter_name)s
            , %(section)s
            , %(section_name)s
            , %(article)s
            , %(article_name)s
            , %(url)s
            , %(contents)s
            , %(updated_time)s
        )
        ON CONFLICT (article) DO UPDATE
        SET
            contents = %(contents)s
            , updated_time = %(updated_time)s
        """

        with self.sql.begin_transaction() as tx:
            for document in documents:
                tx.execute_statement(
                    q,
                    chapter=document.chapter,
                    chapter_name=document.chapter_name,
                    section=document.section,
                    section_name=document.section_name,
                    article=document.article,
                    article_name=document.article_name,
                    url=document.url,
                    contents=document.contents,
                    updated_time=document.updated_time,
                )
