from typing import Iterable, List, Optional

from typing_extensions import Protocol

from src.document import Document

from src.utils.sql import SqlEngine, QueryBuilder


class DocumentsStorage(Protocol):
    def list_similar_documents(self, table_name: str, squery: str) -> List[Document]:
        ...

    def upsert_documents(self, table_name: str, documents: Iterable[Document]) -> None:
        ...


class RemoteDocumentsStorage(DocumentsStorage):
    sql: SqlEngine

    def __init__(self, sql: SqlEngine) -> None:
        self.sql = sql

    def list_similar_documents(self, table_name: str, query: str) -> List[Document]:

        return []

    def upsert_documents(self, table_name: str, documents: Iterable[Document]) -> None:
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
