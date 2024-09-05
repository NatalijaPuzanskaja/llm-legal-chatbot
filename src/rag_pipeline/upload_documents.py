import csv
import copy
import traceback

from langchain_community.document_loaders import PyMuPDFLoader

from src.vector_storage import RemoteDocumentsStorage

from src.rag_pipeline.configs import (
    DB_CONFIGS,
    DATA_SOURCES,
    DataSourceConfig
)
from src.utils.sql import PostgresDataSource, SqlEngine
from src.document import Document


def load_documents():
    try:
        data_source = PostgresDataSource.from_credentials(
            database=DB_CONFIGS['dwh']['dbname'],
            user=DB_CONFIGS['dwh']['user'],
            password=DB_CONFIGS['dwh']['password'],
            host=DB_CONFIGS['dwh']['host'],
        )
        sql = SqlEngine(data_source)
        storage = RemoteDocumentsStorage(sql)

        for data_source in DATA_SOURCES:
            data_source_config = DataSourceConfig(**data_source)

            pdf_loader = PyMuPDFLoader(data_source_config.content)
            pages = pdf_loader.load()[data_source_config.start_page:]
            pages_content = [page.page_content for page in pages]
            restored_document = '\n '.join(pages_content)

            with open(data_source_config.schema, mode='r') as file:
                table_of_contents = list(csv.DictReader(file))

            table_of_contents = sorted(table_of_contents, key=lambda item: item.get('article'))

            current_item_id = 0
            residual_document = copy.deepcopy(restored_document)
            extracted_articles = {}
            while current_item_id + 1 < len(table_of_contents):
                current_article = table_of_contents[current_item_id].get('article')
                next_article = table_of_contents[current_item_id + 1].get('article')

                index_start = (
                    residual_document.find(current_article) + len(current_article)
                )
                index_end = (
                    residual_document.find(next_article) - 1
                )

                extracted_articles[current_article] = residual_document[index_start:index_end]
                residual_document = residual_document[index_end:]

                current_item_id += 1

            last_article = table_of_contents[current_item_id].get('article')
            extracted_articles[last_article] = residual_document

            documents = [
                Document(
                    chapter=item.get('chapter'),
                    chapter_name=item.get('chapter_name'),
                    section=item.get('section'),
                    section_name=item.get('section_name'),
                    article=item.get('article'),
                    article_name=item.get('article_name'),
                    url=item.get('url'),
                    contents=extracted_articles.get(item.get('article')),
                    updated_time=data_source_config.updated_at,
                ) for item in table_of_contents
            ]

            storage.upsert_documents(
                table_name=data_source_config.table_name,
                documents=documents
            )
    
    except Exception as err:
        trace = traceback.format_exc()
        print(f'Error: {err}')
        print(trace)
        raise
