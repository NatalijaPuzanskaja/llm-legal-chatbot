import csv
import copy
import traceback

from typing import List

from langchain_community.document_loaders import PyMuPDFLoader

from src.vector_storage import RemoteDocumentsStorage

from src.rag_pipeline.configs import (
    DB_CONFIGS,
    DATA_SOURCES,
    DataSourceConfig
)
from src.utils.sql import PostgresDataSource, SqlEngine
from src.document import RawDocument


def get_best_search_result(search_results: List[tuple]) -> tuple:
    relevant_results = [
        result for result in search_results
        if result[0] > 0 and result[1] > 0
    ]

    if relevant_results:
        return relevant_results[0]

    return search_results[0]


def get_index_start(cut_word: str, search_string: str) -> int:
    return search_string.find(cut_word) + len(cut_word)


def get_index_end(cut_word: str, search_string: str) -> int:
    return search_string.find(cut_word)


def build_cut_words(fixed_part: str, variable_part: int) -> List[str]:
    return [
        f'''\n{fixed_part} {variable_part}\n''',
        f'''\n {fixed_part} {variable_part}\n''',
        f'''\n{fixed_part} {variable_part} ''',
        f'''{fixed_part} {variable_part} '''
    ]


def load_documents(config: DataSourceConfig) -> List[RawDocument]:
    # load pdf
    pdf_loader = PyMuPDFLoader(config.content)
    pages = pdf_loader.load()[config.start_page:config.end_page]
    pages_content = [page.page_content for page in pages]
    restored_document = '\n '.join(pages_content)

    # load pdf document structure (chapter, section, article)
    with open(config.schema, mode='r') as file:
        table_of_contents = list(csv.DictReader(file))

    table_of_contents = sorted(table_of_contents, key=lambda item: int(item.get('article')))

    # iteratively extract smallest parts of pdf document (articles)
    current_item_id = 1
    residual_document = copy.deepcopy(restored_document)
    extracted_articles = {}
    while current_item_id < len(table_of_contents):
        current_article_cut_words = build_cut_words(
            fixed_part=config.key_word,
            variable_part=current_item_id
        )
        next_article_cut_words = build_cut_words(
            fixed_part=config.key_word,
            variable_part=current_item_id + 1
        )

        cut_words_combinations = [
            (current_, next_) for current_ in current_article_cut_words for next_ in next_article_cut_words
        ]

        cut_words_search_results = [
            (
                get_index_start(current_, residual_document),
                get_index_end(next_, residual_document)
            ) for current_, next_ in cut_words_combinations
        ]

        index_start, index_end = get_best_search_result(cut_words_search_results)

        extracted_articles[current_item_id] = (
            residual_document[index_start:index_end]
        )
        residual_document = residual_document[index_end:]

        current_item_id += 1

    extracted_articles[current_item_id] = residual_document

    return [
        RawDocument(
            chapter=int(item.get('chapter')),
            chapter_name=item.get('chapter_name'),
            section=int(item.get('section')) if item.get('section') else None,
            section_name=item.get('section_name') if item.get('section_name') else None,
            article=int(item.get('article')),
            article_name=item.get('article_name'),
            url=item.get('url'),
            contents=extracted_articles.get(int(item.get('article'))),
            updated_time=config.updated_at,
        ) for item in table_of_contents
    ]


def read_and_write_pdfs() -> None:
    try:
        data_source = PostgresDataSource.from_credentials(**DB_CONFIGS)
        sql = SqlEngine(data_source)
        storage = RemoteDocumentsStorage(sql)

        for data_source in DATA_SOURCES:
            data_source_config = DataSourceConfig(**data_source)
            documents = load_documents(data_source_config)

            storage.upsert_documents(
                table_name=data_source_config.table_name,
                documents=documents
            )

    except Exception as err:
        trace = traceback.format_exc()
        print(f'Error: {err}')
        print(trace)
        raise
