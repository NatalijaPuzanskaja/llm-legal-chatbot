from langchain.docstore.document import Document
from langchain.text_splitter import CharacterTextSplitter
from timescale_vector import client


def get_document(blog):
    text_splitter = CharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=200,
    )
    docs = []
    for chunk in text_splitter.split_text(blog['contents']):
        content = f"Author {blog['author']}, title: {blog['title']}, contents:{chunk}"
        metadata = {
            "id": str(client.uuid_from_time(blog['published_time'])),
            "blog_id": blog['id'],
            "author": blog['author'],
            "category": blog['category'],
            "published_time": blog['published_time'].isoformat(),
        }
        docs.append(Document(page_content=content, metadata=metadata))
    return docs
