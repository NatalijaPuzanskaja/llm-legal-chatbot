from datetime import datetime
from pydantic import BaseModel
from typing import Optional, List


class RawDocument(BaseModel):
    chapter: int
    chapter_name: str
    section: Optional[int] = None
    section_name: Optional[str] = None
    article: int
    article_name: str
    url: str
    contents: str
    updated_time: Optional[datetime] = None


class EmbeddedDocument(BaseModel):
    article: int
    url: str
    contents: str
    tokens: int
    embedding: Optional[List[float]] = None
