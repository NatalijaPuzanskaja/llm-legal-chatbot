from datetime import datetime
from pydantic import BaseModel
from typing import Optional


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
