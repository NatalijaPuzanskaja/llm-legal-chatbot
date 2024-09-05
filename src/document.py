from datetime import datetime
from pydantic import BaseModel
from typing import Optional


class Document(BaseModel):
    chapter: str
    chapter_name: str
    section: Optional[str] = None
    section_name: Optional[str] = None
    article: str
    article_name: str
    url: str
    contents: str
    updated_time: Optional[datetime] = None
