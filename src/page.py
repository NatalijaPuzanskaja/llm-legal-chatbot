from datetime import datetime
from pydantic import BaseModel
from typing import Optional


class Page(BaseModel):
    id: int
    page: int
    contents: str
    updated_time: Optional[datetime] = None
