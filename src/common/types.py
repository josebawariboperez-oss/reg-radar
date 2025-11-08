# -*- coding: utf-8 -*-
from pydantic import BaseModel, Field, HttpUrl
from typing import List, Optional
from datetime import date

class Regulation(BaseModel):
    country: str
    authority: str
    title: str
    url: HttpUrl
    doc_type: Optional[str] = None
    status: Optional[str] = None
    topics: List[str] = Field(default_factory=list)
    publish_date: Optional[date] = None
    effective_date: Optional[date] = None
    summary: Optional[str] = None
    raw_text: Optional[str] = None
    impact_level: Optional[int] = None
    source: Optional[str] = None
