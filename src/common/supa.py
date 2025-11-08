# -*- coding: utf-8 -*-
from dotenv import load_dotenv
from supabase import create_client
import os

def get_client():
    load_dotenv()
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_KEY')
    if not url or not key:
        raise RuntimeError('Faltan SUPABASE_URL o SUPABASE_KEY en .env')
    return create_client(url, key)
