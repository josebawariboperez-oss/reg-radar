from dotenv import load_dotenv
import os, sys

print('Python:', sys.version)
print('CWD:', os.getcwd())

ok = load_dotenv()
print('dotenv loaded:', ok)

for k in ['SUPABASE_URL','SUPABASE_KEY','OPENAI_API_KEY','MAILGUN_DOMAIN','MAILGUN_API_KEY','ALERT_TO_EMAIL']:
    print(f'{k} =', os.getenv(k))
