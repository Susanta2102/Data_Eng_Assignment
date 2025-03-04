import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    return create_engine(
        f"mysql+mysqldb://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
    )

def extract_abr():
    engine = get_db_connection()
    
    # Sample data - replace with actual ABR CSV
    data = {
        'abn': ['51677562504', '24145780372', '35604515332'],
        'company_name': ['Atlassian', 'Canva', 'Riot Games'],
        'entity_type': ['Company', 'Company', 'Company'],
        'state': ['NSW', 'NSW', 'VIC'],
        'postcode': ['2000', '2000', '3000'],
        'registration_date': ['2002-01-01', '2013-01-01', '2020-01-01']
    }
    
    pd.DataFrame(data).to_sql(
        'abr_raw', 
        engine, 
        if_exists='append', 
        index=False
    )

if __name__ == "__main__":
    extract_abr()