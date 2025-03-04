import pandas as pd

# Download from https://data.gov.au/dataset/ds-dga-0e939cd3-169e-4c0e-9bde-4314a4112e69/
ABR_URL = "https://www.abr.gov.au/ABRRegisterSearch/download/ABR_Extract_Full.zip"

def process_abr_data():
    # Load ABR data
    abr = pd.read_csv(ABR_URL, sep='\t', encoding='iso-8859-1')
    
    # Clean and filter columns
    abr_clean = abr[[
        'ABN', 'Organisation Name', 'Entity Type', 
        'State', 'Postcode', 'Registration Date'
    ]].rename(columns={
        'Organisation Name': 'company_name',
        'Entity Type': 'entity_type',
        'Registration Date': 'registration_date'
    })
    
    # Remove invalid ABNs
    abr_clean = abr_clean[abr_clean['ABN'].str.match(r'^\d{11}$')]
    
    return abr_clean

# Merge datasets
def merge_datasets(cc_data, abr_data):
    # Clean names for matching
    cc_data['clean_name'] = cc_data['company_name'].str.lower().str.replace(r'\b(ptyltd|ltd|inc)\b', '', regex=True)
    abr_data['clean_name'] = abr_data['company_name'].str.lower().str.replace(r'\b(ptyltd|ltd|inc)\b', '', regex=True)
    
    # Merge on cleaned names
    merged = pd.merge(
        cc_data,
        abr_data,
        on='clean_name',
        how='left',
        suffixes=('_cc', '_abr')
    )
    
    return merged.drop(columns='clean_name')

# Main execution
if __name__ == "__main__":
    cc_data = pd.read_parquet("common_crawl_data.parquet")
    abr_data = process_abr_data()
    final_data = merge_datasets(cc_data, abr_data)
    final_data.to_sql('companies', engine, if_exists='replace')  # Requires SQLAlchemy connection