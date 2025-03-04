# Data Engineering Assignment



## Project Overview

This repository contains a comprehensive data pipeline for integrating Australian company website data from Common Crawl with business information from the Australian Business Register (ABR). The pipeline efficiently extracts, transforms, and loads data into a MySQL database to enable detailed business analysis.

## Repository Structure

```
Data_Eng_Assignment/
├── dbt_project/                  # DBT project configuration
│   ├── logs/                     # Log files
│   ├── models/                   # DBT transformation models
│   │   ├── marts/                # Dimensional models
│   │   └── staging/              # Initial transformation models
│   ├── tests/                    # Data validation tests
│   ├── .user.yml                 # User configuration
│   ├── dbt_project.yml           # DBT project configuration
│   └── profiles.yml              # DBT profile settings
├── .env                          # Environment variables
├── Documentation.pdf             # Project documentation
├── abr_extractor.py              # Script for extracting ABR data
├── abr_integration.py            # Script for integrating ABR data
├── common_crawl_extractor.py     # Script for extracting Common Crawl data
├── database_setup.sql            # Database schema creation
└── requirements.txt              # Project dependencies
```

## Pipeline Architecture

The data processing workflow follows these key stages:

1. **Data Extraction**: 
   - Extract website data from Common Crawl (200k+ URLs) using `common_crawl_extractor.py`
   - Extract business information from ABR using `abr_extractor.py`

2. **Data Integration**:
   - Merge and transform datasets using `abr_integration.py` and DBT models

3. **Data Storage**:
   - Store processed data in MySQL database initialized with `database_setup.sql`

4. **Data Transformation**:
   - Use DBT models in the `dbt_project` directory for cleaning and business logic
   - Create staging views for initial transformation
   - Build dimensional models in the marts folder

5. **Data Validation**:
   - Implement data quality checks using DBT tests

## Implementation Details

### Data Collection

```python
# From common_crawl_extractor.py
def process_url(url):
    try:
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        return extract_metadata(soup)
    except Exception as e:
        log_error(url, str(e))
        return None
```

### Database Schema

```sql
-- From database_setup.sql
CREATE TABLE companies (
    company_id INT AUTO_INCREMENT PRIMARY KEY,
    url VARCHAR(512) NOT NULL UNIQUE,
    company_name VARCHAR(255) NOT NULL,
    industry VARCHAR(255),
    abn CHAR(11),
    entity_type VARCHAR(50),
    state CHAR(3),
    postcode CHAR(4),
    registration_date DATE
);
```

### DBT Transformation

```sql
-- Example staging model
{{ config(materialized='view') }}

SELECT
    abn,
    TRIM(company_name) AS company_name,
    entity_type,
    state,
    postcode,
    registration_date
FROM {{ source('raw', 'abr_raw') }}
WHERE LENGTH(abn) = 11
```

## Data Quality Checks

- **Uniqueness**: Enforced through database constraints
- **Completeness**: NULL checks for mandatory fields
- **Consistency**: ABN format validation (11 digits)
- **Referential Integrity**: Relationship verification between tables

## Example Queries

### Top Industries Analysis

```sql
SELECT industry, COUNT(*) AS company_count
FROM companies
GROUP BY industry
ORDER BY company_count DESC
LIMIT 10;
```

### Recent Company Registrations

```sql
SELECT company_name, registration_date
FROM companies
WHERE registration_date > '2023-01-01'
ORDER BY registration_date DESC;
```

## Getting Started

1. Clone this repository:
   ```
   git clone https://github.com/Susanta2102/Data_Eng_Assignment.git
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Configure environment variables in `.env` file:
   ```
   DB_HOST=localhost
   DB_USER=username
   DB_PASSWORD=password
   DB_NAME=database
   ```

4. Set up the database:
   ```
   mysql -u username -p < database_setup.sql
   ```

5. Run data extraction scripts:
   ```
   python common_crawl_extractor.py
   python abr_extractor.py
   ```

6. Run data integration:
   ```
   python abr_integration.py
   ```

7. Execute DBT transformations:
   ```
   cd dbt_project
   dbt run
   ```

8. Validate data:
   ```
   dbt test
   ```

## Requirements

- Python 3.8+
- MySQL 8.0+
- DBT Core 1.3+
- BeautifulSoup4
- Requests

## References

- [Common Crawl](https://commoncrawl.org)
- [Australian Business Register](https://data.gov.au)
- [DBT Documentation](https://docs.getdbt.com)
- [MySQL Documentation](https://dev.mysql.com/doc)

## Author

Susanta Baidya

