import os
import gzip
import io
import requests
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database Connection using pymysql
engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
)

def get_cc_index():
    """Fetch the latest Common Crawl index files."""
    index_url = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-23/cc-index.paths.gz"
    response = requests.get(index_url)
    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
        return [f"https://data.commoncrawl.org/{line.decode().strip()}" for line in gz]

def is_australian(url):
    """Check if the URL belongs to an Australian domain."""
    domain = urlparse(url).netloc
    return any(domain.endswith(ext) for ext in ['.com.au', '.net.au', '.org.au', '.edu.au', '.gov.au'])

def extract_metadata(url):
    """Extract metadata from the webpage."""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(url, timeout=15, headers=headers)
        response.raise_for_status()  # Raise exception for HTTP errors
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract company name from common patterns
        name = None
        # Check meta tags first
        meta_title = soup.select_one('meta[property="og:title"]')
        if meta_title and meta_title.get('content'):
            name = meta_title.get('content').strip()
        # Then check title tag
        if not name and soup.title:
            name = soup.title.text.strip()
        # Finally check h1
        if not name:
            h1 = soup.select_one('h1')
            if h1:
                name = h1.text.strip()
        
        # Simple industry classification
        industry = 'Other'
        text = soup.get_text().lower()
        if any(kw in text for kw in ['software', 'tech', 'cloud', 'ai', 'digital', 'computing']):
            industry = 'Technology'
        elif any(kw in text for kw in ['shop', 'store', 'buy', 'cart', 'retail', 'purchase']):
            industry = 'Retail'
        elif any(kw in text for kw in ['bank', 'loan', 'finance', 'insurance', 'invest', 'wealth']):
            industry = 'Finance'
        elif any(kw in text for kw in ['health', 'medical', 'doctor', 'hospital', 'clinic']):
            industry = 'Healthcare'
        elif any(kw in text for kw in ['education', 'school', 'university', 'college', 'learning']):
            industry = 'Education'

        return {'url': url, 'company_name': name, 'industry': industry}

    except Exception as e:
        print(f"❌ Error processing {url}: {str(e)}")
        return None

def process_cc():
    """Process the Common Crawl dataset to extract Australian company data."""
    index_files = get_cc_index()
    print(f"Found {len(index_files)} index files.")

    # Process first 5 files (~100k URLs per file)
    for idx, file_url in enumerate(index_files[:5]):
        try:
            print(f"🔄 Processing batch {idx+1}/5: {file_url}")
            response = requests.get(file_url, stream=True)
            response.raise_for_status()  # Check for HTTP errors

            # Parse the Common Crawl index file which is JSONL format (not just URLs)
            urls = []
            with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
                for line in gz:
                    try:
                        import json
                        data = json.loads(line.decode().strip())
                        if 'url' in data:
                            urls.append(data['url'])
                    except:
                        continue

            # Filter only Australian company URLs
            aus_urls = [url for url in urls if is_australian(url)]
            print(f"Found {len(aus_urls)} Australian URLs out of {len(urls)} total URLs.")

            if not aus_urls:
                print("No Australian URLs found in this batch, skipping...")
                continue

            # Process in smaller batches to avoid memory issues
            batch_size = 100
            all_results = []
            
            for i in range(0, len(aus_urls), batch_size):
                batch = aus_urls[i:i+batch_size]
                print(f"Processing URLs {i+1}-{min(i+batch_size, len(aus_urls))} of {len(aus_urls)}...")
                
                # Process URLs with a timeout
                results = []
                for url in tqdm(batch, desc="Extracting metadata", leave=False):
                    result = extract_metadata(url)
                    if result:
                        results.append(result)
                
                all_results.extend(results)
                
                # Save intermediate results
                if results:
                    df_batch = pd.DataFrame(results)
                    df_batch.to_sql('common_crawl_raw', engine, if_exists='append', index=False)
                    print(f"✅ Saved {len(df_batch)} records to database.")

            # Final save for this batch
            print(f"Completed batch {idx+1}: Processed {len(all_results)} Australian websites.")

        except Exception as e:
            print(f"⚠️ Error processing batch {file_url}: {str(e)}")

if __name__ == "__main__":
    process_cc()