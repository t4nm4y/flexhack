import asyncio
from scrape import ascrape_playwright
from ai_extract import get_categories
import json
import psycopg2
import requests





async def async_scrape(url, tags):
    html_content = await ascrape_playwright(url, tags)
    print("Extracting content with LLM")

    token_limit=15000
    html_content_fitted = html_content[:token_limit]
    print("content:\n",html_content)
    # print("")
    res=get_categories(html_content_fitted)
    return res

tags=["h1", "h2", "h3", "span", "a"]
# url="https://www.sangeethamobiles.com/"
# url="https://myindianthings.com/"
# url="https://www.flipkart.com/"
# url="https://www.amazon.in/"
# url="https://www.cynohub.com/"
# url="https://interwove.in/"
# url="https://www.flexmoney.in/"


# Replace these variables with your PostgreSQL connection details
db_host = "localhost"
db_port = 5432
db_name = "postgres"
db_user = "postgres"
db_password = "postgres"

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host=db_host,
    port=db_port,
    database=db_name,
    user=db_user,
    password=db_password
)

# Create a cursor object to execute SQL commands
cursor = conn.cursor()

# Query fm_web_link values from the table
cursor.execute("SELECT fm_web_link, merchant_id FROM flexpaymerchant")




# Fetch all rows
fm_web_links = cursor.fetchall()

cursor.execute("SELECT merchantId FROM merchant_metadata")
existing_merchant_ids = set(row[0] for row in cursor.fetchall())

# Close the cursor (not needed for the upcoming iteration)
cursor.close()

def check_status(url):
    try:
        response = requests.get(url, allow_redirects=True, timeout=5)
        return response.status_code == 200
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return False
    
# Iterate through fm_web_links and apply your function
async def temp():
    for fm_web_link in fm_web_links:
        # Extract the link from the tuple
        url = fm_web_link[0]
        merchant_id = fm_web_link[1]

        if url.startswith("www."):
            url = "https://" + url
        
        print("\nurl:", url)
        
        isLive=check_status(url)
        print("the site islive: ", isLive)

        cursor = conn.cursor()
        
        if merchant_id in existing_merchant_ids:
            cursor.execute("""
                UPDATE merchant_metadata 
                SET is_healthy = %s 
                WHERE merchantId = %s
            """, (isLive, merchant_id))
            conn.commit()
        else:
           cursor.execute("""
                INSERT INTO merchant_metadata (
                    merchantId, is_healthy
                ) VALUES (%s, %s)
            """, (merchant_id, isLive))
           
           conn.commit()

        cursor.close()
        if not isLive: 
            print("Site is not live, skipping...")
            continue

        cursor = conn.cursor()
        if merchant_id in existing_merchant_ids:
            print("fetching")
            cursor.execute(f"Select category from merchant_metadata where merchantId={merchant_id}")
            category=cursor.fetchall()
            print("category:",category)
            cursor.close()
            if category:
                print(f"\nalready exists. Skipping...\n")
                continue
        else: print("not present")

        res=await async_scrape(url,tags)
        json_data = json.loads(res)
        # Extract required data from json_data
        category = json_data.get('categories')
        price_range = json_data.get('price-range')

        # Insert data into merchant_metadata table
        if merchant_id not in existing_merchant_ids:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO merchant_metadata (
                    merchantId, category, ticket_size
                ) VALUES (%s, %s, %s)
            """, (merchant_id, json.dumps(category), json.dumps(price_range)))
            conn.commit()
            cursor.close()
        else:
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE merchant_metadata 
                SET category = %s, ticket_size = %s 
                WHERE merchantId = %s
            """, (json.dumps(category), json.dumps(price_range), merchant_id))
            conn.commit()
            cursor.close()

        print("added successfully")
        # Add a 60-second delay
        await asyncio.sleep(62)

# Run the asynchronous loop
asyncio.run(temp())
# res=asyncio.run(async_scrape(url,tags))
# json_data = json.loads(res)
# print(json_data)
conn.close()