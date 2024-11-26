import boto3
import csv
import os
import requests

# Initialize S3 client
s3 = boto3.client('s3')

BASE_URL = os.environ.get("BASE_URL")
DEBUG = os.environ.get("DEBUG")

def fetch_access_token(refresh_token):
    TOKEN_URL = f"{BASE_URL}/auth/refresh-token"
    header = {
        "Authorization": f"Bearer {refresh_token}"
    }

    try:
        response = requests.post(TOKEN_URL, headers=header)
        response.raise_for_status()
        data = response.json()
        return data.get("accessToken")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching access token: {e}")
        return None
        

def fetch_isbn_from_google_books(title):
    google_books_api = "https://www.googleapis.com/books/v1/volumes"
    params = {"q": f"intitle:{title}"}

    if DEBUG:
        print(f"Fetching ISBN for title: {title}")
    
    try:
        response = requests.get(google_books_api, params=params)
        response.raise_for_status()
        data = response.json()
        
        if "items" in data:
            for item in data["items"]:
                industry_identifiers = item.get("volumeInfo", {}).get("industryIdentifiers", [])
                for identifier in industry_identifiers:
                    if identifier.get("type") == "ISBN_13":
                        return identifier.get("identifier")
        return None
    except requests.exceptions.RequestException as e:
        if DEBUG:
            print(f"Error fetching ISBN from Google Books API: {e}")
        return None
    
def fetch_google_self_link(isbn, title):
    try:
        response = requests.get(f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}")
        response.raise_for_status()
        data = response.json()
        self_link = data.get("items", [{}])[0].get("selfLink", None)
        if not self_link:
            response = requests.get(f"https://www.googleapis.com/books/v1/volumes?q=intitle:{title}")
            response.raise_for_status()
            data = response.json()
            self_link = data.get("items", [{}])[0].get("selfLink", None)
        return self_link
    except requests.exceptions.RequestException as e:
        if DEBUG:
            print(f"Error fetching Google self link: {e}")
        return None

def lambda_handler(event, context):
    REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN")
    ACCESS_TOKEN = fetch_access_token(REFRESH_TOKEN)

    if not ACCESS_TOKEN:
        print("Failed to obtain access token. Exiting.")
        exit(1)

    HEADERS = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    for record in event['Records']:
        try:
            
          s3_info = record['body']
          bucket_name = s3_info['bucket']['name']
          object_key = s3_info['object']['key']
          
          local_file_path = f"/tmp/{os.path.basename(object_key)}"
          s3.download_file(bucket_name, object_key, local_file_path)
          
          with open(local_file_path, mode='r', encoding='utf-8') as csv_file:
              reader = csv.DictReader(csv_file)
              for row in reader:
                  title = row.get("Title")
                  isbn_or_uid = row.get("ISBN/UID")
                  if not isbn_or_uid.isdigit() or len(isbn_or_uid) != 13:
                      isbn_or_uid = fetch_isbn_from_google_books(title)
                  created_by_id = os.path.splitext(os.path.basename(object_key))[0]
                  if DEBUG:
                      print(f"Created by ID: {created_by_id}")
                      print(f"Processing book: {title} with ISBN/UID: {isbn_or_uid}")

                  google_self_link = fetch_google_self_link(isbn_or_uid, title)

                  # Post to /books
                  book_payload = {
                      "title": title,
                      "isbn": isbn_or_uid,
                      "tags": tags,
                      "createdById": 69,
                      "quickLink": google_self_link
                  }
                  books_response = requests.post(
                      f"{BASE_URL}/books",
                      json=book_payload,
                      headers=HEADERS
                  )
                  books_response.raise_for_status()
                  book = books_response.json()
                  book_id = book["id"]

                  if DEBUG:
                      print(f"Book {title} created with ID: {book_id}")

                  # Post to /users-books
                  user_id = created_by_id
                  dates_read = row.get("Dates Read", "").split("-")
                  date_started = dates_read[0].strip() if dates_read else None
                  date_finished = dates_read[-1].strip() if len(dates_read) > 1 else None
                  user_notes = row.get("Review", "No notes provided")
                  star_rating = row.get("Star Rating", None)
                  user_rating = int(star_rating * 2) if star_rating else None

                  user_books_payload = {
                      "userRating": user_rating,
                      "dateStarted": date_started,
                      "dateFinished": date_finished,
                      "userNotes": user_notes,
                      "import": True
                  }
                  users_books_response = requests.post(
                      f"{BASE_URL}/users-books/{user_id}/{book_id}",
                      json=user_books_payload,
                      headers=HEADERS
                  )
                  users_books_response.raise_for_status()

                  if DEBUG:
                      print(f"Book {title} processed successfully.")

        except Exception as e:
            print(f"Error processing record: {e}")
            continue

    return {
        "statusCode": 200,
        "body": "CSV processing and API updates completed."
    }

if __name__ == "__main__":
    import json
    with open("event.json") as f:
        event = json.load(f)
    context = {}
    response = lambda_handler(event, context)
    print(response)
