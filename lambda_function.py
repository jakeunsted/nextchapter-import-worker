import boto3
import csv
import os
import requests
import json
import time

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
            # Decode the SQS message body
            message_body = record.get('body')
            if isinstance(message_body, str):
                message_body = json.loads(message_body)

            # Extract S3 event notification details
            s3_event = message_body.get('Records', [])[0]
            bucket_name = s3_event.get('s3', {}).get('bucket', {}).get('name')
            object_key = s3_event.get('s3', {}).get('object', {}).get('key')
            
            # Debugging logs
            if DEBUG:
                print(f"Bucket: {bucket_name}, Object Key: {object_key}")
            
            # Download and process the file
            local_file_path = f"/tmp/{os.path.basename(object_key)}"
            s3.download_file(bucket_name, object_key, local_file_path)
            
            with open(local_file_path, mode='r', encoding='utf-8') as csv_file:
                reader = csv.DictReader(csv_file)
                for row in reader:
                    try:
                        # Skip adding if the book was not finished
                        if row.get("Read Status") == "did-not-finish":
                          continue
                        
                        title = row.get("Title")
                        isbn_or_uid = row.get("ISBN/UID")
                        if not isbn_or_uid.isdigit() or len(isbn_or_uid) != 13:
                            isbn_or_uid = fetch_isbn_from_google_books(title)
                        filename = os.path.basename(object_key)
                        user_id, part = filename.split("_part")[0], filename.split("_part")[1].split(".csv")[0]
                        created_by_id = int(user_id)
                        tags = []
                        
                        google_self_link = fetch_google_self_link(isbn_or_uid, title)

                        book_payload = {
                            "title": title,
                            "isbn": isbn_or_uid,
                            "tags": tags,
                            "createdById": 9,
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
                            print(f"Book {title} created with ID: {book_id} from import part: {part}")

                        user_id = created_by_id

                        # Process dates - if there is a "-" then it is a range, otherwise it is a single date
                        dates_read = row.get("Dates Read", "")
                        if "-" in dates_read:
                            date_started, date_finished = map(str.strip, dates_read.split("-"))
                        else:
                            date_started = dates_read.strip()
                            date_finished = date_started
                        # Check if date_started or date_finished is just a year
                        if date_started and len(date_started) == 4 and date_started.isdigit():
                            date_started = f"{date_started}-01-01"
                        if date_finished and len(date_finished) == 4 and date_finished.isdigit():
                            date_finished = f"{date_finished}-01-01"

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

                        for attempt in range(3):  # Retry up to 3 times
                            try:
                                users_books_response = requests.post(
                                    f"{BASE_URL}/users-books/{user_id}/{book_id}",
                                    json=user_books_payload,
                                    headers=HEADERS
                                )
                                users_books_response.raise_for_status()
                                if DEBUG:
                                    print(f"Book {title} processed successfully.")
                                break
                            except requests.exceptions.RequestException as e:
                                if DEBUG:
                                    print(f"Attempt {attempt + 1} failed for {title}: {e}")
                                if attempt < 2:  # Retry for the first 2 attempts
                                    time.sleep(2 ** attempt)  # Exponential backoff: 2, 4 seconds
                                    continue
                                else:
                                    print(f"Final failure for {title}: {e}")
                                    break  # Log and skip the book after 3 attempts

                    except Exception as e:
                        print(f"Error processing record for book {row.get('Title', 'Unknown')}: {e}")
                        continue  # Skip this record and move to the next

        except Exception as e:
            print(f"Error processing record: {e}")
            continue  # Skip this record and move to the next

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
