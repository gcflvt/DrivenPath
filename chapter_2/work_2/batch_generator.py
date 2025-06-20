import random
import csv
import logging
import uuid
import os
import polars as pl
from faker import Faker
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler()]
)

def create_data(locale: str) -> Faker:
    """Create Faker instance with specified locale"""
    logging.info(f"Creating synthetic data for {locale.split('_')[-1]} country code")
    return Faker(locale)

def generate_record(fake: Faker) -> list:
    """Generate a single fake user record"""
    person_name = fake.name()
    user_name = person_name.replace(" ", "").lower()
    email = f"{user_name}@{fake.free_email_domain()}"
    
    return [
        person_name, user_name, email, fake.ssn(), fake.date_of_birth(),
        fake.address().replace("\n", ", "), fake.phone_number(),
        fake.mac_address(), fake.ipv4(), fake.iban(),
        fake.date_time_between("-1y"),  # accessed_at
        random.randint(0, 36000),       # session_duration
        random.randint(0, 1000),        # download_speed
        random.randint(0, 800),         # upload_speed
        random.randint(0, 2000000)       # consumed_traffic
    ]

def write_to_csv(file_path: str, rows: int) -> None:
    """Generate data and write to CSV"""
    fake = create_data("es_MX")
    headers = [
        "person_name", "user_name", "email", "personal_number", "birth_date",
        "address", "phone", "mac_address", "ip_address", "iban", "accessed_at",
        "session_duration", "download_speed", "upload_speed", "consumed_traffic"
    ]

    # Create directory if doesn't exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, mode="w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        
        for _ in range(rows):
            try:
                writer.writerow(generate_record(fake))
            except Exception as e:
                logging.error(f"Error generating record: {e}")
                continue

    logging.info(f"Successfully written {rows} records to {file_path}")

def add_id(file_path: str) -> None:
    """Add UUID column to CSV"""
    try:
        df = pl.read_csv(file_path)
        df = df.with_columns(pl.lit([str(uuid.uuid4()) for _ in range(df.height)]).alias("unique_id"))
        df.write_csv(file_path)
        logging.info("UUIDs added successfully")
    except Exception as e:
        logging.error(f"Error adding UUIDs: {e}")
        raise

def update_datetime(file_path: str) -> None:
    """Update accessed_at timestamps"""
    try:
        current_time = datetime.now().replace(microsecond=0)
        yesterday_time = str(current_time - timedelta(days=1))
        
        df = pl.read_csv(file_path)
        df = df.with_columns(pl.lit(yesterday_time).alias("accessed_at"))
        df.write_csv(file_path)
        logging.info("Timestamps updated successfully")
    except Exception as e:
        logging.error(f"Error updating timestamps: {e}")
        raise

def main():
    try:
        # Use absolute path for output
        output_dir = os.path.join(os.getcwd(), "output")
        os.makedirs(output_dir, exist_ok=True)
        
        current_date = datetime.now().strftime("%Y-%m-%d")
        csv_path = os.path.join(output_dir, f"synth_data_{current_date}.csv")
        
        logging.info(f"Starting batch processing for {current_date}")
        
        write_to_csv(csv_path, rows=100_000)
        add_id(csv_path)
        update_datetime(csv_path)
        
        logging.info(f"Finished processing. File saved to: {csv_path}")
        
    except Exception as e:
        logging.error(f"Batch processing failed: {e}")
        raise

if __name__ == "__main__":
    main()