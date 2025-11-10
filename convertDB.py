import os
import re
import pandas as pd
import urllib.parse
import tempfile
import json
import psycopg2
from supabase import create_client, Client
from dotenv import load_dotenv
from image_converter import convert_image_to_jpg
from psycopg2.extras import execute_batch

# --- Cấu hình Supabase và Kết nối Database ---
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
SUPABASE_HOST = os.environ.get("SUPABASE_HOST")
SUPABASE_DB = os.environ.get("SUPABASE_DB")
SUPABASE_USER = os.environ.get("SUPABASE_USER")
SUPABASE_PASSWORD = os.environ.get("SUPABASE_PASSWORD")
SUPABASE_PORT = os.environ.get("SUPABASE_PORT")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
# -----------------------------------------------

def process_and_ingest_data(csv_file: str, num_rows: int = 300):
    df = pd.read_csv(csv_file).head(num_rows).copy()

    records_to_insert = []
    
    # Tạo chuỗi kết nối trực tiếp (URI) từ các biến môi trường
    password_encoded = urllib.parse.quote_plus(SUPABASE_PASSWORD)
    db_uri_safe = (
        f"postgresql://{SUPABASE_USER}:{password_encoded}@{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}"
    )
    
    print(f"Bat dau xu ly {len(df)} dong...")

    # Vòng lặp xử lý Ảnh, Upload Storage và tạo Records
    for index, row in df.iterrows():
        base64_data = row['base64_column']
        image_url = None
        
        if pd.isna(base64_data) or not isinstance(base64_data, str) or len(base64_data) < 50:
            image_url = None
        else:
            cleaned_base64 = base64_data.strip('"').replace('\n', '').replace('\r', '').replace(' ', '')
            cleaned_name = re.sub(r'[^a-zA-Z0-9_]', '', row['name'].strip()) 
            filename_base = f"{cleaned_name}_{index}"
            
            local_file_path = os.path.join(tempfile.gettempdir(), f"{filename_base}.jpg") 
            storage_path = f"images/{filename_base}.jpg"
            
            try:
                convert_image_to_jpg(cleaned_base64, local_file_path)
            except Exception as e:
                print(f"Loi giai ma Base64 o dong {index}: {e}. Bo qua anh.")
            else:
                try:
                    with open(local_file_path, 'rb') as f:
                        supabase.storage.from_('place_images').upload(
                            file=f, 
                            path=storage_path, 
                            file_options={"content-type": "image/jpeg"}
                        )
                    image_url = supabase.storage.from_('place_images').get_public_url(storage_path)
                except Exception as e:
                    print(f"Loi upload Storage: {e}. Bo qua.")
                finally:
                    if os.path.exists(local_file_path):
                        os.remove(local_file_path)

        # Chuẩn bị Record để Upsert
        records_to_insert.append({
            "name": row['name'], "type": row['type'], "rating": row['rating'], 
            "count": row['count'], "address": row['address'], "comment": row['comment'], 
            "lat": row['lat'], "lon": row['lon'], "image_url": image_url,
            "geometry": f"SRID=4326;POINT({row['lon']} {row['lat']})"
        })

    # 4. Upsert (Chèn/Cập nhật) hàng loạt vào PostgreSQL
    print("Bat dau Upsert du lieu vao PostgreSQL...")
    df_final = pd.DataFrame(records_to_insert)
    
    # Định nghĩa SQL cho INSERT (Dùng %s cho tham số hóa)
    insert_sql = """
    INSERT INTO place (
        name, type, rating, count, address, comment, lat, lon, geometry, image_url
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_GeomFromText(%s), 4326), %s
    )
    ON CONFLICT (name) DO UPDATE SET
        rating = EXCLUDED.rating, comment = EXCLUDED.comment, count = EXCLUDED.count, image_url = EXCLUDED.image_url, geometry = EXCLUDED.geometry;
    """
    
    # Chuyển DataFrame sang list of tuples (psycopg2.extras.execute_batch)
    data_tuples = []
    for record in records_to_insert:
        data_tuple = (
            record['name'], record['type'], record['rating'], record['count'], 
            record['address'], record['comment'], record['lat'], record['lon'], 
            record['geometry'], record['image_url']
        )
        data_tuples.append(data_tuple)
    
    try:
        with psycopg2.connect(db_uri_safe) as conn:
            with conn.cursor() as cur:
                print(f"🚀 Inserting {len(data_tuples)} records in batches...")
                
                # EXECUTE BATCH
                execute_batch(cur, insert_sql, data_tuples, page_size=500)
                
            conn.commit()
            print(f"✅ HOAN TAT! Da xu ly va upsert {len(df)} dong.")
            
    except Exception as e:
        print(f"❌ LOI CHEN DU LIEU CUOI CUNG: {e}")

# --- Chạy hàm chính ---
if __name__ == '__main__':
    # Bỏ tham số DB_URL_DIRECT khỏi hàm gọi
    process_and_ingest_data("cleaned_data.csv", num_rows=1)