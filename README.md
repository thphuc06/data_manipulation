Tuyệt vời\! Tôi sẽ tổng hợp lại tất cả các thao tác bạn đã thực hiện với Supabase (PostgreSQL) và cung cấp code cuối cùng để bạn ghi chú lại.

Quá trình này đã giúp bạn thiết lập một bảng dữ liệu **Hệ thống Thông tin Địa lý (GIS)** hoàn chỉnh, có khả năng **Tìm kiếm Địa lý (Geospatial Search)** và **Tìm kiếm Toàn văn bản (Full-Text Search - FTS) cho Tiếng Việt**.

-----

## 📝 Tổng kết Ghi chú cho Supabase (PostgreSQL)

### 1\. Cấu trúc Bảng Đích

Bảng chính là `"Place"` với các cột sau:

| Cột | Kiểu dữ liệu | Vai trò | Ghi chú |
| :--- | :--- | :--- | :--- |
| **id** | `SERIAL PRIMARY KEY` | Khóa chính | Tự tăng, duy nhất. |
| **name** | `TEXT NOT NULL UNIQUE` | Tên quán | Bắt buộc, không trùng lặp (dùng cho Upsert). |
| **...** | `TEXT, NUMERIC, INTEGER` | Dữ liệu chính | `type`, `rating`, `count`, `address`, `comment`, `image_url`. |
| **geometry** | `GEOMETRY(Point, 4326)` | **Địa lý** | Dùng PostGIS để lưu tọa độ. |
| **comment\_tsv** | `tsvector` | **FTS Tiếng Việt** | Cột được tự động cập nhật để tìm kiếm không dấu. |

-----

### 2\. Code SQL (Thực hiện trong **SQL Editor** Supabase)

Bạn cần chạy các khối lệnh này theo thứ tự. Nếu bạn đã chạy các lệnh **DROP** ở các bước trước, bạn có thể bỏ qua chúng.

#### A. Khởi tạo Cơ sở dữ liệu và Bảng (SETUP)

| Lệnh | Mục đích |
| :--- | :--- |
| `CREATE EXTENSION IF NOT EXISTS postgis;` | **BẬT PostGIS** (Bắt buộc cho cột `GEOMETRY`). |
| `CREATE EXTENSION IF NOT EXISTS unaccent;` | **BẬT Unaccent** (Bắt buộc cho FTS Tiếng Việt không dấu). |
| `DROP TABLE IF EXISTS "Place";` | Xóa bảng cũ nếu muốn tạo lại. |
| **Tạo Bảng** | Đặt tên trong ngoặc kép `"Place"` để giữ chữ hoa. |

```sql
-- CHẠY 1 LẦN DUY NHẤT: BẬT EXTENSIONS
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS unaccent;

-- TẠO LẠI BẢNG (nếu cần)
DROP TABLE IF EXISTS "Place";
CREATE TABLE "Place" (
    id SERIAL PRIMARY KEY, 
    name TEXT NOT NULL UNIQUE,
    type TEXT,
    rating NUMERIC,
    count INTEGER,
    address TEXT,
    comment TEXT,
    lat NUMERIC,
    lon NUMERIC,
    geometry GEOMETRY(Point, 4326), 
    image_url TEXT,
    -- Thêm cột FTS (chỉ mục toàn văn bản)
    comment_tsv tsvector
);
```

#### B. Tạo Index (Tối ưu hóa Truy vấn)

| Lệnh | Mục đích |
| :--- | :--- |
| `idx_place_name_type` | Tăng tốc độ lọc theo tên và loại. |
| `idx_place_place_geometry_gist` | **BẮT BUỘC** cho tìm kiếm địa lý (ví dụ: `ST_DWithin`). |
| `idx_place_comment_gin` | **BẮT BUỘC** cho tìm kiếm Toàn văn bản (FTS). |

```sql
-- TẠO CÁC CHỈ MỤC THÔNG THƯỜNG
CREATE INDEX idx_place_name_type ON "Place" (name, type);
CREATE INDEX idx_place_rating ON "Place" (rating);

-- TẠO CHỈ MỤC GEOSPATIAL (BẮT BUỘC CHO POSTGIS)
CREATE INDEX idx_place_place_geometry_gist ON "Place" USING GIST (geometry);

-- TẠO CHỈ MỤC FULL-TEXT SEARCH (FTS) TIẾNG VIỆT
CREATE INDEX idx_place_comment_gin ON "Place" USING GIN (comment_tsv);
```

#### C. Cài đặt Tự động hóa FTS (Trigger cho Tiếng Việt)

| Lệnh | Mục đích |
| :--- | :--- |
| `CREATE OR REPLACE FUNCTION...` | Tạo hàm tính toán `tsvector` dùng cấu hình `simple` và `unaccent` cho tiếng Việt. |
| `CREATE OR REPLACE TRIGGER...` | Thiết lập Trigger chạy **trước** lệnh `INSERT` hoặc `UPDATE` của cột `comment`, đảm bảo `comment_tsv` luôn được cập nhật. |

```sql
-- 1. TẠO HÀM (FUNCTION) CHO TÍNH TOÁN TSVECTOR (TIẾNG VIỆT)
CREATE OR REPLACE FUNCTION public.place_tsvector_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    -- Tính toán tsvector không dấu cho tiếng Việt
    IF TG_OP = 'INSERT' OR NEW.comment IS DISTINCT FROM OLD.comment THEN
        NEW.comment_tsv := to_tsvector('simple', unaccent(NEW.comment));
    END IF;
    RETURN NEW;
END;
$function$;

-- 2. TẠO TRIGGER TỰ ĐỘNG GỌI HÀM
CREATE OR REPLACE TRIGGER comment_tsv_update
BEFORE INSERT OR UPDATE OF comment ON "Place"
FOR EACH ROW EXECUTE FUNCTION public.place_tsvector_trigger();

-- 3. CẬP NHẬT DỮ LIỆU CŨ (CHỈ CHẠY 1 LẦN NẾU ĐÃ CÓ DATA)
UPDATE "Place" 
SET comment_tsv = to_tsvector('simple', unaccent(comment));
```

-----

### 3\. Code Python (Để Chèn Dữ liệu CSV)

Bạn sẽ làm việc theo một luồng logic rất rõ ràng: **Đọc CSV $\rightarrow$ Xử lý Ảnh $\rightarrow$ Lưu Storage $\rightarrow$ Lấy URL $\rightarrow$ Upsert DB.**


-----

## 🛠️ Quy trình Tổng thể và Code Chi tiết

Bạn cần đảm bảo file **`cleaned_data.csv`** và **`image_converter.py`** nằm cùng cấp với file code chính.

### 1\. File: `image_converter.py` (Chức năng: Giải mã & Convert)

Đoạn code bạn gửi là chính xác, nhưng cần thêm `import` và `return` để sử dụng được.

```python
import base64
from io import BytesIO
from PIL import Image

def convert_image_to_jpg(input_string, output_path):
    data = input_string
    data = data[data.index(',')+1:]
    
    bytes_decoded = base64.b64decode(data)
    image = Image.open(BytesIO(bytes_decoded))

    out_jpg = image.convert("RGB")
    out_jpg.save(output_path, "JPEG")
    
    # Rất quan trọng: Phải đóng file để giải phóng bộ nhớ
    image.close()
    out_jpg.close()
    
    return True 
```

-----

### 2\. File Code Chính: `supabase_ingestion.py`

Đây là file thực hiện việc đọc 300 dòng CSV, gọi hàm chuyển đổi, tải lên Supabase Storage và lưu URL vào PostgreSQL.

#### ⚠️ Yêu cầu và Cài đặt

1.  **Cài đặt:** `pip install pandas supabase-py psycopg2-binary Pillow`
2.  **Chuẩn bị:** File `.env` phải chứa `SUPABASE_URL`, `SUPABASE_KEY`, và chuỗi kết nối **trực tiếp** `DB_URL_DIRECT` (port 5432) cho psycopg.

<!-- end list -->

```python
import os
import pandas as pd
import psycopg
from supabase import create_client, Client
from dotenv import load_dotenv
from image_converter import convert_image_to_jpg # Hàm giải mã ảnh

# --- Cấu hình Supabase và Kết nối Database ---
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
DB_URL_DIRECT = os.environ.get("DB_URL_DIRECT")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
# -----------------------------------------------

def process_and_ingest_data(csv_file: str, db_url_direct: str, num_rows: int = 300):
    df = pd.read_csv(csv_file).head(num_rows).copy()
    
    # Kiem tra cột 'base64_column' co ton tai khong
    if 'base64_column' not in df.columns:
        print("❌ LOI: File CSV khong co cot 'base64_column'. Khong the xu ly anh.")
        # Chinh sua cot image_url mac dinh la None
        df['image_url'] = None
        # Tien hanh Upsert du lieu ma khong co anh (Neu muon)
        # Bỏ qua logic xử lý ảnh và tiếp tục Upsert ở Bước 3 và 4
        # ... (để đơn giản, ta sẽ chỉ báo lỗi và dừng)
        return

    # 1. Chuẩn bị dữ liệu và URL Storage
    records_to_insert = []
    print(f"Bat dau xu ly {len(df)} dong...")

    # 2. Vòng lặp xử lý Ảnh, Upload Storage và tạo Records
    for index, row in df.iterrows():
        base64_data = row['base64_column']
        image_url = None
        
        # --- A. CHECK: Chi xu ly anh neu base64_data hop le ---
        if pd.isna(base64_data) or not isinstance(base64_data, str) or len(base64_data) < 50:
            # Du lieu base64 khong ton tai, khong phai string, hoac qua ngan (khong hop le)
            image_url = None
        else:
            # Du lieu Base64 co ve hop le -> Tien hanh xu ly
            filename_base = f"{row['name'].replace(' ', '_')}_{index}"
            # Su dung thu muc tam thoi de luu file
            local_file_path = f"/tmp/{filename_base}.jpg" 
            storage_path = f"images/{filename_base}.jpg"
            
            # Giải mã và Lưu tạm ảnh
            try:
                convert_image_to_jpg(base64_data, local_file_path)
            except Exception as e:
                print(f"Loi giai ma Base64 o dong {index}: {e}. Bo qua anh.")
                
            else:
                # Upload len Supabase Storage
                try:
                    with open(local_file_path, 'rb') as f:
                        supabase.storage.from_('quanan_images').upload(
                            file=f, 
                            path=storage_path, 
                            file_options={"content-type": "image/jpeg"},
                            # Su dung upsert=True de ghi de neu file da ton tai
                            upsert=True
                        )
                    # Lay URL cong khai
                    image_url = supabase.storage.from_('quanan_images').get_public_url(storage_path)
                except Exception as e:
                    print(f"Loi upload Storage: {e}. Bo qua.")
                finally:
                    # Xoa file tam
                    if os.path.exists(local_file_path):
                        os.remove(local_file_path)

        # 3. Chuẩn bị Record để Upsert (chèn/cập nhật)
        records_to_insert.append({
            "name": row['name'],
            "type": row['type'],
            "rating": row['rating'],
            "count": row['count'],
            "address": row['address'],
            "comment": row['comment'],
            "lat": row['lat'],
            "lon": row['lon'],
            "geometry": f"SRID=4326;POINT({row['lon']} {row['lat']})",
            "image_url": image_url # Luu None hoac URL anh
        })

    # 4. Upsert (Chèn/Cập nhật) hàng loạt vào PostgreSQL
    print("Bat dau Upsert du lieu vao PostgreSQL...")
    
    # Chuyển list records thành DataFrame
    df_final = pd.DataFrame(records_to_insert)
    
    with psycopg.connect(db_url_direct) as conn:
        with conn.cursor() as cur:
            cur.copy_from(
                df_final, 
                "Place", 
                columns=df_final.columns.tolist(),
                on_conflict='do update set rating = EXCLUDED.rating, comment = EXCLUDED.comment, count = EXCLUDED.count, image_url = EXCLUDED.image_url, geometry = EXCLUDED.geometry'
            )
        conn.commit()
        print(f"✅ HOAN TAT! Da xu ly va upsert {len(df)} dong.")

# --- Chạy hàm chính ---
if __name__ == '__main__':
    # THAY 'cleaned_data.csv' bằng tên file thực tế của bạn
    process_and_ingest_data("cleaned_data.csv", DB_URL_DIRECT, num_rows=1)

```