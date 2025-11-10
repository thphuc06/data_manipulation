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
