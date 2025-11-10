"""
🔄 COMPLETE DATA PIPELINE
Excel/CSV → Geocoding → Spatial Filtering → Supabase

Workflow:
1. Load Excel/CSV
3. Filter points within district boundaries
4. Upload to Supabase
"""

import pandas as pd
import geopandas as gpd
import requests
from supabase import create_client, Client
import time
import json
import os
from dotenv import load_dotenv
import osmnx as ox
from sqlalchemy import create_engine

# Load environment variables
load_dotenv()

# ==================== CONFIG ====================
VIETMAP_API_KEY = os.getenv('VIETMAP_API_KEY', 'YOUR_VIETMAP_KEY')
GEOAPIFY_API_KEY = os.getenv('GEOAPIFY_API_KEY', 'f1f9fa86b35b4087b305c6bb4d6250be')

# Supabase
SUPABASE_URL = os.getenv('SUPABASE_URL', 'YOUR_SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', 'YOUR_SUPABASE_KEY')
SUPABASE_TABLE = os.getenv('SUPABASE_TABLE', 'locations')

# PostGIS (PostgreSQL)
POSTGIS_HOST = os.getenv('POSTGIS_HOST', 'localhost')
POSTGIS_PORT = os.getenv('POSTGIS_PORT', '5432')
POSTGIS_DB = os.getenv('POSTGIS_DB', 'locations_db')
POSTGIS_USER = os.getenv('POSTGIS_USER', 'postgres')
POSTGIS_PASSWORD = os.getenv('POSTGIS_PASSWORD', 'YOUR_PASSWORD')
POSTGIS_TABLE = os.getenv('POSTGIS_TABLE', 'locations')

# Note: KHÔNG CẦN file GeoJSON!
# Pipeline tự động tải ranh giới từ OpenStreetMap bằng OSMnx

# ==================== STEP 1: GEOCODING ====================

def geocode_vietmap(address):
    """Geocode bằng Vietmap"""
    url = "https://maps.vietmap.vn/api/search/v3"
    params = {'apikey': VIETMAP_API_KEY, 'text': address}

    try:
        response = requests.get(url, params=params, timeout=5)
        response.raise_for_status()
        data = response.json()

        if data and len(data) > 0:
            return data[0].get('lat'), data[0].get('lng')
    except Exception as e:
        print(f"Vietmap error: {e}")

    return None, None


def geocode_batch(df, address_column='address'):
    """
    Geocode hàng loạt địa chỉ trong DataFrame

    Args:
        df: DataFrame chứa địa chỉ
        address_column: Tên cột chứa địa chỉ

    Returns:
        DataFrame với lat/lon
    """
    print("\n🔍 STEP 1: GEOCODING ADDRESSES...")

    lats, lons = [], []

    for idx, row in df.iterrows():
        address = f"{row[address_column]}, Quận 1, Thành phố Hồ Chí Minh"
        lat, lon = geocode_vietmap(address)

        lats.append(lat)
        lons.append(lon)

        print(f"  [{idx+1}/{len(df)}] {address[:40]}... → ({lat}, {lon})")
        time.sleep(0.5)  # Rate limiting

    df['lat'] = lats
    df['lon'] = lons

    success_rate = df['lat'].notna().sum() / len(df) * 100
    print(f"\n✅ Geocoding complete: {success_rate:.1f}% success rate")

    return df


# ==================== STEP 2: CLEAN DATA ====================

def clean_data(df):
    """
    Làm sạch dữ liệu:
    - Remove duplicates
    - Remove rows without lat/lon
    - Standardize columns
    """
    print("\n🧹 STEP 2: CLEANING DATA...")

    initial_count = len(df)

    # Remove null lat/lon
    df = df.dropna(subset=['lat', 'lon'])

    # Remove duplicates (based on lat/lon)
    df = df.drop_duplicates(subset=['lat', 'lon'])

    # Remove invalid coordinates
    df = df[(df['lat'].between(10.5, 11.0)) & (df['lon'].between(106.5, 107.0))]

    print(f"  Initial rows: {initial_count}")
    print(f"  After cleaning: {len(df)}")
    print(f"  Removed: {initial_count - len(df)} rows")

    return df


# ==================== STEP 3: SPATIAL FILTERING ====================

def filter_by_boundary(df, district_query):
    print(f"\n📍 STEP 3: SPATIAL FILTERING...")
    print(f"  Query: {district_query}")

    # 1. Tải ranh giới từ OpenStreetMap (giống boundary.py)
    print(f"  🌐 Downloading boundary from OpenStreetMap...")
    try:
        gdf_boundary = ox.geocode_to_gdf(district_query)
        print(f"  ✅ Successfully downloaded boundary")
    except Exception as e:
        print(f"  ❌ Error downloading from OSM: {e}")
        raise

    # 2. Tạo GeoDataFrame từ các điểm (giống testDistribution.py)
    print(f"  📊 Creating GeoDataFrame from {len(df)} points...")
    gdf_points = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.lon, df.lat),
        crs="EPSG:4326"
    )

    # 3. Spatial join: CHỈ GIỮ điểm TRONG ranh giới (giống testDistribution.py)
    print(f"  🔍 Filtering points within boundary...")
    gdf_inside = gpd.sjoin(
        gdf_points,
        gdf_boundary,
        how="inner",        # Chỉ giữ điểm nằm trong
        predicate="within"  # Predicate: điểm phải nằm HOÀN TOÀN trong vùng
    )

    # 4. Thống kê
    print(f"  📊 Results:")
    print(f"     Total points: {len(df)}")
    print(f"     ✅ Inside boundary: {len(gdf_inside)}")
    print(f"     ❌ Outside boundary (removed): {len(df) - len(gdf_inside)}")

    # 5. Convert về DataFrame thông thường (bỏ geometry column)
    df_filtered = pd.DataFrame(gdf_inside.drop(columns='geometry'))

    return df_filtered


# ==================== STEP 4: UPLOAD TO SUPABASE ====================

def upload_to_supabase(df, table_name='locations', skip_upload=False):
    """
    Upload dữ liệu đã xử lý lên Supabase

    Args:
        skip_upload: Nếu True, chỉ preview, không upload thật
    """
    print(f"\n☁️ STEP 4: UPLOADING TO SUPABASE (table: {table_name})...")

    if skip_upload:
        print("  ⚠️ SKIP_UPLOAD=True, chỉ preview, không upload thật")
        print(f"  Sẽ upload {len(df)} rows vào bảng '{table_name}'")
        return

    # Kiểm tra cấu hình
    if SUPABASE_URL == 'YOUR_SUPABASE_URL' or SUPABASE_KEY == 'YOUR_SUPABASE_KEY':
        print("  ⚠️ Chưa cấu hình Supabase! Bỏ qua bước upload.")
        print("  → Cấu hình SUPABASE_URL và SUPABASE_KEY trong file .env")
        return

    try:
        # Initialize Supabase client
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

        # Convert DataFrame to list of dicts
        records = df.to_dict('records')

        # Upload in batches (Supabase có limit ~1000 rows/request)
        batch_size = 100
        total_uploaded = 0

        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]

            try:
                response = supabase.table(table_name).insert(batch).execute()
                total_uploaded += len(batch)
                print(f"  Uploaded batch {i//batch_size + 1}: {len(batch)} rows")
            except Exception as e:
                print(f"  ❌ Error uploading batch: {e}")

        print(f"\n✅ Upload complete: {total_uploaded}/{len(records)} rows")

    except Exception as e:
        print(f"  ❌ Lỗi kết nối Supabase: {e}")


def upload_to_postgis(df, table_name='locations', skip_upload=False):
    print(f"\n🗺️ STEP 4: UPLOADING TO POSTGIS (table: {table_name})...")

    if skip_upload:
        print("  ⚠️ SKIP_UPLOAD=True, chỉ preview")
        print(f"  Sẽ upload {len(df)} rows với geometry column")
        return

    # Kiểm tra cấu hình
    if POSTGIS_PASSWORD == 'YOUR_PASSWORD':
        print("  ⚠️ Chưa cấu hình PostGIS! Bỏ qua upload.")
        print("  → Cấu hình POSTGIS_* trong file .env")
        return

    try:
        # Tạo connection string
        conn_string = f"postgresql://{POSTGIS_USER}:{POSTGIS_PASSWORD}@{POSTGIS_HOST}:{POSTGIS_PORT}/{POSTGIS_DB}"
        engine = create_engine(conn_string)

        print(f"  📡 Connecting to PostGIS: {POSTGIS_HOST}:{POSTGIS_PORT}/{POSTGIS_DB}")

        # Tạo GeoDataFrame
        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df.lon, df.lat),
            crs="EPSG:4326"
        )

        # Lưu vào PostGIS
        print(f"  💾 Uploading {len(gdf)} rows...")
        gdf.to_postgis(
            name=table_name,
            con=engine,
            if_exists='replace',  # 'replace' hoặc 'append'
            index=False
        )

        print(f"  ✅ Uploaded to table '{table_name}'")

        # Tạo spatial index (QUAN TRỌNG cho performance!)
        print(f"  🔍 Creating spatial index...")
        with engine.connect() as conn:
            sql = f"""
            CREATE INDEX IF NOT EXISTS {table_name}_geom_idx
            ON {table_name}
            USING GIST (geometry);
            """
            conn.execute(sql)
            conn.commit()

        print(f"\n✅ PostGIS upload complete!")
        print(f"   Table: {table_name}")
        print(f"   Rows: {len(gdf)}")
        print(f"   Spatial index: ✅")
        print(f"\n💡 Bây giờ có thể dùng spatial queries:")
        print(f"   - Tìm điểm gần nhất: ST_Distance()")
        print(f"   - Tìm trong bán kính: ST_DWithin()")
        print(f"   - Xem postgis_utils.py để biết thêm!")

    except Exception as e:
        print(f"  ❌ Lỗi kết nối PostGIS: {e}")
        import traceback
        traceback.print_exc()


# ==================== MAIN PIPELINE ====================

def run_pipeline(
    input_file,
    district_query,
    output_file=None,
    skip_upload=False,
    upload_to='supabase'
):
    """
    Chạy toàn bộ pipeline:
    1. Load Excel/CSV
    2. Geocode
    3. Clean
    4. Filter by boundary (OSMnx)
    5. Upload to database

    Args:
        input_file: File CSV/Excel đầu vào
        district_query: Query string cho OSMnx (BẮT BUỘC)
            Ví dụ: "Quận 1, Thành phố Hồ Chí Minh, Việt Nam"
        output_file: File CSV đầu ra (optional)
        skip_upload: Nếu True, bỏ qua upload
        upload_to: 'supabase', 'postgis', hoặc 'both'

    Examples:
        # Upload lên Supabase (default)
        run_pipeline('food.csv', 'Quận 1, Thành phố Hồ Chí Minh, Việt Nam')

        # Upload lên PostGIS (có spatial queries!)
        run_pipeline(
            'food.csv',
            'Quận 1, Thành phố Hồ Chí Minh, Việt Nam',
            upload_to='postgis'
        )

        # Upload cả 2
        run_pipeline(
            'food.csv',
            'Quận 1, Thành phố Hồ Chí Minh, Việt Nam',
            upload_to='both'
        )

        # Chỉ export CSV, không upload
        run_pipeline(
            'food.csv',
            'Quận 1, Thành phố Hồ Chí Minh, Việt Nam',
            output_file='output.csv',
            skip_upload=True
        )
    """
    print("="*60)
    print("🚀 STARTING DATA PIPELINE")
    print("="*60)

    # Step 0: Load data
    print(f"\n📂 STEP 0: LOADING DATA from {input_file}...")
    if input_file.endswith('.xlsx'):
        df = pd.read_excel(input_file)
    else:
        df = pd.read_csv(input_file, encoding='utf-8')

    print(f"  Loaded {len(df)} rows")

    # Step 1: Geocode
    df = geocode_batch(df, address_column='address')

    # Step 2: Clean
    df = clean_data(df)

    # Step 3: Spatial filter (CHỈ DÙNG OSMnx)
    df = filter_by_boundary(df, district_query=district_query)

    # Step 4: Upload to database
    if not skip_upload:
        if upload_to == 'supabase':
            upload_to_supabase(df, table_name=SUPABASE_TABLE)
        elif upload_to == 'postgis':
            upload_to_postgis(df, table_name=POSTGIS_TABLE)
        elif upload_to == 'both':
            upload_to_supabase(df, table_name=SUPABASE_TABLE)
            upload_to_postgis(df, table_name=POSTGIS_TABLE)
        else:
            print(f"\n⚠️ Invalid upload_to: {upload_to}. Skipping upload.")
    else:
        print(f"\n⚠️ skip_upload=True, bỏ qua upload")

    # Optional: Save to CSV
    if output_file:
        df.to_csv(output_file, encoding='utf-8', index=False)
        print(f"\n💾 Saved processed data to: {output_file}")

    print("\n" + "="*60)
    print("✅ PIPELINE COMPLETE!")
    print("="*60)

    return df


# ==================== EXAMPLE USAGE ====================

if __name__ == "__main__":

    result_df = run_pipeline(
        input_file="food.csv",
        district_query="Quận 1, Thành phố Hồ Chí Minh, Việt Nam",  # ← BẮT BUỘC
        output_file="quan1_filtered.csv",
        skip_upload=True  # True = không upload Supabase
    )

    print("\n" + "="*60)
    print("📊 FINAL RESULTS")
    print("="*60)
    print(f"Total locations: {len(result_df)}")
    print(f"Output file: quan1_filtered.csv")
    print(f"\nPreview:")
    print(result_df.head())

    # ========================================
    # VÍ DỤ: Xử lý nhiều quận
    # ========================================
    # districts = [
    #     "Quận 1, Thành phố Hồ Chí Minh, Việt Nam",
    #     "Quận 2, Thành phố Hồ Chí Minh, Việt Nam",
    #     "Quận 3, Thành phố Hồ Chí Minh, Việt Nam",
    # ]
    #
    # for i, district in enumerate(districts, 1):
    #     result = run_pipeline(
    #         input_file="food.csv",
    #         district_query=district,
    #         output_file=f"quan{i}_filtered.csv",
    #         skip_upload=True
    #     )
    #     print(f"\nQuận {i}: {len(result)} locations")
