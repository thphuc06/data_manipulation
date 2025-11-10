import pandas as pd
import geopandas as gpd
import requests
import time
import os
import osmnx as ox
import re

def geocode_geoapify(query_text):
    url = "https://api.geoapify.com/v1/geocode/search"
    
    print(query_text)

    params = {
        'text': query_text,
        'apiKey': 'f1f9fa86b35b4087b305c6bb4d6250be',
        'limit': 1,
        'format': 'json'  
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data and data.get('results') and len(data['results']) > 0:
            best_result = data['results'][0]
            
            lon = best_result['lon']
            lat = best_result['lat']
            
            confidence = best_result['rank']['confidence']
            print(f"{lat} - {lon}")
            return pd.Series({'lat' :  lat, 'lon':lon})
        else:
            print("nah")
    except Exception as e:
        print(f"Geoapify error: {e}")

    return pd.Series({'lat':None, 'lon':None})


def geocode_batch(df, name_column='name', address_column='address', dist = '', city = ''):
    df['address'] = df['address'] + f', {dist}, {city}' 
    df[['lat', 'lon']] =  df.apply(lambda row: geocode_geoapify(f"{row[name_column]}, {row['address']}"), axis=1)

    prob = df['lat'].notna().sum() / len(df) * 100
    print(f"success: {prob}")
    return df


# ==================== STEP 2: CLEAN DATA ====================
def alter_count(cnt):
    # print(type(cnt))
    try:
        cnt = float(cnt)
        if cnt < 0.0:
            return -cnt
        else:
            return cnt
    except ValueError:
        #string -> need to parse
        pattern = r'\(?([\d,.]+)([A-Z\sa-z]*)\)?'
        cnt = cnt.replace('"', '').strip()
        # print(cnt)
        match = re.search(pattern, cnt)
        if match:
            try:
                num = match.group(1)
                num = float(num.replace(',','.'))
                char = match.group(2).strip()
                # print(char)
                if char and (char.upper() == 'N' or char.upper() == 'K'):
                    return num * 1000
                else:
                    return num
            except ValueError as e:
                return
        else:
            print('check again pattern')
    return 0

def alter_rating(rating):
    
    if isinstance(rating, (int, float)):
        if pd.isna(rating): 
            return 0.0     
        return float(rating)

    if isinstance(rating, str):
        try:
            clean_str = rating.replace(',', '.').replace('"', '').strip()
            
            if not clean_str:
                return 0.0

            return float(clean_str)
        
        except ValueError:
            return 0.0
        
    return 0.0

def clean_data(df):
    initial_count = len(df)

    # 1. Remove null lat/lon (geocode thất bại)
    df = df.dropna(subset=['lat', 'lon'])
    print(f"  Removed {initial_count - len(df)} rows (Geocode failed)")

    # 3. Remove duplicates (based on lat/lon)
    df = df.drop_duplicates(subset=['lat', 'lon'])
    print(f"  Removed {initial_count - len(df)} rows (Duplicates)")

    df.fillna(value={'comment': 'không có đánh giá'}, inplace=True)    
    df['comment'] = df['comment'].str.strip(' "')
    df['type'] = df['type'].str.replace('· ', '').str.strip()

    df['count'] = df.apply(lambda row: alter_count(row['count']), axis = 1)

    df['rating'] = df.apply(lambda row: alter_rating(row['rating']), axis = 1)

    # df.drop('category', axis = 1, inplace = True)

    print(f"\n  Initial rows: {initial_count}")
    print(f"  After cleaning: {len(df)}")
    print(f"  Total removed: {initial_count - len(df)} rows")

    return df


# ==================== STEP 3: SPATIAL FILTERING ====================
def filter_by_boundary(df, district_query):
    print(f" Query: {district_query}")
    try:
        gdf_boundary = ox.geocode_to_gdf(district_query)
        print(f"   Successfully downloaded boundary")
    except Exception as e:
        print(f"   Error downloading from OSM: {e}")
        raise
        
    print(f" Creating GeoDataFrame from {len(df)} points...")
    gdf_points = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.lon, df.lat),
        crs="epsg:4326"
    )

    # Lưu tên các cột gốc (không bao gồm geometry)
    original_columns = [col for col in gdf_points.columns if col != 'geometry']
    gdf_boundary = gpd.GeoDataFrame(gdf_boundary.geometry)
    print(f"   Filtering points within boundary...")
    gdf_inside = gpd.sjoin(
        gdf_points,
        gdf_boundary,
        how="inner",
        predicate="within"
    )

    print(f"   📊 Results:")
    print(f"     Total points (pre-filter): {len(gdf_points)}")
    print(f"     ✅ Inside boundary: {len(gdf_inside)}")
    print(f"     ❌ Outside boundary (removed): {len(gdf_points) - len(gdf_inside)}")

    # Kết quả trả về là một GeoDataFrame
    return gdf_inside[df.columns.to_list() + ['geometry']] 

# ==================== MAIN PIPELINE ====================

def run_pipeline(
    input_file,
    district,
    city,
    output_file,
    name_column='name',
    address_column='address'
):
    """
    Chạy toàn bộ pipeline:
    1. Load Excel/CSV
    2. Geocode (Geoapify)
    3. Clean
    4. Filter by boundary (OSMnx)
    5. Save to new CSV
    """
    print("="*60)
    print("🚀 STARTING DATA PIPELINE (Geoapify -> CSV)")
    print("="*60)

    # Step 0: Load data
    print(f"\n📂 STEP 0: LOADING DATA from {input_file}...")
    if input_file.endswith('.xlsx'):
        df = pd.read_excel(input_file)
    else:
        df = pd.read_csv(input_file, encoding='utf-8')

    print(f"  Loaded {len(df)} rows")

    # Step 1: Geocode
    df = geocode_batch(df, name_column=name_column, address_column=address_column, dist = district, city = city)

    # Step 2: Clean
    df = clean_data(df)
    
    # Kiểm tra nếu không còn dữ liệu sau khi clean
    if df.empty:
        print("\nNo data left after cleaning. Exiting.")
        return df

    # Step 3: Spatial filter
    df = filter_by_boundary(df, district_query=f'{district}, Thành phố Hồ Chí Minh, Việt Nam')

    # Step 4: Save to CSV
    if output_file:
        df.to_csv(output_file, encoding='utf-8', index=False)
        print(f"\nSaved processed data to: {output_file}")
    else:
        print("\nNo output_file specified. Skipping save.")

    print("\n" + "="*60)
    print("✅ PIPELINE COMPLETE!")
    print("="*60)

    return df


# ==================== EXAMPLE USAGE ====================

if __name__ == "__main__":

    # Đảm bảo bạn có file .env với GEOAPIFY_API_KEY

    result_gdf = run_pipeline(
        input_file="full_q1.csv",      # File input của bạn
        name_column="name",         # Tên cột chứa tên địa điểm
        address_column="address",   # Tên cột chứa địa chỉ
        district="Quận 1",
        city = "Thành phố Hồ Chí Minh",
        output_file="quan1_filtered_full.csv"  # File output
    )

    print("\n" + "="*60)
    print("📊 FINAL RESULTS PREVIEW")
    print("="*60)
    if not result_gdf.empty:
        print(f"Total locations: {len(result_gdf)}")
        print(f"Output file: quan1_filtered_geoapify.csv")
        print(f"\nPreview:")
        print(result_gdf.head())
    else:
        print("No locations found matching all criteria.")