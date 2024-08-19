import pandas as pd
import requests
from io import StringIO
import json

# アメダス観測所一覧を読み込み
amedas_df = pd.read_csv('amedastable.csv', encoding='utf-8')

# 気象データのURL
wind_speed_url = 'https://www.data.jma.go.jp/stats/data/mdrr/wind_rct/alltable/mxwsp00_rct.csv'
temperature_url = 'https://www.data.jma.go.jp/stats/data/mdrr/tem_rct/alltable/mxtemsadext00_rct.csv'

# 気象データを読み込み、品質情報を文字列として読み込む
wind_speed_df = pd.read_csv(wind_speed_url, encoding='shift-jis', dtype={
    '最大値の品質情報': str, 
    '最大値観測時の風向の品質情報': str
})
temperature_df = pd.read_csv(temperature_url, encoding='shift-jis', dtype={
    '最高気温の品質情報': str
})

# 日付に基づく列名を動的に取得
max_wind_speed_col = [col for col in wind_speed_df.columns if '最大値(m/s)' in col][0]
max_wind_quality_col = [col for col in wind_speed_df.columns if '最大値の品質情報' in col][0]
wind_direction_col = [col for col in wind_speed_df.columns if '最大値観測時の風向' in col][0]
wind_direction_quality_col = [col for col in wind_speed_df.columns if '最大値観測時の風向の品質情報' in col][0]

max_temp_col = [col for col in temperature_df.columns if '最高気温(℃)' in col][0]
max_temp_quality_col = [col for col in temperature_df.columns if '最高気温の品質情報' in col][0]

# データ型を統一し、余分なスペースを削除
amedas_df['AmeCode'] = amedas_df['AmeCode'].astype(str).str.strip()
wind_speed_df['観測所番号'] = wind_speed_df['観測所番号'].astype(str).str.strip()
temperature_df['観測所番号'] = temperature_df['観測所番号'].astype(str).str.strip()

# 品質情報の列を文字列型に変換
wind_speed_df[max_wind_quality_col] = wind_speed_df[max_wind_quality_col].astype(str)
wind_speed_df[wind_direction_quality_col] = wind_speed_df[wind_direction_quality_col].astype(str)
temperature_df[max_temp_quality_col] = temperature_df[max_temp_quality_col].astype(str)

# 日時の列を結合するための関数
def concatenate_datetime(df, year_col, month_col, day_col, hour_col, minute_col):
    return pd.to_datetime(df[[year_col, month_col, day_col, hour_col, minute_col]].astype(str).agg('-'.join, axis=1), format='%Y-%m-%d-%H-%M')

# 各データフレームで日時の列を結合して「現在時刻」列を作成
wind_speed_df['現在時刻'] = concatenate_datetime(wind_speed_df, '現在時刻(年)', '現在時刻(月)', '現在時刻(日)', '現在時刻(時)', '現在時刻(分)')
temperature_df['現在時刻'] = concatenate_datetime(temperature_df, '現在時刻(年)', '現在時刻(月)', '現在時刻(日)', '現在時刻(時)', '現在時刻(分)')

# マージ前に最大風速と最高気温の現在時刻を取得
common_timestamp = wind_speed_df['現在時刻'].iloc[0]  # 最大風速と最高気温の時刻は同じと仮定

# 対応する降水量データのURLを生成
precipitation_url = f'https://www.data.jma.go.jp/stats/data/mdrr/pre_rct/alltable/pre1h00_{common_timestamp.strftime("%Y%m%d%H%M")}.csv'

# 降水量データを取得
response = requests.get(precipitation_url)
precipitation_df = pd.read_csv(StringIO(response.content.decode('shift-jis')), dtype={'現在値の品質情報': str})

# 観測所番号を文字列に変換
precipitation_df['観測所番号'] = precipitation_df['観測所番号'].astype(str).str.strip()

# 日時の列を結合して「現在時刻」列を作成
precipitation_df['現在時刻'] = concatenate_datetime(precipitation_df, '現在時刻(年)', '現在時刻(月)', '現在時刻(日)', '現在時刻(時)', '現在時刻(分)')

# 必要な列を選択し、アメダス観測所一覧に結合
precipitation_merge_cols = ['観測所番号', '現在時刻', '現在値(mm)', '現在値の品質情報']
wind_speed_merge_cols = ['観測所番号', '現在時刻', max_wind_speed_col, max_wind_quality_col, wind_direction_col, wind_direction_quality_col]
temperature_merge_cols = ['観測所番号', '現在時刻', max_temp_col, max_temp_quality_col]

# マージの実行
merged_df = amedas_df.merge(precipitation_df[precipitation_merge_cols], left_on='AmeCode', right_on='観測所番号', how='left')
merged_df = merged_df.merge(wind_speed_df[wind_speed_merge_cols], left_on='AmeCode', right_on='観測所番号', how='left', suffixes=('_precipitation', '_wind_speed'))
merged_df = merged_df.merge(temperature_df[temperature_merge_cols], left_on='AmeCode', right_on='観測所番号', how='left')

# 不要な観測所番号と2番目の現在時刻を削除
columns_to_drop = ['観測所番号_precipitation', '観測所番号_wind_speed', '観測所番号', '現在時刻_wind_speed', '現在時刻']
columns_to_drop = [col for col in columns_to_drop if col in merged_df.columns]
merged_df.drop(columns=columns_to_drop, inplace=True)

# 列名の変更
merged_df.rename(columns={
    '現在時刻_precipitation': 'time',
    '現在値(mm)': 'rain_1h_mm',
    '現在値の品質情報': 'rain_qual',
    max_wind_speed_col: 'wind_max_ms',
    max_wind_quality_col: 'wind_qual',
    wind_direction_col: 'wind_dir',
    wind_direction_quality_col: 'wind_dir_qual',
    max_temp_col: 'temp_max_c',
    max_temp_quality_col: 'temp_qual'
}, inplace=True)

# 結果を新しいCSVファイルに保存
merged_df.to_csv('amedas-data.csv', index=False, encoding='utf-8')

# CSVからGeoJSONを生成する
features = []
for _, row in merged_df.iterrows():
    # TimestampをISOフォーマットの文字列に変換
    row['time'] = row['time'].isoformat()
    
    # プロパティを生成。数値はそのまま、文字列のみstr()で変換。
    properties = {}
    for k, v in row.drop(['lat', 'lon']).items():
        if isinstance(v, (int, float)) and pd.notna(v):
            properties[k] = v
        else:
            properties[k] = str(v) if pd.notna(v) else None
    
    feature = {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [row['lon'], row['lat']]
        },
        "properties": properties
    }
    features.append(feature)

geojson = {
    "type": "FeatureCollection",
    "features": features
}

# GeoJSONファイルとして保存
with open('amedas-data.geojson', 'w', encoding='utf-8') as f:
    json.dump(geojson, f, ensure_ascii=False, indent=2)
