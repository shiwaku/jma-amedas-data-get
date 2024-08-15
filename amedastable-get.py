import requests
import csv
import json

# JSONファイルのURL
amedas_url = "https://www.jma.go.jp/bosai/amedas/const/amedastable.json"

# URLからJSONデータを取得
amedas_data = requests.get(amedas_url).json()

# 度分を10進数の度に変換する関数
def convert_to_decimal(degrees, minutes):
    return degrees + minutes / 60

# amedastable.jsonを10進数の度に変換してCSVに変換
with open("amedastable.csv", "w", newline="", encoding="utf-8") as amedas_csv:
    writer = csv.writer(amedas_csv)
    writer.writerow(["AmeCode", "type", "elems", "lat", "lon", "alt", "kjName", "knName", "enName"])

    # GeoJSON用のリストを初期化
    features = []

    for code, details in amedas_data.items():
        lat = convert_to_decimal(details["lat"][0], details["lat"][1])
        lon = convert_to_decimal(details["lon"][0], details["lon"][1])
        writer.writerow([code, details["type"], details["elems"], lat, lon, details["alt"], details["kjName"], details["knName"], details["enName"]])

        # GeoJSONのフィーチャーを作成
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat]
            },
            "properties": {
                "AmeCode": code,
                "type": details["type"],
                "elems": details["elems"],
                "alt": details["alt"],
                "kjName": details["kjName"],
                "knName": details["knName"],
                "enName": details["enName"]
            }
        }
        features.append(feature)

    # GeoJSONデータを作成
    geojson_data = {
        "type": "FeatureCollection",
        "features": features
    }

    # GeoJSONファイルとして保存
    with open("amedastable.geojson", "w", encoding="utf-8") as geojson_file:
        json.dump(geojson_data, geojson_file, ensure_ascii=False, indent=2)
