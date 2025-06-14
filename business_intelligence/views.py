from django.shortcuts import render
from django.http import HttpResponse
from django.db.models import Avg
from .models import OlapData
import csv
import numpy as np
from sklearn.linear_model import LinearRegression

def business_intelligence(request):
    return HttpResponse("Halaman Utama Business Intelligence")

def import_olap_csv(request):
    with open('/home/rafi/airflow/dags/OLAP_datarafi.csv', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            try:
                OlapData.objects.create(
                    year=int(float(row['year'])),
                    month=int(float(row['month'])),
                    price=float(row['price']) if row['price'] else 0,
                    area=float(row['area']) if row['area'] else 0,
                    bedrooms=float(row['bedrooms']) if row.get('bedrooms') else 0,
                    bathrooms=float(row['bathrooms']) if row.get('bathrooms') else 0,
                    stars=float(row['stars']) if row.get('stars') else None,
                    thumbs_up=float(row['thumbs_up']) if row.get('thumbs_up') else None,
                    thumbs_down=float(row['thumbs_down']) if row.get('thumbs_down') else None,
                    reply_count=float(row['reply_count']) if row.get('reply_count') else None,
                    best_score=float(row['best_score']) if row.get('best_score') else None,
                )
            except Exception as e:
                print(f"Error parsing row {row}: {e}")
    return HttpResponse("OLAP data imported successfully.")

def olap_analysis_view(request):
    data = (
        OlapData.objects
        .values('year', 'month')
        .annotate(
            avg_price=Avg('price'),
            avg_area=Avg('area'),
            avg_stars=Avg('stars'),
            avg_thumbs_up=Avg('thumbs_up'),
        )
        .order_by('year', 'month')
    )

    if not data:
        return HttpResponse("Tidak ada data untuk analisis OLAP.")

    X, y_price, y_stars = [], [], []
    labels = []

    for row in data:
        time_value = row['year'] + (row['month'] - 1) / 12
        X.append([time_value])
        y_price.append(row['avg_price'] or 0)
        y_stars.append(row['avg_stars'] or 0)
        labels.append(f"{row['year']}-{row['month']:02d}")

    X = np.array(X)
    y_price = np.array(y_price)
    y_stars = np.array(y_stars)

    if X.size == 0 or y_price.size == 0 or y_stars.size == 0:
        return HttpResponse("Data tidak cukup untuk analisis prediktif.")

    model_price = LinearRegression().fit(X, y_price)
    model_stars = LinearRegression().fit(X, y_stars)

    y_price_pred = model_price.predict(X)
    y_stars_pred = model_stars.predict(X)

    context = {
        'labels': labels,
        'price_actual': y_price.tolist(),
        'price_predicted': y_price_pred.tolist(),
        'stars_actual': y_stars.tolist(),
        'stars_predicted': y_stars_pred.tolist(),
    }

    return render(request, 'olap_analysis.html', context)