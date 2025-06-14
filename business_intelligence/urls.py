from django.urls import path
from . import views

urlpatterns = [
    path('', views.business_intelligence, name='business_intelligence'),
    path('import/', views.import_olap_csv, name='import_olap_csv'),
    path('analysis/', views.olap_analysis_view, name='olap_analysis'),
]
