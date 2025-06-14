import csv
from django.core.management.base import BaseCommand
from business_intelligence.models import OlapData

class Command(BaseCommand):
    help = 'Load OLAP data from a CSV file'

    def add_arguments(self, parser):
        parser.add_argument('csv_file', type=str, help='Path to the CSV file to import')

    def handle(self, *args, **kwargs):
        csv_file = kwargs['csv_file']

        try:
            with open(csv_file, newline='', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                count = 0
                for row in reader:
                    try:
                        OlapData.objects.create(
                            year=int(row['year']),
                            month=int(row['month']),
                            price=float(row['price']),
                            area=float(row['area']),
                            bedrooms=float(row['bedrooms']),
                            bathrooms=float(row['bathrooms']),
                            stars=float(row['stars']) if row.get('stars') else None,
                            thumbs_up=float(row['thumbs_up']) if row.get('thumbs_up') else None,
                            thumbs_down=float(row['thumbs_down']) if row.get('thumbs_down') else None,
                            reply_count=float(row['reply_count']) if row.get('reply_count') else None,
                            best_score=float(row['best_score']) if row.get('best_score') else None,
                        )
                        count += 1
                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f"Failed to import row {row}: {e}"))

            self.stdout.write(self.style.SUCCESS(f'Successfully imported {count} rows from {csv_file}'))

        except FileNotFoundError:
            self.stdout.write(self.style.ERROR(f'File "{csv_file}" not found'))
