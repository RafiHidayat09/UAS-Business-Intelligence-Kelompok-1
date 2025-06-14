from django.db import models

class OlapData(models.Model):
    year = models.IntegerField()
    month = models.IntegerField()
    price = models.FloatField(default=0)
    area = models.FloatField(default=0)
    bedrooms = models.FloatField(default=0)
    bathrooms = models.FloatField(default=0)
    stars = models.FloatField(null=True, blank=True)
    thumbs_up = models.FloatField(null=True, blank=True)
    thumbs_down = models.FloatField(null=True, blank=True)
    reply_count = models.FloatField(null=True, blank=True)
    best_score = models.FloatField(null=True, blank=True)

    def __str__(self):
        return f"{self.year}-{self.month}"
