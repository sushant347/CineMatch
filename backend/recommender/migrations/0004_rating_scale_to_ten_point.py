import django.core.validators
from django.db import migrations, models
from django.db.models import F


def migrate_custom_ratings_to_ten_scale(apps, schema_editor):
    Rating = apps.get_model("recommender", "Rating")
    UserAccount = apps.get_model("recommender", "UserAccount")

    custom_user_ids = list(UserAccount.objects.values_list("user_id", flat=True))
    if not custom_user_ids:
        return

    # Existing custom ratings were previously stored on a 0.5-5.0 scale.
    Rating.objects.filter(user_id__in=custom_user_ids, rating__lte=5.0).update(
        rating=F("rating") * 2.0
    )


def migrate_custom_ratings_back_to_five_scale(apps, schema_editor):
    Rating = apps.get_model("recommender", "Rating")
    UserAccount = apps.get_model("recommender", "UserAccount")

    custom_user_ids = list(UserAccount.objects.values_list("user_id", flat=True))
    if not custom_user_ids:
        return

    Rating.objects.filter(user_id__in=custom_user_ids, rating__gt=5.0).update(
        rating=F("rating") / 2.0
    )


class Migration(migrations.Migration):

    dependencies = [
        ("recommender", "0003_useraccount_review_watchedmovie"),
    ]

    operations = [
        migrations.AlterField(
            model_name="rating",
            name="rating",
            field=models.FloatField(
                validators=[
                    django.core.validators.MinValueValidator(0.5),
                    django.core.validators.MaxValueValidator(10.0),
                ]
            ),
        ),
        migrations.RunPython(
            migrate_custom_ratings_to_ten_scale,
            reverse_code=migrate_custom_ratings_back_to_five_scale,
        ),
    ]
