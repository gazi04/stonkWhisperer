from celery import Celery
from core.config_loader import settings

app = Celery("data_task",
             broker=settings.celery_broker_url,
             backend=settings.celery_result_backend,
             include=["tasks.extraction", "tasks.loading"])

# Optional configuration, see the application user guide.
app.conf.update(
    result_expires=3600,
)

if __name__ == "__main__":
    app.start()

