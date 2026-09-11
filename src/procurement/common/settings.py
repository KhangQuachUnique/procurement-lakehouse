import os

from dotenv import load_dotenv

load_dotenv()


class Settings:
    APP_ENV = os.getenv("APP_ENV", "dev")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    MUASAMCONG_BASE_URL = os.getenv(
        "MUASAMCONG_BASE_URL",
        "https://muasamcong.mpi.gov.vn",
    )
    MUASAMCONG_TIMEOUT_SECONDS = int(
        os.getenv("MUASAMCONG_TIMEOUT_SECONDS", "30")
    )

    OBJECT_STORAGE_ENDPOINT = os.getenv(
        "OBJECT_STORAGE_ENDPOINT",
        "http://localhost:8333",
    )
    OBJECT_STORAGE_ACCESS_KEY = os.getenv("OBJECT_STORAGE_ACCESS_KEY")
    OBJECT_STORAGE_SECRET_KEY = os.getenv("OBJECT_STORAGE_SECRET_KEY")
    OBJECT_STORAGE_BUCKET = os.getenv(
        "OBJECT_STORAGE_BUCKET",
        "procurement-lakehouse",
    )


settings = Settings()