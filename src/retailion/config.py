from dataclasses import dataclass
import os

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        values = {key: os.getenv(key) for key in (
            "DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"
        )}
        missing = [key for key, value in values.items() if not value]
        if missing:
            raise ValueError(f"Missing environment variables: {', '.join(missing)}")
        return cls(
            db_host=values["DB_HOST"],
            db_port=int(values["DB_PORT"]),
            db_name=values["DB_NAME"],
            db_user=values["DB_USER"],
            db_password=values["DB_PASSWORD"],
        )

