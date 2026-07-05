from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    database_url: str
    anthropic_api_key: str
    hn_scrape_concurrency: int = 15
    llm_concurrency: int = 5
    scrape_timeout_seconds: int = 10


settings = Settings()
