"""Configuration management using Pydantic Settings."""

from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration settings."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # LLM Configuration
    llm_enabled: bool = Field(default=True, description="Enable LLM-based extraction")
    ollama_base_url: str = Field(default="http://localhost:11434", description="Ollama API base URL")
    ollama_model: str = Field(default="qwen2:1.5b-instruct", description="Ollama model name")
    
    # HTTP Client Configuration
    http_cache_dir: Path = Field(default=Path("data/cache/http"), description="HTTP cache directory")
    user_agent: str = Field(default="bharat-resto-mvp/0.1 (+https://example.local)", description="User agent string")
    rate_limit_per_host: float = Field(default=0.5, description="Requests per second per host")
    timeout_seconds: int = Field(default=20, description="HTTP timeout in seconds")
    
    # Geocoding Configuration
    geocode_cache: Path = Field(default=Path("data/cache/geocode/cache.sqlite"), description="Geocoding cache path")
    
    # Database Configuration
    db_path: Path = Field(default=Path("db/restaurants.db"), description="SQLite database path")
    
    # Data Directories
    data_dir: Path = Field(default=Path("data"), description="Base data directory")
    raw_data_dir: Path = Field(default=Path("data/raw"), description="Raw data directory")
    parsed_data_dir: Path = Field(default=Path("data/parsed"), description="Parsed data directory")
    
    # Export Configuration
    export_dir: Path = Field(default=Path("exports"), description="Export directory")
    
    # Logging Configuration
    log_level: str = Field(default="INFO", description="Log level")
    log_format: str = Field(default="json", description="Log format (json or console)")
    
    def __init__(self, **kwargs):
        """Initialize settings and create directories."""
        super().__init__(**kwargs)
        self._create_directories()
    
    def _create_directories(self) -> None:
        """Create necessary directories if they don't exist."""
        directories = [
            self.data_dir,
            self.raw_data_dir, 
            self.parsed_data_dir,
            self.http_cache_dir,
            self.geocode_cache.parent,
            self.db_path.parent,
            self.export_dir,
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)


# Global settings instance
settings = Settings()
