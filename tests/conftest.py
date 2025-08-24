"""Test configuration and shared fixtures."""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from app import config, persist


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def test_config(temp_dir):
    """Create test configuration."""
    original_settings = config.settings
    
    # Create test settings
    test_settings = config.Settings(
        data_dir=temp_dir / "data",
        export_dir=temp_dir / "exports", 
        db_path=temp_dir / "test.sqlite",
        llm_enabled=False,
        log_level="DEBUG"
    )
    
    config.settings = test_settings
    
    # Create directories
    test_settings.data_dir.mkdir(exist_ok=True)
    test_settings.export_dir.mkdir(exist_ok=True)
    
    yield test_settings
    
    # Restore original settings
    config.settings = original_settings


@pytest.fixture
def test_db(test_config):
    """Create test database."""
    persist.db_manager.init_db()
    yield persist.db_manager
    persist.db_manager.close()


@pytest.fixture
def sample_restaurant():
    """Sample restaurant data."""
    return {
        "restaurant_id": "test_resto_001",
        "canonical_name": "Test Restaurant",
        "address_full": "123 Test Street, Test City, 560001",
        "pincode": "560001",
        "lat": 12.9716,
        "lon": 77.5946,
        "phone": "+91 80 1234 5678",
        "website": "https://test-restaurant.com",
        "cuisines": ["North Indian", "Chinese"],
        "hours": {
            "mon": "10:00-22:00",
            "tue": "10:00-22:00",
            "wed": "10:00-22:00",
            "thu": "10:00-22:00",
            "fri": "10:00-23:00",
            "sat": "10:00-23:00",
            "sun": "10:00-22:00"
        }
    }


@pytest.fixture
def sample_html_content():
    """Sample HTML content for testing parsing."""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Test Restaurant</title>
    </head>
    <body>
        <h1>Test Restaurant</h1>
        <div class="contact">
            <p>Address: 123 Test Street, Test City, 560001</p>
            <p>Phone: +91 80 1234 5678</p>
            <p>Cuisines: North Indian, Chinese</p>
        </div>
        <div class="hours">
            <p>Mon-Thu: 10:00 AM - 10:00 PM</p>
            <p>Fri-Sat: 10:00 AM - 11:00 PM</p>
            <p>Sun: 10:00 AM - 10:00 PM</p>
        </div>
    </body>
    </html>
    """


@pytest.fixture
def mock_llm_client():
    """Mock LLM client for testing."""
    with patch('app.extract.client.LLMClient') as mock_class:
        mock_instance = Mock()
        mock_instance.extract_structured.return_value = {
            "address_full": "123 Test Street, Test City",
            "pincode": "560001"
        }
        mock_class.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_http_response():
    """Mock HTTP response for testing."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.headers = {"content-type": "text/html"}
    mock_response.text = "<html><body>Test content</body></html>"
    mock_response.url = "https://test.com"
    return mock_response
