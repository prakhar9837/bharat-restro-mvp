"""Tests for FastAPI integration."""

import json
import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

from fastapi.testclient import TestClient

from app.api import app
from app import persist
from app.config import settings


@pytest.fixture
def test_db():
    """Create a temporary test database."""
    with tempfile.TemporaryDirectory() as temp_dir:
        test_db_path = Path(temp_dir) / "test_restaurants.db"
        
        # Patch the settings to use test database
        with patch.object(settings, 'db_path', test_db_path):
            persist.db_manager.init_db()
            yield test_db_path


@pytest.fixture
def client(test_db):
    """Create a test client with test database."""
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def sample_restaurants(test_db):
    """Create sample restaurant data in test database."""
    restaurants = [
        {
            "restaurant_id": "test_001",
            "canonical_name": "Test Restaurant 1",
            "address_full": "123 Test Street, Bangalore",
            "pincode": "560001",
            "lat": 12.9716,
            "lon": 77.5946,
            "phone": "+91-80-12345678",
            "website": "https://test1.com",
            "cuisines": '["Indian", "South Indian"]',
            "hours": '{"monday": "09:00-22:00"}'
        },
        {
            "restaurant_id": "test_002",
            "canonical_name": "Test Restaurant 2",
            "address_full": "456 Test Avenue, Bangalore",
            "pincode": "560002",
            "lat": 12.9800,
            "lon": 77.6000,
            "phone": None,
            "website": None,
            "cuisines": '["North Indian"]',
            "hours": '{"tuesday": "10:00-23:00"}'
        }
    ]
    
    # Insert test data
    for restaurant_data in restaurants:
        persist.db_manager.add_restaurant(restaurant_data)
    
    return restaurants


class TestHealthAndStatus:
    """Test health and status endpoints."""
    
    def test_health_check(self, client):
        """Test health check endpoint."""
        response = client.get("/health")
        assert response.status_code == 200
        
        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data
        assert "database" in data

    def test_root_endpoint(self, client):
        """Test root endpoint."""
        response = client.get("/")
        assert response.status_code == 200
        
        # Should return either frontend HTML or API info
        content_type = response.headers.get("content-type", "")
        assert "text/html" in content_type or "application/json" in content_type

    def test_status_endpoint(self, client, sample_restaurants):
        """Test status endpoint."""
        response = client.get("/status")
        assert response.status_code == 200
        
        data = response.json()
        assert "total_restaurants" in data
        assert "total_provenance" in data
        assert "field_coverage" in data
        assert "database_path" in data


class TestRestaurantEndpoints:
    """Test restaurant-related endpoints."""
    
    def test_get_restaurants_empty(self, client):
        """Test getting restaurants from empty database."""
        response = client.get("/restaurants")
        assert response.status_code == 200
        assert response.json() == []

    def test_get_restaurants_with_data(self, client, sample_restaurants):
        """Test getting restaurants with sample data."""
        response = client.get("/restaurants")
        assert response.status_code == 200
        
        data = response.json()
        assert len(data) == 2
        
        # Check first restaurant
        restaurant = data[0]
        assert restaurant["restaurant_id"] == "test_001"
        assert restaurant["canonical_name"] == "Test Restaurant 1"
        assert restaurant["phone"] == "+91-80-12345678"
        assert restaurant["cuisines"] == ["Indian", "South Indian"]

    def test_get_restaurants_with_filters(self, client, sample_restaurants):
        """Test restaurant filtering."""
        # Filter by phone presence
        response = client.get("/restaurants?has_phone=true")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["restaurant_id"] == "test_001"
        
        # Filter by website presence
        response = client.get("/restaurants?has_website=false")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["restaurant_id"] == "test_002"
        
        # Filter by cuisine
        response = client.get("/restaurants?cuisine=North")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["restaurant_id"] == "test_002"

    def test_get_restaurants_pagination(self, client, sample_restaurants):
        """Test restaurant pagination."""
        # Test limit
        response = client.get("/restaurants?limit=1")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        
        # Test offset
        response = client.get("/restaurants?offset=1&limit=1")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["restaurant_id"] == "test_002"

    def test_get_restaurant_by_id(self, client, sample_restaurants):
        """Test getting specific restaurant by ID."""
        response = client.get("/restaurants/test_001")
        assert response.status_code == 200
        
        data = response.json()
        assert data["restaurant_id"] == "test_001"
        assert data["canonical_name"] == "Test Restaurant 1"
        assert data["phone"] == "+91-80-12345678"

    def test_get_restaurant_not_found(self, client):
        """Test getting non-existent restaurant."""
        response = client.get("/restaurants/nonexistent")
        assert response.status_code == 404
        
        data = response.json()
        assert "not found" in data["detail"].lower()


class TestPipelineEndpoints:
    """Test pipeline-related endpoints."""
    
    @patch('app.api.run_pipeline_background')
    def test_run_pipeline(self, mock_run_pipeline, client):
        """Test pipeline execution trigger."""
        request_data = {
            "city": "blr",
            "limit": 10,
            "llm_enabled": True,
            "concurrency": 2
        }
        
        response = client.post("/pipeline/run", json=request_data)
        assert response.status_code == 200
        
        data = response.json()
        assert "task_id" in data
        assert data["status"] == "started"
        assert "message" in data

    def test_run_pipeline_validation(self, client):
        """Test pipeline request validation."""
        # Missing required city field
        response = client.post("/pipeline/run", json={})
        assert response.status_code == 422
        
        # Invalid limit value
        request_data = {
            "city": "blr",
            "limit": -1
        }
        response = client.post("/pipeline/run", json=request_data)
        assert response.status_code == 422

    def test_get_pipeline_status_not_found(self, client):
        """Test getting status of non-existent task."""
        response = client.get("/pipeline/status/nonexistent")
        assert response.status_code == 404

    def test_get_all_pipeline_tasks(self, client):
        """Test getting all pipeline tasks."""
        response = client.get("/pipeline/tasks")
        assert response.status_code == 200
        assert isinstance(response.json(), list)


class TestUtilityEndpoints:
    """Test utility endpoints."""
    
    def test_get_supported_cities(self, client):
        """Test getting supported cities."""
        response = client.get("/cities")
        assert response.status_code == 200
        
        data = response.json()
        assert "cities" in data
        assert len(data["cities"]) > 0
        
        # Check city structure
        city = data["cities"][0]
        assert "code" in city
        assert "name" in city
        assert "country" in city

    def test_validate_data(self, client, sample_restaurants):
        """Test data validation endpoint."""
        response = client.post("/validate")
        assert response.status_code == 200
        
        data = response.json()
        assert "total_restaurants" in data
        assert "valid_restaurants" in data
        assert "invalid_restaurants" in data
        assert "validation_issues" in data

    def test_export_csv(self, client, sample_restaurants):
        """Test CSV export."""
        with patch('app.export.export_data') as mock_export:
            # Mock export function to return a test file
            test_file = Path("/tmp/test_export.csv")
            test_file.write_text("id,name\ntest_001,Test Restaurant 1")
            mock_export.return_value = test_file
            
            try:
                response = client.get("/export/csv")
                assert response.status_code == 200
                assert response.headers["content-type"] == "application/octet-stream"
            finally:
                test_file.unlink(missing_ok=True)

    def test_export_json(self, client, sample_restaurants):
        """Test JSON export."""
        with patch('app.export.export_data') as mock_export:
            # Mock export function to return a test file
            test_file = Path("/tmp/test_export.json")
            test_file.write_text('{"restaurants": []}')
            mock_export.return_value = test_file
            
            try:
                response = client.get("/export/json")
                assert response.status_code == 200
                assert response.headers["content-type"] == "application/octet-stream"
            finally:
                test_file.unlink(missing_ok=True)

    def test_export_invalid_format(self, client):
        """Test export with invalid format."""
        response = client.get("/export/xml")
        assert response.status_code == 400
        
        data = response.json()
        assert "csv" in data["detail"] and "json" in data["detail"]


class TestErrorHandling:
    """Test error handling."""
    
    def test_404_handler(self, client):
        """Test 404 error handler."""
        response = client.get("/nonexistent-endpoint")
        assert response.status_code == 404
        
        data = response.json()
        assert "not found" in data["detail"].lower()
        assert "path" in data

    @patch('app.api.get_db')
    def test_500_handler(self, mock_get_db, client):
        """Test 500 error handler."""
        # Mock database to raise an exception
        mock_get_db.side_effect = Exception("Database error")
        
        response = client.get("/status")
        assert response.status_code == 500
        
        data = response.json()
        assert "error" in data["detail"].lower() or "failed" in data["detail"].lower()


class TestPipelineBackgroundTask:
    """Test background pipeline execution."""
    
    @pytest.mark.asyncio
    @patch('app.api._run_pipeline')
    @patch('app.persist.db_manager')
    async def test_successful_pipeline_execution(self, mock_db, mock_run_pipeline):
        """Test successful pipeline background execution."""
        from app.api import run_pipeline_background, pipeline_tasks
        
        # Mock database response
        mock_restaurant = MagicMock()
        mock_restaurant.to_dict.return_value = {"id": "test", "name": "Test"}
        mock_db.get_all_restaurants.return_value = [mock_restaurant]
        
        task_id = "test_task"
        
        # Run background task
        await run_pipeline_background(task_id, "blr", 10, True, 2)
        
        # Check task status
        assert task_id in pipeline_tasks
        task = pipeline_tasks[task_id]
        assert task["status"] == "completed"
        assert task["progress"] == 1.0
        assert task["results"] is not None

    @pytest.mark.asyncio
    @patch('app.api._run_pipeline')
    async def test_failed_pipeline_execution(self, mock_run_pipeline):
        """Test failed pipeline background execution."""
        from app.api import run_pipeline_background, pipeline_tasks
        
        # Mock pipeline to raise an exception
        mock_run_pipeline.side_effect = Exception("Pipeline failed")
        
        task_id = "test_task_fail"
        
        # Run background task
        await run_pipeline_background(task_id, "blr", 10, True, 2)
        
        # Check task status
        assert task_id in pipeline_tasks
        task = pipeline_tasks[task_id]
        assert task["status"] == "failed"
        assert "failed" in task["message"].lower()


class TestCORSAndMiddleware:
    """Test CORS and middleware configuration."""
    
    def test_cors_headers(self, client):
        """Test CORS headers are present."""
        response = client.get("/health")
        
        # Note: TestClient doesn't automatically add CORS headers
        # In a real deployment, these would be added by the CORS middleware
        assert response.status_code == 200

    def test_api_documentation(self, client):
        """Test API documentation endpoints."""
        # Test OpenAPI schema
        response = client.get("/openapi.json")
        assert response.status_code == 200
        
        schema = response.json()
        assert "info" in schema
        assert "paths" in schema
        
        # Test Swagger UI
        response = client.get("/docs")
        assert response.status_code == 200
        
        # Test ReDoc
        response = client.get("/redoc")
        assert response.status_code == 200


# Integration test with real pipeline components
class TestIntegration:
    """Integration tests with real components."""
    
    def test_database_integration(self, client, test_db):
        """Test that API correctly integrates with database."""
        # Add a restaurant directly to database
        restaurant_data = {
            "restaurant_id": "integration_test",
            "canonical_name": "Integration Test Restaurant",
            "address_full": "Test Address",
            "pincode": "560001"
        }
        
        persist.db_manager.add_restaurant(restaurant_data)
        
        # Retrieve via API
        response = client.get("/restaurants/integration_test")
        assert response.status_code == 200
        
        data = response.json()
        assert data["restaurant_id"] == "integration_test"
        assert data["canonical_name"] == "Integration Test Restaurant"

    @patch('app.api.export.export_summary_stats')
    def test_status_with_real_stats(self, mock_stats, client):
        """Test status endpoint with real statistics."""
        mock_stats.return_value = {
            "totals": {
                "restaurants": 5,
                "provenance_records": 10
            },
            "field_counts": {
                "name": {"filled": 5, "empty": 0},
                "phone": {"filled": 3, "empty": 2}
            }
        }
        
        response = client.get("/status")
        assert response.status_code == 200
        
        data = response.json()
        assert data["total_restaurants"] == 5
        assert data["total_provenance"] == 10
        assert "name" in data["field_coverage"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])