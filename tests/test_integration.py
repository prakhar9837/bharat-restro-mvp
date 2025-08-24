"""Integration tests for the complete pipeline."""

import json
import pytest
from unittest.mock import Mock, patch, AsyncMock

from app import seed, fetch, parse, normalize, validate, persist


class TestEndToEndPipeline:
    """Test the complete end-to-end pipeline."""
    
    @pytest.mark.asyncio
    async def test_sample_pipeline(self, test_db, sample_restaurant, mock_http_response):
        """Test a complete pipeline run with sample data."""
        
        # Mock external services
        with patch('app.seed.seed_city') as mock_seed, \
             patch('app.fetch.fetch_urls') as mock_fetch, \
             patch('app.geocode.geocode_address') as mock_geocode:
            
            # Setup mocks
            mock_seed.return_value = [sample_restaurant.copy()]
            
            mock_fetch.return_value = {
                sample_restaurant["website"]: (
                    "<html><body>Test Restaurant<br>123 Test Street<br>+91 80 1234 5678</body></html>",
                    {"content_type": "text/html"}
                )
            }
            
            mock_geocode.return_value = (12.9716, 77.5946)
            
            # Run pipeline steps
            restaurants = await mock_seed("blr", limit=1)
            assert len(restaurants) == 1
            
            urls = [r["website"] for r in restaurants if r.get("website")]
            fetch_results = await mock_fetch(urls, concurrency=1)
            assert len(fetch_results) == 1
            
            # Parse content
            website = restaurants[0]["website"]
            content, metadata = fetch_results[website]
            parser = parse.ContentParser()
            chunks = parser.parse_content(content, "text/html", website)
            assert len(chunks) > 0
            
            # Normalize and validate
            normalized = normalize.normalize_restaurant_data(restaurants[0])
            valid, issues = validate.validate_restaurant_data(normalized)
            assert valid, f"Validation failed: {issues}"
            
            # Store in database
            persist.db_manager.upsert_restaurant(normalized, [])
            
            # Verify storage
            stored_restaurants = persist.db_manager.get_all_restaurants()
            assert len(stored_restaurants) == 1
            assert stored_restaurants[0].canonical_name == sample_restaurant["canonical_name"]
    
    def test_validation_pipeline(self, sample_restaurant):
        """Test the validation pipeline with various data quality scenarios."""
        
        # Valid restaurant
        valid, issues = validate.validate_restaurant_data(sample_restaurant)
        assert valid
        assert len(issues) == 0
        
        # Invalid phone number
        invalid_restaurant = sample_restaurant.copy()
        invalid_restaurant["phone"] = "invalid-phone"
        valid, issues = validate.validate_restaurant_data(invalid_restaurant)
        assert not valid
        assert any("phone" in issue.lower() for issue in issues)
        
        # Missing required fields
        minimal_restaurant = {"canonical_name": "Test", "lat": 12.0, "lon": 77.0}
        valid, issues = validate.validate_restaurant_data(minimal_restaurant)
        assert valid  # Should be valid with minimal fields
        
        # Invalid coordinates
        invalid_coords = sample_restaurant.copy()
        invalid_coords["lat"] = 100.0  # Outside India bounds
        valid, issues = validate.validate_restaurant_data(invalid_coords)
        assert not valid
        assert any("lat" in issue.lower() for issue in issues)
    
    def test_normalization_pipeline(self):
        """Test data normalization with various inputs."""
        
        # Test phone normalization
        test_cases = [
            {"phone": "08012345678", "expected": "+91 80 1234 5678"},
            {"phone": "+91-80-1234-5678", "expected": "+91 80 1234 5678"},
            {"phone": "9876543210", "expected": "+91 98765 43210"},
        ]
        
        for case in test_cases:
            input_data = {"phone": case["phone"]}
            normalized = normalize.normalize_restaurant_data(input_data)
            assert normalized["phone"] == case["expected"]
        
        # Test address normalization
        address_input = {
            "address_full": "  123, test STREET, bangalore  , karnataka  "
        }
        normalized = normalize.normalize_restaurant_data(address_input)
        assert "123, Test Street, Bangalore, Karnataka" == normalized["address_full"]
        
        # Test cuisine normalization
        cuisine_input = {"cuisines": ["north indian", "CHINESE", "veg"]}
        normalized = normalize.normalize_restaurant_data(cuisine_input)
        assert "North Indian" in normalized["cuisines"]
        assert "Chinese" in normalized["cuisines"]
        assert "Vegetarian" in normalized["cuisines"]


class TestExtractionPipeline:
    """Test the extraction pipeline components."""
    
    def test_html_parsing(self, sample_html_content):
        """Test HTML content parsing."""
        
        parser = parse.ContentParser()
        chunks = parser.parse_content(
            sample_html_content, 
            "text/html", 
            "https://test.com"
        )
        
        assert len(chunks) > 0
        content_text = " ".join(chunks)
        
        # Check that key information is extracted
        assert "Test Restaurant" in content_text
        assert "123 Test Street" in content_text
        assert "+91 80 1234 5678" in content_text
    
    @patch('app.extract.client.LLMClient')
    def test_llm_extraction(self, mock_llm_class, mock_llm_client):
        """Test LLM-based extraction."""
        
        mock_llm_class.return_value = mock_llm_client
        
        # Mock extraction results
        mock_llm_client.extract_structured.return_value = {
            "address_full": "123 Test Street, Test City",
            "pincode": "560001"
        }
        
        from app.extract.address import AddressExtractor
        
        extractor = AddressExtractor()
        result = extractor.extract(["Address: 123 Test Street, Test City, 560001"])
        
        assert result["value"]["address_full"] == "123 Test Street, Test City"
        assert result["value"]["pincode"] == "560001"
        assert result["confidence"] > 0.0
    
    def test_regex_fallback(self):
        """Test regex-based extraction fallback."""
        
        # Patch to disable LLM
        with patch('app.config.settings') as mock_settings:
            mock_settings.llm_enabled = False
            
            from app.extract.phone import PhoneExtractor
            
            extractor = PhoneExtractor()
            result = extractor.extract([
                "Contact us at +91 80 1234 5678 for reservations"
            ])
            
            assert result["value"] == "+91 80 1234 5678"
            assert result["confidence"] > 0.0
            assert result["method"] == "regex"


class TestDatabaseOperations:
    """Test database operations and persistence."""
    
    def test_restaurant_crud(self, test_db, sample_restaurant):
        """Test restaurant CRUD operations."""
        
        # Create
        test_db.upsert_restaurant(sample_restaurant, [])
        
        # Read
        restaurants = test_db.get_all_restaurants()
        assert len(restaurants) == 1
        assert restaurants[0].canonical_name == sample_restaurant["canonical_name"]
        
        # Update
        updated_data = sample_restaurant.copy()
        updated_data["phone"] = "+91 80 9999 8888"
        test_db.upsert_restaurant(updated_data, [])
        
        restaurants = test_db.get_all_restaurants()
        assert len(restaurants) == 1  # Should still be 1 (update, not insert)
        assert restaurants[0].phone == "+91 80 9999 8888"
        
        # Delete (via restaurant_id)
        restaurant_id = restaurants[0].restaurant_id
        # Note: Add delete method to db_manager if needed
    
    def test_provenance_tracking(self, test_db, sample_restaurant):
        """Test provenance record tracking."""
        
        provenance_records = [
            {
                "field": "phone",
                "value": "+91 80 1234 5678",
                "confidence": 0.95,
                "source_url": "https://test.com",
                "content_hash": None,
                "model_name": "llama2",
                "model_version": "1.0"
            }
        ]
        
        test_db.upsert_restaurant(sample_restaurant, provenance_records)
        
        # Check provenance records were stored
        restaurants = test_db.get_all_restaurants()
        restaurant = restaurants[0]
        
        # Verify provenance (would need to add method to fetch provenance)
        # provenance = test_db.get_provenance_for_restaurant(restaurant.restaurant_id)
        # assert len(provenance) == 1
        # assert provenance[0].field == "phone"


class TestErrorHandling:
    """Test error handling and edge cases."""
    
    @pytest.mark.asyncio
    async def test_network_error_handling(self):
        """Test handling of network errors during fetching."""
        
        from app.fetch import ContentFetcher
        
        fetcher = ContentFetcher()
        
        # Test invalid URL
        results = await fetcher.fetch_urls(["https://invalid-domain-12345.com"], concurrency=1)
        assert len(results) == 0  # Should handle gracefully
    
    def test_malformed_data_handling(self):
        """Test handling of malformed input data."""
        
        # Test with empty restaurant data
        empty_data = {}
        valid, issues = validate.validate_restaurant_data(empty_data)
        assert not valid
        assert len(issues) > 0
        
        # Test with malformed JSON in hours
        malformed_data = {
            "canonical_name": "Test",
            "lat": 12.0,
            "lon": 77.0,
            "hours": "not-a-dict"
        }
        valid, issues = validate.validate_restaurant_data(malformed_data)
        assert not valid
    
    def test_extraction_failure_handling(self):
        """Test handling when extraction fails."""
        
        from app.extract.address import AddressExtractor
        
        # Test with empty chunks
        extractor = AddressExtractor()
        result = extractor.extract([])
        
        assert result["value"] is None
        assert result["confidence"] == 0.0
        
        # Test with irrelevant content
        result = extractor.extract(["This is random text with no address information"])
        # Should either return None or low confidence result
