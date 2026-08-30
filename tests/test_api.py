# Directory: yt-DeepResearch-Backend/tests/test_api.py
"""
Test suite for Deep Research Agent API
Tests basic functionality and streaming endpoints
"""

import pytest
import httpx
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)


class TestBasicEndpoints:
    """Test basic API endpoints"""
    
    def test_root_endpoint(self):
        """Test the root health check endpoint"""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        
        assert "message" in data
        assert "Deep Research Agent API is running" in data["message"]
    
    def test_health_check(self):
        """Test the detailed health check endpoint"""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "services" in data
        assert "supported_models" in data
    
    def test_get_models(self):
        """Test the models endpoint"""
        response = client.get("/models")
        assert response.status_code == 200
        data = response.json()
        assert "models" in data
        assert len(data["models"]) >= 3  # Should have at least OpenAI, Anthropic, Kimi
        
        # Check model structure
        for model in data["models"]:
            assert "id" in model
            assert "name" in model
            assert "provider" in model
    
    def test_research_history_empty(self):
        """Test research history when empty"""
        response = client.get("/research/history")
        assert response.status_code == 200
        data = response.json()
        assert "history" in data
        assert "total_count" in data
    
    def test_model_comparison_empty(self):
        """Test model comparison when no data exists"""
        response = client.get("/research/comparison")
        assert response.status_code == 200
        data = response.json()
        assert "models" in data
        assert "total_requests" in data


class TestResearchEndpoints:
    """Test research-related endpoints"""
    
    def test_research_test_endpoint(self):
        """Test the research test endpoint"""
        test_request = {
            "query": "What is artificial intelligence?",
            "model": "openai",
            "api_key": "test-key-12345"
        }
        
        response = client.post("/research/test", json=test_request)
        assert response.status_code == 200
        data = response.json()
        assert data["query"] == test_request["query"]
        assert data["model"] == test_request["model"]
        assert data["api_key_length"] == len(test_request["api_key"])
    
    def test_research_stream_validation(self):
        """Test research stream endpoint validation

        Note: query/api_key enforce min_length=1 and model is a ModelType enum
        at the Pydantic schema layer, so FastAPI rejects these with 422
        (Unprocessable Entity) before the handler's own manual checks ever run.
        """
        # Test empty query
        response = client.post("/research/stream", json={
            "query": "",
            "model": "openai",
            "api_key": "test-key"
        })
        assert response.status_code == 422

        # Test empty API key
        response = client.post("/research/stream", json={
            "query": "Test query",
            "model": "openai",
            "api_key": ""
        })
        assert response.status_code == 422

        # Test invalid model
        response = client.post("/research/stream", json={
            "query": "Test query",
            "model": "invalid-model",
            "api_key": "test-key"
        })
        assert response.status_code == 422


class TestModelService:
    """Test model service functionality"""
    
    def test_model_validation(self):
        """Test model validation"""
        from services.model_service import ModelService
        
        service = ModelService()
        
        # Test valid models
        assert service.validate_model("openai") == True
        assert service.validate_model("anthropic") == True
        assert service.validate_model("kimi") == True
        
        # Test invalid model
        assert service.validate_model("invalid") == False
    
    def test_model_config(self):
        """Test model configuration retrieval"""
        from services.model_service import ModelService
        
        service = ModelService()
        
        # Test valid model config
        config = service.get_model_config("openai")
        assert config is not None
        assert config.id == "openai"
        assert config.provider == "OpenAI"
        
        # Test invalid model config
        config = service.get_model_config("invalid")
        assert config is None


if __name__ == "__main__":
    pytest.main([__file__])
