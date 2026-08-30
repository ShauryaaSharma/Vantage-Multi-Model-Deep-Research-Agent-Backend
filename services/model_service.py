# Directory: yt-DeepResearch-Backend/services/model_service.py
"""
Model Service - Manages AI model configurations and availability
Supports OpenAI, Anthropic, and Kimi K2 models
"""

import logging
from typing import Dict, List, Optional
from models.research_models import AvailableModel

logger = logging.getLogger(__name__)


class ModelService:
    """Service for managing AI model configurations and capabilities"""
    
    def __init__(self):
        """Initialize the model service with supported models"""
        self._models = self._initialize_models()
    
    def _initialize_models(self) -> Dict[str, AvailableModel]:
        """Initialize supported AI models with their configurations"""
        return {
            "openai": AvailableModel(
                id="openai",
                name="OpenAI GPT-5 (Latest)",
                provider="OpenAI",
                description="Latest GPT-5 model with excellent reasoning and research capabilities",
                capabilities=[
                    "web_search",
                    "document_analysis", 
                    "code_analysis",
                    "multi-step_reasoning",
                    "structured_output"
                ],
                max_tokens=128000
            ),
            "anthropic": AvailableModel(
                id="anthropic",
                name="Claude 4 Sonnet (Latest)",
                provider="Anthropic",
                description="Claude 4 Sonnet - latest model with enhanced analytical and research capabilities (200k tokens)",
                capabilities=[
                    "web_search",
                    "document_analysis",
                    "ethical_reasoning",
                    "multi-step_reasoning", 
                    "structured_output"
                ],
                max_tokens=200000
            ),
            "kimi": AvailableModel(
                id="kimi",
                name="Kimi K2 0905 Preview (Latest)",
                provider="Moonshot AI",
                description="Kimi K2 0905 preview via Moonshot Anthropic-compatible endpoint",
                capabilities=[
                    "web_search",
                    "document_analysis",
                    "multilingual_support",
                    "multi-step_reasoning",
                    "structured_output",
                    "tool_calling",
                    "coding_assistance"
                ],
                max_tokens=128000
            )
        }
    
    async def get_available_models(self) -> Dict[str, List[AvailableModel]]:
        """
        Get list of available AI models
        
        Returns:
            Dict containing available models and metadata
        """
        try:
            return {
                "models": list(self._models.values()),
                "total_count": len(self._models),
                "supported_providers": ["OpenAI", "Anthropic", "Moonshot AI"]
            }
        except Exception as e:
            logger.error(f"Error getting available models: {str(e)}")
            raise
    
    def get_model_config(self, model_id: str) -> Optional[AvailableModel]:
        """
        Get configuration for a specific model
        
        Args:
            model_id: Model identifier
            
        Returns:
            Model configuration or None if not found
        """
        return self._models.get(model_id)
    
    def validate_model(self, model_id: str) -> bool:
        """
        Validate if a model is supported
        
        Args:
            model_id: Model identifier to validate
            
        Returns:
            True if model is supported, False otherwise
        """
        return model_id in self._models
    
    def get_model_provider_mapping(self) -> Dict[str, str]:
        """
        Get mapping of model IDs to their LangChain model names
        
        Returns:
            Dictionary mapping model IDs to LangChain model names
        """
        return {
            # OpenAI
            # Update to GPT-5 family; replace with the exact deployment/model ID you have access to.
            "openai": "gpt-5",
            # Anthropic
            # Claude 3.5 Sonnet - current stable model
            "anthropic": "claude-sonnet-4-20250514",
            # Moonshot Kimi K2 0905 preview via Anthropic-compatible endpoint
            "kimi": "kimi-k2-0905-preview"
        }
    
    def get_api_key_env_var(self, model_id: str) -> Optional[str]:
        """
        Get the environment variable name for a model's API key
        
        Args:
            model_id: Model identifier
            
        Returns:
            Environment variable name or None
        """
        env_vars = {
            "openai": "OPENAI_API_KEY",
            "anthropic": "ANTHROPIC_API_KEY", 
            "kimi": "ANTHROPIC_API_KEY"  # Kimi uses Anthropic-compatible API key
        }
        return env_vars.get(model_id)
