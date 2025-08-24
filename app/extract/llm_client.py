"""Ollama LLM client for structured JSON extraction."""

import json
import time
from pathlib import Path
from typing import Any, Dict, Optional

import httpx

from ..config import settings
from ..log import logger


class LLMDisabled(Exception):
    """Exception raised when LLM is disabled but required."""
    pass


class OllamaClient:
    """Client for Ollama API with JSON schema validation."""
    
    def __init__(self):
        self.base_url = settings.ollama_base_url
        self.model = settings.ollama_model
        self.timeout = httpx.Timeout(120.0)  # LLM can be slow
        
        # Load prompts
        self.prompts_dir = Path("models/prompts")
        self.system_prompt = self._load_system_prompt()
        self.schemas = self._load_schemas()
    
    def _load_system_prompt(self) -> str:
        """Load system prompt from file."""
        try:
            system_file = self.prompts_dir / "system.txt"
            with open(system_file, 'r', encoding='utf-8') as f:
                return f.read().strip()
        except Exception as e:
            logger.error("Failed to load system prompt", error=str(e))
            return "You are a helpful assistant that extracts structured data from text."
    
    def _load_schemas(self) -> Dict[str, Dict]:
        """Load JSON schemas from files."""
        schemas = {}
        
        schema_files = ["address.json", "hours.json", "cuisines.json"]
        
        for filename in schema_files:
            try:
                schema_file = self.prompts_dir / filename
                with open(schema_file, 'r', encoding='utf-8') as f:
                    schema_name = filename.replace('.json', '')
                    schemas[schema_name] = json.load(f)
            except Exception as e:
                logger.error("Failed to load schema", filename=filename, error=str(e))
        
        return schemas
    
    async def generate_json(
        self,
        model: Optional[str],
        system_prompt: str,
        user_prompt: str,
        schema: Dict[str, Any],
        temperature: float = 0.1,
        max_tokens: int = 256
    ) -> Dict[str, Any]:
        """Generate JSON response using Ollama API."""
        
        if not settings.llm_enabled:
            raise LLMDisabled("LLM is disabled in configuration")
        
        model = model or self.model
        
        logger.info("Generating LLM response", model=model, temperature=temperature)
        
        # Construct the prompt
        full_prompt = f"""System: {system_prompt}

Schema: {json.dumps(schema, indent=2)}

User: {user_prompt}

Response (JSON only):"""
        
        payload = {
            "model": model,
            "prompt": full_prompt,
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_predict": max_tokens,
            },
            "format": "json"  # Request JSON format
        }
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.base_url}/api/generate",
                    json=payload
                )
                response.raise_for_status()
                
                result = response.json()
                
                if not result.get("response"):
                    raise ValueError("Empty response from Ollama")
                
                # Parse JSON response
                json_text = result["response"].strip()
                
                # Try to parse as JSON
                try:
                    parsed_json = json.loads(json_text)
                except json.JSONDecodeError as e:
                    # Try to fix common JSON issues
                    fixed_json = self._fix_json(json_text)
                    try:
                        parsed_json = json.loads(fixed_json)
                    except json.JSONDecodeError:
                        logger.error("Failed to parse JSON response", json_text=json_text, error=str(e))
                        # Return minimal valid response
                        return {"confidence": 0.0, "error": "Invalid JSON response"}
                
                # Validate against schema
                if self._validate_json_schema(parsed_json, schema):
                    logger.debug("Generated valid JSON", model=model)
                    return parsed_json
                else:
                    logger.warning("Generated JSON doesn't match schema", json_data=parsed_json)
                    # Return with low confidence
                    parsed_json["confidence"] = min(parsed_json.get("confidence", 0.0), 0.3)
                    return parsed_json
                
        except Exception as e:
            logger.error("LLM generation failed", model=model, error=str(e))
            # Return minimal response
            return {"confidence": 0.0, "error": str(e)}
    
    def _fix_json(self, json_text: str) -> str:
        """Attempt to fix common JSON formatting issues."""
        # Remove any text before first {
        start_idx = json_text.find('{')
        if start_idx > 0:
            json_text = json_text[start_idx:]
        
        # Remove any text after last }
        end_idx = json_text.rfind('}')
        if end_idx > 0:
            json_text = json_text[:end_idx + 1]
        
        # Fix common issues
        json_text = json_text.replace("'", '"')  # Single to double quotes
        json_text = json_text.replace('True', 'true')  # Python to JSON boolean
        json_text = json_text.replace('False', 'false')
        json_text = json_text.replace('None', 'null')
        
        return json_text
    
    def _validate_json_schema(self, data: Dict[str, Any], schema: Dict[str, Any]) -> bool:
        """Basic JSON schema validation."""
        try:
            # Check required fields
            required = schema.get("required", [])
            for field in required:
                if field not in data:
                    return False
            
            # Check field types
            properties = schema.get("properties", {})
            for field, value in data.items():
                if field in properties:
                    prop_schema = properties[field]
                    expected_type = prop_schema.get("type")
                    
                    if expected_type:
                        if isinstance(expected_type, list):
                            # Handle ["string", "null"] type definitions
                            if value is not None and not any(self._check_type(value, t) for t in expected_type):
                                return False
                        else:
                            if not self._check_type(value, expected_type):
                                return False
            
            return True
            
        except Exception as e:
            logger.error("Schema validation failed", error=str(e))
            return False
    
    def _check_type(self, value: Any, expected_type: str) -> bool:
        """Check if value matches expected JSON type."""
        type_map = {
            "string": str,
            "number": (int, float),
            "integer": int,
            "boolean": bool,
            "array": list,
            "object": dict,
            "null": type(None)
        }
        
        expected_python_type = type_map.get(expected_type)
        if expected_python_type:
            return isinstance(value, expected_python_type)
        
        return False
    
    def get_schema(self, schema_name: str) -> Optional[Dict[str, Any]]:
        """Get schema by name."""
        return self.schemas.get(schema_name)


# Global Ollama client instance
ollama_client = OllamaClient()


async def generate_json(
    model: Optional[str] = None,
    system_prompt: Optional[str] = None,
    user_prompt: str = "",
    schema: Optional[Dict[str, Any]] = None,
    temperature: float = 0.1,
    max_tokens: int = 256
) -> Dict[str, Any]:
    """Generate JSON using Ollama with default system prompt."""
    
    if not settings.llm_enabled:
        raise LLMDisabled("LLM is disabled in configuration")
    
    system_prompt = system_prompt or ollama_client.system_prompt
    schema = schema or {}
    
    return await ollama_client.generate_json(
        model=model,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        schema=schema,
        temperature=temperature,
        max_tokens=max_tokens
    )
