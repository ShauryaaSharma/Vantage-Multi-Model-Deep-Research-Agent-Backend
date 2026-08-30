# Kimi K2 Integration Guide

## Overview

This backend integrates **Kimi K2** (Moonshot AI's 1T parameter model) by leveraging the Anthropic API format with a custom base URL redirect.

## Technical Architecture

```
Frontend → FastAPI Backend → LangChain (Anthropic Format) → Moonshot AI API → Kimi K2 Model
```

## How It Works

### 1. **Model Configuration**
- **Frontend Selection**: User selects "Kimi K2" from the dropdown
- **Backend Mapping**: Maps `"kimi"` → `"anthropic:claude-3-5-sonnet-20241022"` (uses Claude model name, redirected to Kimi K2)
- **API Format**: Uses Anthropic API format but redirects to Moonshot endpoint

### 2. **API Key Handling**
```python
# In services/deep_research_service.py
elif model == "kimi":
    # Kimi K2 uses Anthropic API format with custom base URL
    api_keys["ANTHROPIC_API_KEY"] = api_key  # User's Moonshot API key
    # Set custom base URL for Kimi K2 (Moonshot AI)
    os.environ["ANTHROPIC_BASE_URL"] = "https://api.moonshot.ai"
```

### 3. **Base URL Switching**
- **Kimi K2**: `https://api.moonshot.ai`
- **Regular Anthropic**: Default Anthropic endpoint
- **OpenAI**: Default OpenAI endpoint

## Configuration Files

### Backend Model Service
```python
# services/model_service.py
def get_model_provider_mapping(self):
    return {
        "openai": "openai:gpt-4o",
        "anthropic": "anthropic:claude-3-5-sonnet-20241022", 
        "kimi": "anthropic:claude-3-5-sonnet-20241022"  # Kimi K2 via Claude model name
    }
```

### Token Limits
```python
# open_deep_research/utils.py
MODEL_TOKEN_LIMITS = {
    "moonshot:kimi-k2-0905-preview": 128000,  # Kimi K2 0905 preview (current)
    "anthropic:kimi-k2-0905": 128000,        # Kimi K2 0905 alias via Anthropic-style prefix
    "moonshot:moonshot-v1-128k": 128000,     # Legacy Kimi K2 model
    # ... other models
}
```

## User Experience

### Frontend Display
- **Model Name**: "Kimi K2" 
- **API Key Field**: Users enter their Moonshot AI API key
- **Same Interface**: Identical to other models from user perspective

### Backend Processing
1. User selects "Kimi K2" and enters Moonshot API key
2. Backend sets `MOONSHOT_API_KEY` environment variable
3. LangChain uses native Moonshot integration to connect to Kimi K2
4. Responses stream back through the same pipeline

## Performance Benefits

Published benchmarks comparing Kimi K2 against Claude Sonnet 4:

| Benchmark           | Kimi K2   | Claude Sonnet 4 | Advantage  |
| ------------------- | --------- | --------------- | ---------- |
| LiveCodeBench v6    | **53.7%** | 47.4%           | **+6.3%**  |
| AIME 2024           | **69.6%** | 43.4%           | **+26.2%** |
| Tool Use (Berkeley) | **90.2%** | ~85%            | **+5.2%**  |
| Agentic Benchmarks  | **70.6%** | ~65%            | **+5.6%**  |

## API Key Setup

Users need a **Moonshot AI API key** from:
1. Visit [Moonshot AI Platform](https://platform.moonshot.ai/)
2. Create account and get API key
3. Format: `sk-xxx...` (similar to OpenAI format)
4. Enter this key in the "API Key" field when selecting "Kimi K2"

## Environment Variables

The backend automatically manages these environment variables:

```bash
# For Kimi K2 requests
ANTHROPIC_BASE_URL=https://api.moonshot.ai
ANTHROPIC_API_KEY=<user_provided_moonshot_key>

# Automatically cleared for other models
```

## Error Handling

- **Invalid API Key**: Standard Anthropic-format error messages
- **Rate Limits**: Moonshot AI rate limits apply
- **Token Limits**: 128k tokens for Kimi K2 model
- **Network Issues**: Standard retry logic applies

## Testing

### Verify Model Version

To confirm you're using Kimi K2 0905:

```bash
# Run the manual debugging script (prompts for your Moonshot API key)
cd yt-DeepResearch-Backend
python scripts/test_kimi_model.py
```

This script will:
- Test the API connection
- Ask the model to identify itself
- Show response metadata
- Verify you're using the correct model version

### Integration Testing

To test Kimi K2 integration:

1. **Get Moonshot API Key**: Register at platform.moonshot.ai
2. **Run Model Test**: Use `python scripts/test_kimi_model.py` to verify version
3. **Select Kimi K2**: Choose from frontend dropdown
4. **Enter API Key**: Your Moonshot AI key
5. **Run Research**: Should work identically to other models
6. **Check Logs**: Backend logs will show Moonshot API calls

## Troubleshooting

### Common Issues

1. **"Invalid API Key"**: Check Moonshot AI key format
2. **"Model not found"**: Ensure base URL is correctly set
3. **Rate Limits**: Moonshot AI has different limits than Anthropic
4. **Region Restrictions**: May need VPN depending on location

### Debug Logs

```python
# Backend logs will show:
logger.info(f"Using model: {langchain_model}")
logger.info(f"Base URL: {os.environ.get('ANTHROPIC_BASE_URL', 'default')}")
```

## Future Improvements

1. **Model Selection**: Add more Moonshot AI models
2. **Parameter Tuning**: Expose Kimi-specific parameters
3. **Optimization**: Cache base URL switching
4. **Monitoring**: Add Kimi-specific metrics

This integration allows users to access Kimi K2's superior performance while maintaining a consistent interface with other AI models.
