#!/usr/bin/env python3
"""
Manual debugging script to verify which Kimi K2 model version is being used.

Not an automated test: it prompts for a live API key via input(), makes a
real network call, and requires a human to read the printed output. Not
collected by pytest. Run directly: python scripts/test_kimi_model.py
"""

import os
import sys
import asyncio
from langchain.chat_models import init_chat_model
from langchain_core.messages import HumanMessage

async def test_kimi_model():
    """Test the Kimi K2 model configuration and verify version"""
    
    print("🧪 Testing Kimi K2 Model Configuration")
    print("=" * 50)
    
    # Set up environment for Kimi K2 (using Moonshot AI API)
    test_api_key = input("Enter your Moonshot AI API key (sk-xxx): ").strip()
    if not test_api_key:
        print("❌ No API key provided. Exiting.")
        return
    
    # Configure environment variables for Kimi K2 (Anthropic-compatible)
    os.environ["ANTHROPIC_API_KEY"] = test_api_key
    os.environ["ANTHROPIC_BASE_URL"] = "https://api.moonshot.ai/anthropic"
    
    print(f"🔧 Configuration:")
    print(f"   API Key: {test_api_key[:10]}...{test_api_key[-4:]}")
    print(f"   Provider: Anthropic-compatible via Moonshot endpoint")
    print(f"   Base URL: https://api.moonshot.ai/anthropic")
    print(f"   Model: kimi-k2-instruct-0905 (Latest Kimi K2)")
    print()
    
    try:
        # Initialize the model using our configuration
        model = init_chat_model(
            model="kimi-k2-instruct-0905",
            model_provider="anthropic",
            api_key=test_api_key
        )
        
        print("✅ Model initialized successfully")
        
        # Test with a simple message
        test_message = "What model are you? Please tell me your exact model name and version."
        print(f"📤 Sending test message: {test_message}")
        
        response = await model.ainvoke([HumanMessage(content=test_message)])
        
        print("📥 Response received:")
        print("-" * 30)
        print(response.content)
        print("-" * 30)
        
        # Check if response metadata contains model info
        if hasattr(response, 'response_metadata'):
            print("📊 Response Metadata:")
            for key, value in response.response_metadata.items():
                print(f"   {key}: {value}")
        
        print("\n✅ Test completed successfully!")
        print("🔍 Look for model version info in the response above")
        
    except Exception as e:
        print(f"❌ Error testing Kimi K2 model: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        
        # Additional debugging info
        print("\n🔧 Debug Information:")
        print(f"   ANTHROPIC_BASE_URL: {os.environ.get('ANTHROPIC_BASE_URL', 'Not set')}")
        print(f"   ANTHROPIC_API_KEY: {'Set' if os.environ.get('ANTHROPIC_API_KEY') else 'Not set'}")
        
        # Suggest possible fixes
        print("\n💡 Possible Solutions:")
        print("   1. Verify your Moonshot AI API key is correct")
        print("   2. Check if the base URL is accessible from your location")
        print("   3. Ensure the model name 'kimi-k2-0905' is correct")
        print("   4. Try with a different model name like 'kimi-k2-0905-preview'")

if __name__ == "__main__":
    print("🚀 Kimi K2 Model Verification Tool")
    print("This script will test if we're using the correct Kimi K2 0905 model\n")
    
    asyncio.run(test_kimi_model())
