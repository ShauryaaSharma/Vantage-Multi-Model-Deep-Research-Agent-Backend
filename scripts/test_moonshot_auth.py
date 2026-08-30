#!/usr/bin/env python3
"""
Manual debugging script to verify Moonshot AI (Kimi K2) authentication and
API connectivity.

Not an automated test: it prompts for a live API key via input() if
MOONSHOT_API_KEY isn't set, makes real network calls, and requires a human
to read the printed output. Not collected by pytest.
Run directly: python scripts/test_moonshot_auth.py
"""

import os
import requests
import json
from datetime import datetime

def test_moonshot_auth():
    """Test Moonshot AI authentication with different configurations"""
    
    # Test configurations
    test_configs = [
        {
            "name": "Direct Moonshot API",
            "base_url": "https://api.moonshot.ai/v1",
            "model": "moonshot-v1-8k",
            "headers": lambda api_key: {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            }
        },
        {
            "name": "Anthropic API Format",
            "base_url": "https://api.moonshot.ai/v1",
            "model": "claude-3-5-sonnet-20241022",
            "headers": lambda api_key: {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "anthropic-version": "2023-06-01"
            }
        }
    ]
    
    # Get API key from environment or prompt
    api_key = os.getenv("MOONSHOT_API_KEY")
    if not api_key:
        api_key = input("Enter your Moonshot AI API key: ").strip()
        if not api_key:
            print("❌ No API key provided")
            return
    
    print(f"🔑 Using API key: {api_key[:8]}...{api_key[-4:]}")
    print(f"⏰ Test started at: {datetime.now()}")
    print("=" * 60)
    
    for config in test_configs:
        print(f"\n🧪 Testing: {config['name']}")
        print(f"🌐 Base URL: {config['base_url']}")
        print(f"🤖 Model: {config['model']}")
        
        try:
            # Prepare request
            url = f"{config['base_url']}/chat/completions"
            headers = config['headers'](api_key)
            
            payload = {
                "model": config['model'],
                "messages": [
                    {"role": "user", "content": "Hello, can you identify yourself and confirm this API connection works?"}
                ],
                "max_tokens": 100,
                "temperature": 0.1
            }
            
            print(f"📤 Request URL: {url}")
            print(f"📤 Headers: {json.dumps({k: v for k, v in headers.items() if k != 'Authorization'}, indent=2)}")
            print(f"📤 Payload: {json.dumps(payload, indent=2)}")
            
            # Make request
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            
            print(f"📥 Response Status: {response.status_code}")
            print(f"📥 Response Headers: {dict(response.headers)}")
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Success! Response: {json.dumps(data, indent=2)}")
            else:
                print(f"❌ Error {response.status_code}: {response.text}")
                
        except Exception as e:
            print(f"💥 Exception: {str(e)}")
        
        print("-" * 40)
    
    print(f"\n⏰ Test completed at: {datetime.now()}")

if __name__ == "__main__":
    test_moonshot_auth()
