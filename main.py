# Copyright 2024
# Directory: yt-DeepResearch-Backend/main.py
"""
FastAPI Backend for Deep Research Agent
Provides streaming API endpoints for deep research with multiple AI model support
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime
from typing import AsyncGenerator, Dict, List, Optional

import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from services.deep_research_service import DeepResearchService
from services.model_service import ModelService
from models.research_models import (
    ResearchRequest,
    ResearchResponse,
    StreamingEvent,
    ModelComparison,
    ResearchHistory,
    ComparisonSession,
    ComparisonResult,
    StageTimings
)
from utils.metrics import MetricsCollector


class MultiModelComparisonRequest(BaseModel):
    """Request model for multi-model comparison"""
    query: str = Field(..., description="Research question to test across models", min_length=1)
    models: List[str] = Field(..., description="List of model IDs to compare", min_items=1)
    api_keys: Dict[str, str] = Field(..., description="API keys for each model", min_items=1)

# Configure logging with Google Standards
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app with comprehensive configuration
app = FastAPI(
    title="Deep Research Agent API",
    description="Streaming deep research API with multiple AI model support (OpenAI, Anthropic, Kimi K2)",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configure CORS for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services
research_service = DeepResearchService()
model_service = ModelService()
metrics_collector = MetricsCollector()

@app.get("/")
@app.head("/")
async def root():
    """Health check endpoint - supports both GET and HEAD for Cloud Run health checks"""
    return {
        "message": "Deep Research Agent API is running",
        "version": "1.0.0",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/health")
@app.head("/health")
async def health_check():
    """Detailed health check with service status - supports both GET and HEAD"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "services": {
            "research_service": "active",
            "model_service": "active",
            "metrics_collector": "active"
        },
        "supported_models": ["openai", "anthropic", "kimi"]
    }

@app.post("/research/stream")
async def stream_research(request: ResearchRequest):
    """
    Stream deep research process with real-time updates
    
    This endpoint provides Server-Sent Events streaming of the research process,
    showing each stage, thinking process, and tool usage in real-time.
    
    Args:
        request: Research request containing query, model, and API key
    
    Returns:
        StreamingResponse: Server-sent events stream
    """
    try:
        # Validate the request
        if not request.query.strip():
            raise HTTPException(status_code=400, detail="Research query cannot be empty")
        
        if not request.api_key.strip():
            raise HTTPException(status_code=400, detail="API key is required")
        
        # Validate model selection
        available_models = await model_service.get_available_models()
        if request.model not in [model.id for model in available_models["models"]]:
            raise HTTPException(
                status_code=400, 
                detail=f"Unsupported model: {request.model}. Available models: {[m.id for m in available_models['models']]}"
            )
        
        # Create streaming generator
        async def generate_research_stream() -> AsyncGenerator[str, None]:
            research_id = f"research_{int(time.time())}"
            start_time = time.time()
            
            try:
                # Initialize research session
                yield f"data: {json.dumps({'type': 'session_start', 'research_id': research_id, 'timestamp': datetime.utcnow().isoformat(), 'model': request.model, 'query': request.query})}\n\n"
                
                # Stream the research process
                async for event in research_service.stream_research(
                    query=request.query,
                    model=request.model,
                    api_key=request.api_key,
                    research_id=research_id
                ):
                    yield f"data: {json.dumps(event.dict())}\n\n"
                
                # Calculate final metrics
                end_time = time.time()
                duration = end_time - start_time
                
                # Send completion event
                completion_event = {
                    'type': 'research_complete',
                    'research_id': research_id,
                    'duration': duration,
                    'model': request.model,
                    'timestamp': datetime.utcnow().isoformat()
                }
                yield f"data: {json.dumps(completion_event)}\n\n"
                
                # Store metrics for comparison
                await metrics_collector.store_research_metrics(
                    research_id=research_id,
                    model=request.model,
                    duration=duration,
                    query=request.query
                )
                
            except Exception as e:
                logger.error(f"Error in research stream: {str(e)}")
                error_event = {
                    'type': 'error',
                    'message': str(e),
                    'research_id': research_id,
                    'timestamp': datetime.utcnow().isoformat()
                }
                yield f"data: {json.dumps(error_event)}\n\n"
        
        return StreamingResponse(
            generate_research_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "Access-Control-Allow-Origin": "*",
                "X-Accel-Buffering": "no",  # Disable nginx buffering
                "Keep-Alive": "timeout=300, max=100",  # 5 minute timeout
                "X-Content-Type-Options": "nosniff",
            }
        )
        
    except Exception as e:
        logger.error(f"Error starting research stream: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/models")
async def get_available_models():
    """Get list of available AI models with their capabilities"""
    return await model_service.get_available_models()

@app.get("/research/history")
async def get_research_history(limit: int = 10):
    """Get research history with performance metrics"""
    return await metrics_collector.get_research_history(limit=limit)

@app.get("/research/comparison")
async def get_model_comparison():
    """Get performance comparison between different models"""
    return await metrics_collector.get_model_comparison()

@app.delete("/research/history/{research_id}")
async def delete_research(research_id: str):
    """Delete a specific research session"""
    success = await metrics_collector.delete_research(research_id)
    if not success:
        raise HTTPException(status_code=404, detail="Research not found")
    return {"message": "Research deleted successfully"}

@app.post("/research/compare")
async def run_multi_model_comparison(request: MultiModelComparisonRequest):
    """
    Run the same research query across multiple models in parallel
    and store comprehensive comparison metrics
    """
    try:
        # Validate models
        available_models = await model_service.get_available_models()
        available_model_ids = [model.id for model in available_models["models"]]
        
        for model in request.models:
            if model not in available_model_ids:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unsupported model: {model}. Available: {available_model_ids}"
                )
            if model not in request.api_keys or not request.api_keys[model].strip():
                raise HTTPException(
                    status_code=400,
                    detail=f"Missing API key for model: {model}"
                )
        
        session_id = f"compare_{int(time.time())}"
        comparison_results = []
        
        # Run research for each model in parallel
        async def run_model_research(model: str) -> ComparisonResult:
            start_time = time.time()
            stage_start_time = start_time
            current_stage = "clarification"
            
            stage_timings = StageTimings()
            sources_found = 0
            report_content = ""
            supervisor_tools = []
            error = None
            
            try:
                # Stream the research process and collect metrics
                async for event in research_service.stream_research(
                    query=request.query,
                    model=model,
                    api_key=request.api_keys[model],
                    research_id=f"{session_id}_{model}"
                ):
                    # Track stage transitions
                    if event.stage and event.stage != current_stage:
                        # Complete previous stage
                        stage_duration = time.time() - stage_start_time
                        if current_stage == "clarification":
                            stage_timings.clarification = stage_duration
                        elif current_stage == "research_brief":
                            stage_timings.research_brief = stage_duration
                        elif current_stage == "research_execution":
                            stage_timings.research_execution = stage_duration
                        elif current_stage == "final_report":
                            stage_timings.final_report = stage_duration
                        
                        # Start new stage
                        current_stage = event.stage
                        stage_start_time = time.time()
                    
                    # Track sources found
                    if event.type == "sources_found" and event.metadata:
                        sources = event.metadata.get("sources", [])
                        if isinstance(sources, list):
                            sources_found += len(sources)
                    
                    # Track supervisor tools
                    if event.type == "tool_usage" and event.metadata:
                        tool_name = event.metadata.get("tool_name")
                        if tool_name and tool_name not in supervisor_tools:
                            supervisor_tools.append(tool_name)
                    
                    # Collect content
                    if event.content:
                        report_content += event.content + "\n"
                
                # Complete final stage
                final_stage_duration = time.time() - stage_start_time
                if current_stage == "clarification":
                    stage_timings.clarification = final_stage_duration
                elif current_stage == "research_brief":
                    stage_timings.research_brief = final_stage_duration
                elif current_stage == "research_execution":
                    stage_timings.research_execution = final_stage_duration
                elif current_stage == "final_report":
                    stage_timings.final_report = final_stage_duration
                
                success = True
                
            except Exception as e:
                logger.error(f"Error in model {model} research: {str(e)}")
                error = str(e)
                success = False
                
                # Complete current stage with error
                final_stage_duration = time.time() - stage_start_time
                if current_stage == "clarification":
                    stage_timings.clarification = final_stage_duration
                elif current_stage == "research_brief":
                    stage_timings.research_brief = final_stage_duration
                elif current_stage == "research_execution":
                    stage_timings.research_execution = final_stage_duration
                elif current_stage == "final_report":
                    stage_timings.final_report = final_stage_duration
            
            total_duration = time.time() - start_time
            word_count = len(report_content.split()) if report_content else 0
            
            return ComparisonResult(
                model=model,
                duration=total_duration,
                stage_timings=stage_timings,
                sources_found=sources_found,
                word_count=word_count,
                success=success,
                error=error,
                report_content=report_content,
                supervisor_tools_used=supervisor_tools
            )
        
        # Execute all model research in parallel
        tasks = [run_model_research(model) for model in request.models]
        comparison_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions and create proper results
        valid_results = []
        for i, result in enumerate(comparison_results):
            if isinstance(result, Exception):
                logger.error(f"Exception in model {request.models[i]}: {str(result)}")
                # Create error result
                valid_results.append(ComparisonResult(
                    model=request.models[i],
                    duration=0.0,
                    stage_timings=StageTimings(),
                    sources_found=0,
                    word_count=0,
                    success=False,
                    error=str(result),
                    report_content="",
                    supervisor_tools_used=[]
                ))
            else:
                valid_results.append(result)
        
        # Create comparison session
        comparison_session = ComparisonSession(
            session_id=session_id,
            query=request.query,
            timestamp=datetime.utcnow().isoformat(),
            results=valid_results
        )
        
        # Store the session
        await metrics_collector.store_comparison_session(comparison_session)
        
        return comparison_session
        
    except Exception as e:
        logger.error(f"Error in multi-model comparison: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/research/test")
async def test_research_endpoint(request: ResearchRequest):
    """Test endpoint for development and debugging"""
    return {
        "message": "Test endpoint - research parameters received",
        "query": request.query,
        "model": request.model,
        "api_key_length": len(request.api_key) if request.api_key else 0,
        "timestamp": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    # Disable reload in production for faster startup
    reload = os.getenv("ENVIRONMENT", "production") == "development"
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=reload,
        log_level="info"
    )