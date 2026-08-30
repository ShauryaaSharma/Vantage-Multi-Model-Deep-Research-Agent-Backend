# Directory: yt-DeepResearch-Backend/utils/metrics.py
"""
Metrics Collection Service - Tracks and compares model performance
Provides evaluation metrics for OpenAI, Anthropic, and Kimi K2 comparison
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional
import asyncio

from models.research_models import (
    ModelMetrics, 
    ModelComparison, 
    ResearchHistory,
    ComparisonSession,
    ComparisonResult,
    StageTimings
)
from services.supabase_service import SupabaseService

logger = logging.getLogger(__name__)


class MetricsCollector:
    """Service for collecting and analyzing research performance metrics"""
    
    def __init__(self):
        """Initialize metrics collector with Supabase and fallback in-memory storage"""
        self.supabase_service = SupabaseService()
        self.research_history: List[Dict] = []
        self.model_metrics: Dict[str, Dict] = {
            "openai": {"requests": [], "total_duration": 0, "success_count": 0},
            "anthropic": {"requests": [], "total_duration": 0, "success_count": 0},
            "kimi": {"requests": [], "total_duration": 0, "success_count": 0}
        }
    
    async def store_research_metrics(
        self,
        research_id: str,
        model: str,
        duration: float,
        query: str,
        success: bool = True,
        error: Optional[str] = None
    ) -> None:
        """
        Store metrics for a completed research session
        
        Args:
            research_id: Unique research session identifier
            model: AI model used
            duration: Research duration in seconds
            query: Research query
            success: Whether research completed successfully
            error: Error message if applicable
        """
        try:
            timestamp = datetime.utcnow().isoformat()
            
            # Store in research history
            research_record = {
                "research_id": research_id,
                "model": model,
                "duration": duration,
                "query": query,
                "success": success,
                "timestamp": timestamp,
                "error": error
            }
            self.research_history.append(research_record)
            
            # Update model-specific metrics
            if model in self.model_metrics:
                self.model_metrics[model]["requests"].append({
                    "duration": duration,
                    "success": success,
                    "timestamp": timestamp
                })
                self.model_metrics[model]["total_duration"] += duration
                if success:
                    self.model_metrics[model]["success_count"] += 1
            
            logger.info(f"Stored metrics for research {research_id} using {model}")
            
        except Exception as e:
            logger.error(f"Error storing research metrics: {str(e)}")
    
    async def get_research_history(self, limit: int = 10) -> Dict:
        """
        Get recent research history
        
        Args:
            limit: Maximum number of records to return
            
        Returns:
            Dictionary containing research history
        """
        try:
            # This list also holds comparison-session entries (keys: "type",
            # "data", "timestamp") stored by store_comparison_session, which
            # don't have the fields a research record has. Only keep actual
            # research records here.
            research_records = [
                item for item in self.research_history if "research_id" in item
            ]

            # Sort by timestamp (most recent first)
            sorted_history = sorted(
                research_records,
                key=lambda x: x["timestamp"],
                reverse=True
            )

            # Convert to ResearchHistory objects
            history_items = []
            for item in sorted_history[:limit]:
                history_items.append(ResearchHistory(
                    research_id=item["research_id"],
                    query=item["query"],
                    model=item["model"],
                    duration=item["duration"],
                    success=item["success"],
                    timestamp=item["timestamp"],
                    summary=item.get("summary", f"Research using {item['model']}")
                ))

            return {
                "history": [item.dict() for item in history_items],
                "total_count": len(research_records),
                "generated_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting research history: {str(e)}")
            return {"history": [], "total_count": 0, "error": str(e)}
    
    async def store_comparison_session(self, session: ComparisonSession) -> bool:
        """
        Store a complete comparison session
        
        Args:
            session: ComparisonSession with all model results
            
        Returns:
            bool: Success status
        """
        try:
            # Try Supabase first
            if self.supabase_service.is_available():
                success = await self.supabase_service.store_comparison_session(session)
                if success:
                    logger.info(f"Stored comparison session {session.session_id} in Supabase")
                    return True
            
            # Fallback to in-memory storage
            session_dict = session.dict()
            self.research_history.append({
                "type": "comparison_session",
                "data": session_dict,
                "timestamp": session.timestamp
            })
            
            # Update model metrics from comparison results
            for result in session.results:
                if result.model in self.model_metrics:
                    metrics = self.model_metrics[result.model]
                    metrics["requests"].append({
                        "research_id": session.session_id,
                        "duration": result.duration,
                        "success": result.success,
                        "timestamp": session.timestamp,
                        "stage_timings": result.stage_timings.dict(),
                        "sources_found": result.sources_found,
                        "word_count": result.word_count
                    })
                    metrics["total_duration"] += result.duration
                    if result.success:
                        metrics["success_count"] += 1
            
            logger.info(f"Stored comparison session {session.session_id} in memory")
            return True
            
        except Exception as e:
            logger.error(f"Error storing comparison session: {str(e)}")
            return False

    async def get_model_comparison(self) -> ModelComparison:
        """
        Get performance comparison between different models
        
        Returns:
            ModelComparison with comprehensive metrics for each model
        """
        try:
            # Try to get metrics from Supabase first
            if self.supabase_service.is_available():
                supabase_metrics = await self.supabase_service.get_model_metrics()
                if supabase_metrics:
                    total_requests = sum(m.total_requests for m in supabase_metrics)
                    return ModelComparison(
                        models=supabase_metrics,
                        total_requests=total_requests,
                        generated_at=datetime.utcnow().isoformat()
                    )
            
            # Fallback to in-memory metrics
            model_metrics_list = []
            total_requests = 0
            
            for model_id, metrics in self.model_metrics.items():
                requests = metrics["requests"]
                request_count = len(requests)
                total_requests += request_count
                
                if request_count > 0:
                    # Calculate average duration
                    avg_duration = metrics["total_duration"] / request_count
                    
                    # Calculate success rate
                    success_rate = (metrics["success_count"] / request_count) * 100
                    
                    # Get last used timestamp
                    last_used = max(req["timestamp"] for req in requests) if requests else None
                    
                    # Calculate average stage timings if available
                    avg_stage_timings = None
                    sources_total = 0
                    words_total = 0
                    
                    for req in requests:
                        if "stage_timings" in req:
                            if not avg_stage_timings:
                                avg_stage_timings = StageTimings()
                            timings = req["stage_timings"]
                            avg_stage_timings.clarification += timings.get("clarification", 0)
                            avg_stage_timings.research_brief += timings.get("research_brief", 0)
                            avg_stage_timings.research_execution += timings.get("research_execution", 0)
                            avg_stage_timings.final_report += timings.get("final_report", 0)
                        
                        sources_total += req.get("sources_found", 0)
                        words_total += req.get("word_count", 0)
                    
                    # Average the stage timings
                    if avg_stage_timings:
                        avg_stage_timings.clarification /= request_count
                        avg_stage_timings.research_brief /= request_count
                        avg_stage_timings.research_execution /= request_count
                        avg_stage_timings.final_report /= request_count
                else:
                    avg_duration = 0.0
                    success_rate = 0.0
                    last_used = None
                    avg_stage_timings = None
                    sources_total = 0
                    words_total = 0
                
                model_metrics_list.append(ModelMetrics(
                    model=model_id,
                    total_requests=request_count,
                    average_duration=round(avg_duration, 2),
                    success_rate=round(success_rate, 2),
                    last_used=last_used,
                    average_stage_timings=avg_stage_timings,
                    average_sources_found=round(sources_total / max(request_count, 1), 1),
                    average_word_count=round(words_total / max(request_count, 1), 0)
                ))
            
            return ModelComparison(
                models=model_metrics_list,
                total_requests=total_requests,
                generated_at=datetime.utcnow().isoformat()
            )
            
        except Exception as e:
            logger.error(f"Error generating model comparison: {str(e)}")
            return ModelComparison(
                models=[],
                total_requests=0,
                generated_at=datetime.utcnow().isoformat()
            )
    
    async def delete_research(self, research_id: str) -> bool:
        """
        Delete a specific research session
        
        Args:
            research_id: Research session to delete
            
        Returns:
            True if deleted successfully, False if not found
        """
        try:
            # Find and remove from history
            original_count = len(self.research_history)
            self.research_history = [
                item for item in self.research_history
                if item.get("research_id") != research_id
            ]
            
            deleted = len(self.research_history) < original_count
            
            if deleted:
                logger.info(f"Deleted research session {research_id}")
            
            return deleted
            
        except Exception as e:
            logger.error(f"Error deleting research {research_id}: {str(e)}")
            return False
    
    async def get_detailed_metrics(self, model: str) -> Dict:
        """
        Get detailed metrics for a specific model
        
        Args:
            model: Model identifier
            
        Returns:
            Detailed metrics dictionary
        """
        try:
            if model not in self.model_metrics:
                return {"error": f"Model {model} not found"}
            
            metrics = self.model_metrics[model]
            requests = metrics["requests"]
            
            if not requests:
                return {
                    "model": model,
                    "total_requests": 0,
                    "metrics": {}
                }
            
            # Calculate detailed statistics
            durations = [req["duration"] for req in requests]
            successful_requests = [req for req in requests if req["success"]]
            
            return {
                "model": model,
                "total_requests": len(requests),
                "successful_requests": len(successful_requests),
                "success_rate": (len(successful_requests) / len(requests)) * 100,
                "average_duration": sum(durations) / len(durations),
                "min_duration": min(durations),
                "max_duration": max(durations),
                "total_duration": sum(durations),
                "recent_requests": requests[-5:] if len(requests) >= 5 else requests
            }
            
        except Exception as e:
            logger.error(f"Error getting detailed metrics for {model}: {str(e)}")
            return {"error": str(e)}
