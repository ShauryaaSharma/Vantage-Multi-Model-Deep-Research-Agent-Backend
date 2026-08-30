# Directory: yt-DeepResearch-Backend/services/supabase_service.py
"""
Supabase Service - Handles persistent storage for research metrics and comparison data
Provides comprehensive tracking of model performance and research sessions
"""

import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Any
from supabase import create_client, Client
from models.research_models import (
    ComparisonSession, 
    ComparisonResult, 
    ModelMetrics, 
    StageTimings
)

logger = logging.getLogger(__name__)


class SupabaseService:
    """Service for managing persistent storage with Supabase"""
    
    def __init__(self):
        """Initialize Supabase client"""
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_ANON_KEY")
        self.client: Optional[Client] = None
        
        if self.supabase_url and self.supabase_key:
            try:
                self.client = create_client(self.supabase_url, self.supabase_key)
                logger.info("Supabase client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Supabase client: {str(e)}")
        else:
            logger.warning("Supabase credentials not provided. Using in-memory storage.")
    
    async def store_comparison_session(self, session: ComparisonSession) -> bool:
        """
        Store a complete comparison session with all model results
        
        Args:
            session: ComparisonSession object with all results
            
        Returns:
            bool: Success status
        """
        if not self.client:
            logger.warning("Supabase not configured, skipping storage")
            return False
        
        try:
            # Store main session record
            session_data = {
                "session_id": session.session_id,
                "query": session.query,
                "timestamp": session.timestamp,
                "user_feedback": session.user_feedback
            }
            
            session_result = self.client.table("comparison_sessions").insert(session_data).execute()
            
            # Store individual model results
            for result in session.results:
                result_data = {
                    "session_id": session.session_id,
                    "model": result.model,
                    "duration": result.duration,
                    "stage_timings": result.stage_timings.dict(),
                    "sources_found": result.sources_found,
                    "word_count": result.word_count,
                    "success": result.success,
                    "error": result.error,
                    "report_content": result.report_content,
                    "supervisor_tools_used": result.supervisor_tools_used
                }
                
                self.client.table("comparison_results").insert(result_data).execute()
            
            logger.info(f"Stored comparison session {session.session_id} successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error storing comparison session: {str(e)}")
            return False
    
    async def get_comparison_sessions(self, limit: int = 10) -> List[ComparisonSession]:
        """
        Retrieve recent comparison sessions
        
        Args:
            limit: Number of sessions to retrieve
            
        Returns:
            List of ComparisonSession objects
        """
        if not self.client:
            return []
        
        try:
            # Get sessions with their results
            sessions_data = (
                self.client.table("comparison_sessions")
                .select("*, comparison_results(*)")
                .order("timestamp", desc=True)
                .limit(limit)
                .execute()
            )
            
            sessions = []
            for session_data in sessions_data.data:
                results = []
                for result_data in session_data.get("comparison_results", []):
                    stage_timings = StageTimings(**result_data["stage_timings"])
                    result = ComparisonResult(
                        model=result_data["model"],
                        duration=result_data["duration"],
                        stage_timings=stage_timings,
                        sources_found=result_data["sources_found"],
                        word_count=result_data["word_count"],
                        success=result_data["success"],
                        error=result_data.get("error"),
                        report_content=result_data["report_content"],
                        supervisor_tools_used=result_data.get("supervisor_tools_used", [])
                    )
                    results.append(result)
                
                session = ComparisonSession(
                    session_id=session_data["session_id"],
                    query=session_data["query"],
                    timestamp=session_data["timestamp"],
                    results=results,
                    user_feedback=session_data.get("user_feedback")
                )
                sessions.append(session)
            
            return sessions
            
        except Exception as e:
            logger.error(f"Error retrieving comparison sessions: {str(e)}")
            return []
    
    async def get_model_metrics(self) -> List[ModelMetrics]:
        """
        Calculate and retrieve comprehensive model metrics
        
        Returns:
            List of ModelMetrics with detailed performance data
        """
        if not self.client:
            return []
        
        try:
            # Get aggregated metrics for each model
            models = ["openai", "anthropic", "kimi"]
            metrics = []
            
            for model in models:
                # Get all results for this model
                results_data = (
                    self.client.table("comparison_results")
                    .select("*")
                    .eq("model", model)
                    .execute()
                )
                
                if not results_data.data:
                    continue
                
                results = results_data.data
                total_requests = len(results)
                successful_requests = len([r for r in results if r["success"]])
                
                # Calculate averages
                avg_duration = sum(r["duration"] for r in results) / total_requests
                success_rate = (successful_requests / total_requests) * 100
                avg_sources = sum(r["sources_found"] for r in results) / total_requests
                avg_words = sum(r["word_count"] for r in results) / total_requests
                
                # Calculate average stage timings
                avg_clarification = sum(r["stage_timings"]["clarification"] for r in results) / total_requests
                avg_brief = sum(r["stage_timings"]["research_brief"] for r in results) / total_requests
                avg_execution = sum(r["stage_timings"]["research_execution"] for r in results) / total_requests
                avg_final = sum(r["stage_timings"]["final_report"] for r in results) / total_requests
                
                avg_stage_timings = StageTimings(
                    clarification=avg_clarification,
                    research_brief=avg_brief,
                    research_execution=avg_execution,
                    final_report=avg_final
                )
                
                # Get last used timestamp
                last_used = max(r["created_at"] for r in results) if results else None
                
                model_metrics = ModelMetrics(
                    model=model,
                    total_requests=total_requests,
                    average_duration=round(avg_duration, 2),
                    success_rate=round(success_rate, 2),
                    last_used=last_used,
                    average_stage_timings=avg_stage_timings,
                    average_sources_found=round(avg_sources, 1),
                    average_word_count=round(avg_words, 0)
                )
                
                metrics.append(model_metrics)
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error calculating model metrics: {str(e)}")
            return []
    
    async def update_user_feedback(
        self, 
        session_id: str, 
        feedback: Dict[str, Any]
    ) -> bool:
        """
        Update user feedback for a comparison session
        
        Args:
            session_id: Session identifier
            feedback: User feedback dictionary
            
        Returns:
            bool: Success status
        """
        if not self.client:
            return False
        
        try:
            self.client.table("comparison_sessions").update({
                "user_feedback": feedback
            }).eq("session_id", session_id).execute()
            
            logger.info(f"Updated feedback for session {session_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating user feedback: {str(e)}")
            return False
    
    def is_available(self) -> bool:
        """Check if Supabase service is available"""
        return self.client is not None
