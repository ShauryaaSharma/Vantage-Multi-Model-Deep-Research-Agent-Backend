# Directory: yt-DeepResearch-Backend/services/deep_research_service.py
"""
Deep Research Service - Integrates the original deep_researcher.py with streaming capabilities
Provides real-time streaming of research workflow stages and thinking processes
"""

import asyncio
import json
import logging
import re
import sys
import os
from datetime import datetime
from typing import AsyncGenerator, Dict, Any, Optional, List

# Default to getting API keys from config, unless the environment already
# sets GET_API_KEYS_FROM_CONFIG explicitly (e.g. to opt into env-based keys).
os.environ.setdefault("GET_API_KEYS_FROM_CONFIG", "true")

# Add the open_deep_research to Python path
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from open_deep_research.deep_researcher import deep_researcher
from open_deep_research.configuration import Configuration
from open_deep_research.state import AgentInputState
from langchain_core.messages import HumanMessage
from langchain_core.runnables import RunnableConfig

from models.research_models import StreamingEvent, ResearchStage
from services.model_service import ModelService

logger = logging.getLogger(__name__)


class DeepResearchService:
    """Service for handling deep research operations with streaming support"""
    
    def __init__(self):
        """Initialize the deep research service"""
        self.model_service = ModelService()
    
    async def stream_research(
        self,
        query: str,
        model: str,
        api_key: str,
        research_id: str
    ) -> AsyncGenerator[StreamingEvent, None]:
        """
        Stream the deep research process with real-time updates
        
        Args:
            query: Research question/topic
            model: AI model to use (openai, anthropic, kimi)
            api_key: User's API key for the selected model
            research_id: Unique identifier for this research session
        
        Yields:
            StreamingEvent: Real-time updates about the research progress
        """
        import time
        import asyncio
        current_stage = ResearchStage.INITIALIZATION
        workflow_start_time = time.time()
        last_heartbeat = time.time()
        
        # BEST PRACTICE: Set reasonable timeout limits
        RESEARCH_TIMEOUT = 900  # 15 minutes - reasonable for complex research
        HEARTBEAT_INTERVAL = 25  # Send heartbeat every 25 seconds to keep connection alive
        
        try:
            # Configure the research workflow and capture resolved model name
            config, resolved_models = await self._create_research_config(model, api_key)
            
            # Create initial state
            initial_state = AgentInputState(
                messages=[HumanMessage(content=query)]
            )
            
            # Yield initial event (include resolved model for transparency)
            yield StreamingEvent(
                type="stage_start",
                stage=ResearchStage.INITIALIZATION,
                content=f"🚀 Starting deep research for: {query}",
                timestamp=datetime.utcnow().isoformat(),
                research_id=research_id,
                model=model,
                metadata={
                    "query": query,
                    "model_config": model,
                    "resolved_model": resolved_models.get("research_model"),
                    "provider": resolved_models.get("provider"),
                    "start_time": workflow_start_time,
                }
            )
            # Also emit an API log event showing exact model resolved
            yield StreamingEvent(
                type="api_call",
                stage=ResearchStage.INITIALIZATION,
                content=f"Using resolved model: {resolved_models.get('research_model')} (provider: {resolved_models.get('provider')})",
                timestamp=datetime.utcnow().isoformat(),
                research_id=research_id,
                model=model,
            )
            
            # Stream the research workflow with timeout protection
            node_count = 0
            chunk_start_time = time.time()
            
            try:
                # BEST PRACTICE: Track time manually for timeout handling
                async for chunk in deep_researcher.astream(
                    initial_state,
                    config=config,
                    stream_mode="updates"
                ):
                    node_count += 1
                    chunk_duration = time.time() - chunk_start_time
                    logger.info(f"Processing chunk {node_count}: {list(chunk.keys())} (took {chunk_duration:.2f}s)")
                    
                    # BEST PRACTICE: Enhanced monitoring with performance metrics
                    total_elapsed = time.time() - workflow_start_time
                    yield StreamingEvent(
                        type="api_call",
                        stage=current_stage,
                        content=f"📊 Processing chunk {node_count}: {', '.join(chunk.keys())} (⏱️ {chunk_duration:.1f}s, total: {total_elapsed:.1f}s)",
                        timestamp=datetime.utcnow().isoformat(),
                        research_id=research_id,
                        model=model,
                        metadata={
                            "chunk_number": node_count, 
                            "nodes": list(chunk.keys()),
                            "chunk_duration": chunk_duration,
                            "total_elapsed": total_elapsed,
                            "performance": {
                                "chunks_per_minute": (node_count / total_elapsed) * 60 if total_elapsed > 0 else 0,
                                "avg_chunk_duration": total_elapsed / node_count if node_count > 0 else 0,
                                "timeout_risk": "high" if total_elapsed > RESEARCH_TIMEOUT * 0.8 else "low"
                            }
                        }
                    )
                    
                    # Reset timer for next chunk
                    chunk_start_time = time.time()
                    
                    # BEST PRACTICE: Send heartbeat to keep connection alive during long operations
                    current_time = time.time()
                    if current_time - last_heartbeat > HEARTBEAT_INTERVAL:
                        yield StreamingEvent(
                            type="heartbeat",
                            stage=current_stage,
                            content=f"🔄 Research in progress... (elapsed: {current_time - workflow_start_time:.0f}s)",
                            timestamp=datetime.utcnow().isoformat(),
                            research_id=research_id,
                            model=model,
                            metadata={
                                "elapsed_time": current_time - workflow_start_time,
                                "chunks_processed": node_count,
                                "heartbeat": True
                            }
                        )
                        last_heartbeat = current_time
                    
                    # BEST PRACTICE: Check for manual timeout
                    if current_time - workflow_start_time > RESEARCH_TIMEOUT:
                        logger.warning(f"Research timed out after {RESEARCH_TIMEOUT}s, stopping gracefully")
                        yield StreamingEvent(
                            type="timeout_warning",
                            stage=current_stage,
                            content=f"⏰ Research timed out after {RESEARCH_TIMEOUT//60} minutes. Providing partial results based on {node_count} completed research steps.",
                            timestamp=datetime.utcnow().isoformat(),
                            research_id=research_id,
                            model=model,
                            metadata={
                                "timeout_duration": RESEARCH_TIMEOUT,
                                "chunks_completed": node_count,
                                "elapsed_time": current_time - workflow_start_time
                            }
                        )
                        break
                    
                    # Process each chunk and convert to streaming event
                    for node_name, node_data in chunk.items():
                        try:
                            node_start_time = time.time()
                            logger.info(f"Processing node: {node_name} (chunk {node_count})")
                            event = await self._process_workflow_node(
                                node_name, node_data, research_id, model, node_count
                            )
                            
                            if event:
                                current_stage = event.stage or current_stage
                                node_duration = time.time() - node_start_time
                                logger.info(f"Yielding event for {node_name}: {event.type} (took {node_duration:.2f}s)")
                                
                                # Add timing info to event metadata
                                if event.metadata:
                                    event.metadata["node_duration"] = node_duration
                                else:
                                    event.metadata = {"node_duration": node_duration}
                                
                                yield event
                                
                                # Extract and send sources immediately if found
                                if event.content:
                                    sources = self._extract_sources_from_text(event.content)
                                    if sources:
                                        yield StreamingEvent(
                                            type="sources_found",
                                            stage=current_stage,
                                            content=f"📎 Found {len(sources)} sources",
                                            timestamp=datetime.utcnow().isoformat(),
                                            research_id=research_id,
                                            model=model,
                                            metadata={"sources": sources, "node_name": node_name}
                                        )
                                
                            # Special handling for research_supervisor chunk to show research progress
                            if node_name == "research_supervisor" and node_data:
                                logger.info(f"Processing research supervisor data for chunk {node_count}")
                                async for research_event in self._process_research_supervisor_data(
                                    node_data, research_id, model, node_count
                                ):
                                    logger.info(f"Yielding research supervisor event: {research_event.type}")
                                    yield research_event
                        except Exception as e:
                            logger.error(f"Error processing node {node_name}: {str(e)}")
                            # Continue processing other nodes
                            continue
                            
                logger.info(f"Completed streaming workflow with {node_count} chunks")
            except Exception as e:
                logger.error(f"Error in streaming workflow: {str(e)}")
                raise
            
            # Final completion event
            yield StreamingEvent(
                type="stage_complete",
                stage=ResearchStage.COMPLETED,
                content="✅ Deep research completed successfully!",
                timestamp=datetime.utcnow().isoformat(),
                research_id=research_id,
                model=model,
                metadata={"total_nodes": node_count}
            )
                    
        except Exception as e:
            logger.error(f"Error in stream_research: {str(e)}")
            yield StreamingEvent(
                type="error",
                stage=current_stage,
                content=f"❌ Error occurred: {str(e)}",
                timestamp=datetime.utcnow().isoformat(),
                research_id=research_id,
                model=model,
                error=str(e)
            )
    
    async def _create_research_config(self, model: str, api_key: str) -> tuple[RunnableConfig, dict]:
        """
        Create LangChain configuration for the research workflow
        
        Args:
            model: Model identifier
            api_key: User's API key
            
        Returns:
            RunnableConfig for the research workflow
        """
        try:
            # Get model mapping
            model_mapping = self.model_service.get_model_provider_mapping()
            langchain_model = model_mapping.get(model, "gpt-4")
            
            # For Kimi, use the model from mapping (should be kimi-k2-instruct-0905)
            # No override needed - let the model mapping handle it
            
            # SIMPLIFIED: Direct user API key approach
            # Store user's API key directly in config for immediate use
            user_api_key = api_key
            
            # Configure environment for different model providers
            if model == "openai":
                # Ensure we use default OpenAI base URL
                os.environ.pop("ANTHROPIC_BASE_URL", None)
            elif model == "anthropic":
                # Ensure we use default Anthropic base URL
                os.environ.pop("ANTHROPIC_BASE_URL", None)
            elif model == "kimi":
                # Kimi K2 uses Anthropic-compatible API via Moonshot endpoint
                # Set Anthropic base URL to Moonshot AI Anthropic-compatible endpoint
                os.environ["ANTHROPIC_BASE_URL"] = "https://api.moonshot.ai/anthropic"
                logger.info(f"Configured Kimi K2-Instruct-0905 with Anthropic-compatible API via Moonshot endpoint")
            
            # LEGACY: Old complex API keys system (commented out for future env variable use)
            # api_keys = {}
            # if model == "openai":
            #     api_keys["OPENAI_API_KEY"] = api_key
            # elif model == "anthropic":
            #     api_keys["ANTHROPIC_API_KEY"] = api_key
            # elif model == "kimi":
            #     api_keys["ANTHROPIC_API_KEY"] = api_key
            
            # For Kimi, we need to specify the model provider explicitly
            model_provider = None
            if model == "kimi":
                model_provider = "anthropic"  # Kimi uses Anthropic-compatible API
            elif model == "openai":
                model_provider = "openai"
            elif model == "anthropic":
                model_provider = "anthropic"
            
            # BEST PRACTICE: Balanced configuration for production use
            # Optimized for reliability, speed, and cost-effectiveness
            config_dict = {
                "research_model": langchain_model,
                "research_model_max_tokens": 4000,
                "final_report_model": langchain_model,  # Use same model for final report
                "final_report_model_max_tokens": 8000,
                "compression_model": langchain_model,  # Use same model for compression
                "compression_model_max_tokens": 4000,
                "summarization_model": langchain_model,  # Use same model for summarization
                "summarization_model_max_tokens": 4000,
                "allow_clarification": False,  # Skip clarification for faster results
                "max_structured_output_retries": 2,  # Reasonable retry limit
                "search_api": "anthropic",  # Use Anthropic search API
                
                # BEST PRACTICE: Conservative limits to prevent timeouts
                "max_researcher_iterations": 1,  # Single iteration to stay under timeout
                "max_react_tool_calls": 3,      # Focused research with 3 searches max
                "max_concurrent_research_units": 2,  # Limited parallelism for stability
                
                # API key configuration
                "user_api_key": user_api_key
            }
            
            # Add model provider if specified
            if model_provider:
                config_dict["research_model_provider"] = model_provider
                config_dict["final_report_model_provider"] = model_provider
                config_dict["compression_model_provider"] = model_provider
                config_dict["summarization_model_provider"] = model_provider
            
            config = RunnableConfig(
                configurable=config_dict,
                metadata={
                    "model_type": model
                }
            )
            
            # Return config plus resolved model/provider for transparency
            return config, {
                "research_model": langchain_model,
                "provider": model_provider or model,
            }
            
        except Exception as e:
            logger.error(f"Error creating research config: {str(e)}")
            raise
    
    async def _process_workflow_node(
        self,
        node_name: str,
        node_data: Any,
        research_id: str,
        model: str,
        node_count: int
    ) -> Optional[StreamingEvent]:
        """
        Process a workflow node and convert to streaming event
        
        Args:
            node_name: Name of the workflow node
            node_data: Data from the workflow node
            research_id: Research session ID
            model: Model being used
            node_count: Current node number
            
        Returns:
            StreamingEvent or None
        """
        try:
            # Map node names to research stages
            stage_mapping = {
                "clarify_with_user": ResearchStage.CLARIFICATION,
                "write_research_brief": ResearchStage.RESEARCH_BRIEF,
                "research_supervisor": ResearchStage.RESEARCH_EXECUTION,
                "final_report_generation": ResearchStage.FINAL_REPORT
            }
            
            stage = stage_mapping.get(node_name, ResearchStage.RESEARCH_EXECUTION)
            
            # Create content based on node type
            content = await self._generate_node_content(node_name, node_data, node_count)
            
            return StreamingEvent(
                type="stage_update",
                stage=stage,
                content=content,
                timestamp=datetime.utcnow().isoformat(),
                research_id=research_id,
                model=model,
                metadata={
                    "node_name": node_name,
                    "node_count": node_count,
                    "has_messages": hasattr(node_data, 'messages') if hasattr(node_data, '__dict__') else False
                }
            )
            
        except Exception as e:
            logger.error(f"Error processing workflow node {node_name}: {str(e)}")
            return StreamingEvent(
                type="error",
                stage=ResearchStage.ERROR,
                content=f"Error processing {node_name}: {str(e)}",
                timestamp=datetime.utcnow().isoformat(),
                research_id=research_id,
                model=model,
                error=str(e)
            )
    
    async def _generate_node_content(self, node_name: str, node_data: Any, node_count: int) -> str:
        """
        Generate human-readable content for a workflow node with comprehensive data extraction
        
        Args:
            node_name: Name of the workflow node
            node_data: Data from the workflow node
            node_count: Current node number
            
        Returns:
            Human-readable content string with actual AI messages and content
        """
        try:
            # Enhanced logging for debugging
            logger.info(f"=== PROCESSING NODE {node_name} ===")
            logger.info(f"Node data type: {type(node_data)}")
            if hasattr(node_data, '__dict__'):
                logger.info(f"Node data attributes: {list(node_data.__dict__.keys())}")
                # Log the actual values for key attributes
                for attr in ['messages', 'final_report', 'research_brief', 'notes', 'compressed_research']:
                    if hasattr(node_data, attr):
                        attr_value = getattr(node_data, attr)
                        logger.info(f"  {attr}: {type(attr_value)} - {str(attr_value)[:200] if attr_value else 'None'}...")
            
            # Try to log the raw node_data structure
            logger.info(f"Raw node_data: {str(node_data)[:500]}...")
            
            # Extract actual content based on node type
            extracted_content = ""
            
            if node_name == "clarify_with_user":
                extracted_content = f"🔍 Step {node_count}: Analyzing research scope and clarifying requirements"
                
                # Handle None case for clarify_with_user
                if node_data is None:
                    extracted_content += f"\n✅ No clarification needed - proceeding with original query"
                else:
                    # Extract all messages from clarification
                    ai_messages = self._extract_ai_messages(node_data)
                    if ai_messages:
                        extracted_content += f"\n\n💭 AI Clarification Process:"
                        for i, msg in enumerate(ai_messages[:3]):  # Show up to 3 messages
                            extracted_content += f"\n🤖 Message {i+1}: {msg}"
                    else:
                        # Fallback: try to extract any text content
                        fallback_content = self._extract_text_content(node_data)
                        if fallback_content:
                            extracted_content += f"\n💭 AI Decision: {fallback_content}"
                
            elif node_name == "write_research_brief":
                extracted_content = f"📝 Step {node_count}: Creating comprehensive research brief and strategy"
                
                # Handle dict structure for research brief
                if isinstance(node_data, dict) and 'research_brief' in node_data:
                    brief_content = str(node_data['research_brief'])
                    # Show the full research brief content (no truncation)
                    extracted_content += f"\n\n📋 Generated Research Brief:\n{brief_content}"
                else:
                    # Extract AI messages from research brief creation
                    ai_messages = self._extract_ai_messages(node_data)
                    if ai_messages:
                        extracted_content += f"\n\n📋 AI Research Brief Generation:"
                        for i, msg in enumerate(ai_messages[:2]):  # Show up to 2 messages
                            extracted_content += f"\n🤖 Brief {i+1}: {msg}"
                    
                    # Also look for specific research brief attributes
                    if hasattr(node_data, 'research_brief') and node_data.research_brief:
                        brief_content = str(node_data.research_brief)[:300] + "..." if len(str(node_data.research_brief)) > 300 else str(node_data.research_brief)
                        extracted_content += f"\n📄 Final Brief: {brief_content}"
                    
                    # Fallback content extraction
                    if not ai_messages:
                        fallback_content = self._extract_text_content(node_data)
                        if fallback_content:
                            extracted_content += f"\n📋 Research Strategy: {fallback_content}"
                
            elif node_name == "research_supervisor":
                extracted_content = f"🔬 Step {node_count}: Conducting deep research using multiple tools and sources"
                
                # Extract research findings
                if hasattr(node_data, 'notes') and node_data.notes:
                    findings_count = len(node_data.notes)
                    extracted_content += f"\n📊 Found {findings_count} research findings"
                    # Show first few findings with source extraction
                    for i, note in enumerate(node_data.notes[:3]):
                        if note and len(str(note)) > 20:
                            note_content = str(note)
                            # Extract sources from the note
                            sources = self._extract_sources_from_text(note_content)
                            note_preview = note_content[:150] + "..." if len(note_content) > 150 else note_content
                            extracted_content += f"\n🔍 Finding {i+1}: {note_preview}"
                            if sources:
                                extracted_content += f"\n📎 Sources: {', '.join(sources[:2])}"
                
                # Extract compressed research
                if hasattr(node_data, 'compressed_research') and node_data.compressed_research:
                    research_summary = str(node_data.compressed_research)[:200] + "..." if len(str(node_data.compressed_research)) > 200 else str(node_data.compressed_research)
                    extracted_content += f"\n📝 Research Summary: {research_summary}"
                
            elif node_name == "final_report_generation":
                extracted_content = f"📊 Step {node_count}: Generating final research report with findings and analysis"
                
                # Handle dict structure for final report
                if isinstance(node_data, dict) and 'final_report' in node_data:
                    report_content = str(node_data['final_report'])
                    # Ensure sources are included in the final report
                    sources = self._extract_sources_from_text(report_content)
                    if sources:
                        report_content += f"\n\n## Sources and References\n"
                        for i, source in enumerate(sources, 1):
                            if source.startswith('http'):
                                report_content += f"{i}. {source}\n"
                            else:
                                report_content += f"{i}. {source}\n"
                    # Show the full final report content with sources
                    extracted_content += f"\n\n📄 Final Report: {report_content}"
                else:
                    # Extract AI messages from final report generation
                    ai_messages = self._extract_ai_messages(node_data)
                    if ai_messages:
                        extracted_content += f"\n\n📄 AI Report Generation:"
                        for i, msg in enumerate(ai_messages[:2]):  # Show up to 2 messages
                            extracted_content += f"\n🤖 Report {i+1}: {msg}"
                    
                    # Extract final report content
                    if hasattr(node_data, 'final_report') and node_data.final_report:
                        report_preview = str(node_data.final_report)[:400] + "..." if len(str(node_data.final_report)) > 400 else str(node_data.final_report)
                        extracted_content += f"\n📋 Generated Report: {report_preview}"
                    
                    # Fallback content extraction
                    if not ai_messages:
                        fallback_content = self._extract_text_content(node_data)
                        if fallback_content:
                            extracted_content += f"\n📄 Final Report: {fallback_content}"
            
            else:
                extracted_content = f"⚙️ Step {node_count}: Processing {node_name.replace('_', ' ').title()}"
            
            # Always try to extract general message content if we haven't found specific content
            if "\n" not in extracted_content and hasattr(node_data, 'messages') and node_data.messages:
                for msg in node_data.messages:
                    if hasattr(msg, 'content') and isinstance(msg.content, str) and len(msg.content) > 50:
                        preview = msg.content[:200] + "..." if len(msg.content) > 200 else msg.content
                        extracted_content += f"\n💭 Content: {preview}"
                        break
            
            return extracted_content
            
        except Exception as e:
            logger.error(f"Error generating node content for {node_name}: {str(e)}")
            return f"⚙️ Step {node_count}: Processing {node_name}"
    
    def _extract_ai_messages(self, node_data: Any) -> List[str]:
        """
        Extract AI messages from node data
        
        Args:
            node_data: Data from the workflow node
            
        Returns:
            List of AI message contents
        """
        messages = []
        
        try:
            # Check for messages attribute
            if hasattr(node_data, 'messages') and node_data.messages:
                for msg in node_data.messages:
                    if hasattr(msg, 'content'):
                        content = str(msg.content)
                        # Filter for substantial AI responses (not just system messages)
                        if len(content) > 20 and not content.startswith('Human:'):
                            # Truncate very long messages
                            if len(content) > 500:
                                content = content[:500] + "..."
                            messages.append(content)
            
            # Also check for direct content in various possible attributes
            content_attributes = ['content', 'response', 'output', 'result', 'text']
            for attr in content_attributes:
                if hasattr(node_data, attr):
                    attr_value = getattr(node_data, attr)
                    if attr_value and isinstance(attr_value, str) and len(attr_value) > 20:
                        if len(attr_value) > 500:
                            attr_value = attr_value[:500] + "..."
                        messages.append(attr_value)
                        break
            
            return messages[:5]  # Limit to 5 messages max
            
        except Exception as e:
            logger.error(f"Error extracting AI messages: {str(e)}")
            return []
    
    def _extract_text_content(self, node_data: Any) -> str:
        """
        Extract any meaningful text content from node data as fallback
        
        Args:
            node_data: Data from the workflow node
            
        Returns:
            Extracted text content or empty string
        """
        try:
            # Try various ways to extract text content
            if hasattr(node_data, 'messages') and node_data.messages:
                # Get the last substantial message
                for msg in reversed(node_data.messages):
                    if hasattr(msg, 'content') and isinstance(msg.content, str):
                        if len(msg.content) > 50:
                            return msg.content[:300] + ("..." if len(msg.content) > 300 else "")
            
            # Try converting the whole object to string as last resort
            if hasattr(node_data, '__dict__'):
                data_str = str(node_data.__dict__)
                if len(data_str) > 100:
                    return data_str[:200] + "..."
            
            return ""
            
        except Exception as e:
            logger.error(f"Error extracting text content: {str(e)}")
            return ""
    
    def _extract_sources_from_text(self, text: str) -> List[str]:
        """
        Extract sources and URLs from research text
        
        Args:
            text: Text content to extract sources from
            
        Returns:
            List of extracted sources
        """
        sources = []
        
        try:
            # Extract URLs
            url_pattern = r'https?://[^\s<>"{}|\\^`[\]]+'
            urls = re.findall(url_pattern, text)
            sources.extend(urls)
            
            # Extract source patterns
            source_patterns = [
                r'SOURCE:\s*([^\n]+)',
                r'Source:\s*([^\n]+)', 
                r'from\s+([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
                r'according to\s+([^\n,.]+)',
                r'cited from\s+([^\n,.]+)',
                r'reference:\s*([^\n]+)',
                r'via\s+([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'
            ]
            
            for pattern in source_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                sources.extend([match.strip() for match in matches if len(match.strip()) > 3])
            
            # Remove duplicates and filter
            unique_sources = list(set(sources))
            return [source for source in unique_sources if len(source) > 3][:5]  # Limit to 5 sources
            
        except Exception as e:
            logger.error(f"Error extracting sources: {str(e)}")
            return []
    
    async def _process_research_supervisor_data(
        self,
        node_data: Any,
        research_id: str,
        model: str,
        node_count: int
    ) -> AsyncGenerator[StreamingEvent, None]:
        """
        Process research supervisor data to extract and stream research findings
        
        Args:
            node_data: Data from the research supervisor node
            research_id: Research session ID
            model: Model being used
            node_count: Current node number
        """
        try:
            # Show research planning phase
            yield StreamingEvent(
                type="research_step",
                stage=ResearchStage.RESEARCH_PLANNING,
                content="🎯 Planning research strategy and identifying key information sources...",
                timestamp=datetime.utcnow().isoformat(),
                research_id=research_id,
                model=model,
                metadata={"step": "planning", "node_count": node_count}
            )
            
            # Extract supervisor messages to show tool decisions
            supervisor_messages = []
            if hasattr(node_data, 'supervisor_messages') and node_data.supervisor_messages:
                supervisor_messages = node_data.supervisor_messages
            elif isinstance(node_data, dict) and 'supervisor_messages' in node_data:
                supervisor_messages = node_data['supervisor_messages']
            
            # Show supervisor tool decisions
            for i, msg in enumerate(supervisor_messages[:3]):
                if hasattr(msg, 'tool_calls') and msg.tool_calls:
                    for j, tool_call in enumerate(msg.tool_calls):
                        tool_name = tool_call.get('name', 'unknown_tool')
                        tool_args = tool_call.get('args', {})
                        
                        if tool_name == 'ConductResearch':
                            research_topic = tool_args.get('research_topic', 'Unknown topic')
                            yield StreamingEvent(
                                type="research_step",
                                stage=ResearchStage.RESEARCH_EXECUTION,
                                content=f"🎯 **Supervisor Decision**: Conducting research on '{research_topic}'",
                                timestamp=datetime.utcnow().isoformat(),
                                research_id=research_id,
                                model=model,
                                metadata={"step": "supervisor_decision", "tool": tool_name, "topic": research_topic}
                            )
                        elif tool_name == 'think_tool':
                            reflection = tool_args.get('reflection', '')[:200] + "..." if len(tool_args.get('reflection', '')) > 200 else tool_args.get('reflection', '')
                            yield StreamingEvent(
                                type="research_step",
                                stage=ResearchStage.RESEARCH_PLANNING,
                                content=f"🤔 **Supervisor Thinking**: {reflection}",
                                timestamp=datetime.utcnow().isoformat(),
                                research_id=research_id,
                                model=model,
                                metadata={"step": "supervisor_thinking", "tool": tool_name}
                            )
                        elif tool_name == 'ResearchComplete':
                            yield StreamingEvent(
                                type="research_step",
                                stage=ResearchStage.RESEARCH_SYNTHESIS,
                                content=f"✅ **Supervisor Decision**: Research complete - sufficient information gathered",
                                timestamp=datetime.utcnow().isoformat(),
                                research_id=research_id,
                                model=model,
                                metadata={"step": "supervisor_completion", "tool": tool_name}
                            )
            
            # Extract and show AI messages (research queries being made)
            ai_messages = self._extract_ai_messages(node_data)
            research_queries = []
            search_urls = []
            
            for i, message in enumerate(ai_messages[:5]):  # Show up to 5 research queries
                if len(message) > 50:  # Show more queries for transparency
                    # Extract search terms and URLs from message
                    urls_in_message = self._extract_sources_from_text(message)
                    search_urls.extend(urls_in_message)
                    
                    # Clean up message for display and detect tool usage
                    display_message = message
                    if 'search' in message.lower():
                        display_message = f"🌐 **Web Search**: {message[:150]}..." if len(message) > 150 else f"🌐 **Web Search**: {message}"
                    elif 'think' in message.lower():
                        display_message = f"💭 **AI Thinking**: {message[:150]}..." if len(message) > 150 else f"💭 **AI Thinking**: {message}"
                    else:
                        display_message = f"🔍 **Research Action**: {message[:150]}..." if len(message) > 150 else f"🔍 **Research Action**: {message}"
                    
                    research_queries.append(display_message)
                    
                    yield StreamingEvent(
                        type="research_step",
                        stage=ResearchStage.RESEARCH_EXECUTION,
                        content=display_message,
                        timestamp=datetime.utcnow().isoformat(),
                        research_id=research_id,
                        model=model,
                        metadata={
                            "step": "research_action", 
                            "query_index": i+1, 
                            "total_queries": len(ai_messages),
                            "urls": urls_in_message
                        }
                    )
                    
                    # Send sources immediately if found
                    if urls_in_message:
                        yield StreamingEvent(
                            type="sources_found",
                            stage=ResearchStage.RESEARCH_EXECUTION,
                            content=f"📎 Found {len(urls_in_message)} sources from research action {i+1}",
                            timestamp=datetime.utcnow().isoformat(),
                            research_id=research_id,
                            model=model,
                            metadata={"sources": urls_in_message, "search_index": i+1}
                        )
            
            # Show comprehensive research progress
            if research_queries:
                comprehensive_update = f"## 🔍 Research Activities Completed:\n\n"
                for i, query in enumerate(research_queries, 1):
                    comprehensive_update += f"**{i}.** {query}\n\n"
                
                if search_urls:
                    comprehensive_update += f"\n## 📎 Sources Discovered:\n"
                    for i, url in enumerate(search_urls[:5], 1):  # Show first 5 URLs
                        domain = url.replace('https://', '').replace('http://', '').split('/')[0]
                        comprehensive_update += f"- [{domain}]({url})\n"
                    
                    if len(search_urls) > 5:
                        comprehensive_update += f"- +{len(search_urls) - 5} more sources...\n"
                
                yield StreamingEvent(
                    type="research_step",
                    stage=ResearchStage.RESEARCH_EXECUTION,
                    content=comprehensive_update,
                    timestamp=datetime.utcnow().isoformat(),
                    research_id=research_id,
                    model=model,
                    metadata={"step": "comprehensive_progress", "total_searches": len(research_queries), "total_sources": len(search_urls)}
                )
            
            # Show analysis phase
            yield StreamingEvent(
                type="research_step",
                stage=ResearchStage.RESEARCH_ANALYSIS,
                content="📊 Analyzing findings from multiple sources and cross-referencing information...",
                timestamp=datetime.utcnow().isoformat(),
                research_id=research_id,
                model=model,
                metadata={"step": "analysis", "node_count": node_count}
            )
            
            # Check if we have research findings
            if hasattr(node_data, 'notes') and node_data.notes:
                for i, note in enumerate(node_data.notes):
                    if note and len(str(note)) > 50:  # Only show substantial content
                        yield StreamingEvent(
                            type="research_finding",
                            stage=ResearchStage.RESEARCH_EXECUTION,
                            content=f"🔍 Research Finding {i+1}: {str(note)[:200]}..." if len(str(note)) > 200 else f"🔍 Research Finding {i+1}: {str(note)}",
                            timestamp=datetime.utcnow().isoformat(),
                            research_id=research_id,
                            model=model,
                            metadata={
                                "finding_index": i+1,
                                "finding_length": len(str(note)),
                                "node_count": node_count
                            }
                        )
            
            # Show synthesis phase
            yield StreamingEvent(
                type="research_step",
                stage=ResearchStage.RESEARCH_SYNTHESIS,
                content="🧠 Synthesizing findings and preparing comprehensive analysis...",
                timestamp=datetime.utcnow().isoformat(),
                research_id=research_id,
                model=model,
                metadata={"step": "synthesis", "node_count": node_count}
            )
            
            # Check for compressed research
            if hasattr(node_data, 'compressed_research') and node_data.compressed_research:
                yield StreamingEvent(
                    type="research_summary",
                    stage=ResearchStage.RESEARCH_EXECUTION,
                    content=f"📊 Research Summary: {str(node_data.compressed_research)[:300]}..." if len(str(node_data.compressed_research)) > 300 else f"📊 Research Summary: {str(node_data.compressed_research)}",
                    timestamp=datetime.utcnow().isoformat(),
                    research_id=research_id,
                    model=model,
                    metadata={
                        "summary_length": len(str(node_data.compressed_research)),
                        "node_count": node_count
                    }
                )
                
        except Exception as e:
            logger.error(f"Error processing research supervisor data: {str(e)}")
            # Don't yield error events for this as it's supplementary
