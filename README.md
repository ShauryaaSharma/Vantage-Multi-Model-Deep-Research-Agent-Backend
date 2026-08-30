# Deep Research Agent — Backend

A FastAPI backend that runs a multi-stage, multi-model **deep research agent**. Give it a question and one of three model providers, and it clarifies the question if needed, drafts a research brief, runs a multi-tool research loop, and streams the whole process — plus a fully-sourced final report — back to the client in real time over Server-Sent Events (SSE).

It's built on [LangGraph](https://langchain-ai.github.io/langgraph/) (via the [`open_deep_research`](https://github.com/langchain-ai/open_deep_research) research engine), containerized for deployment on Google Cloud Run, and designed to pair with a separate Next.js frontend — [Vantage-Multi-Model-Deep-Research-Agent-Frontend](https://github.com/ShauryaaSharma/Vantage-Multi-Model-Deep-Research-Agent-Frontend) (branded **Vantage** in its UI).

## Features

- **Multi-model support** — OpenAI (GPT-5), Anthropic (Claude 4 Sonnet), and Kimi K2 0905 (via Moonshot AI's Anthropic-compatible endpoint)
- **Multi-stage research pipeline** — clarify → research brief → supervised multi-tool research → compression → final report, each stage streamed as it happens
- **Bring-your-own-key** — callers supply their own provider API key per request; nothing is stored server-side
- **Model comparison** — run the same query across multiple models in parallel and compare duration, sources found, and word count
- **Optional persistence** — comparison sessions can be saved to Supabase; falls back to in-memory storage automatically if unconfigured
- **Cloud Run ready** — Dockerfile with pinned dependencies for reproducible builds

## Architecture

For the full picture — the request lifecycle, the LangGraph pipeline node-by-node with diagrams, configuration resolution, and known architectural limitations — see [ARCHITECTURE.md](ARCHITECTURE.md). Quick map of the codebase:

```
main.py                          FastAPI app, routes, CORS, logging
services/
  deep_research_service.py       Builds the LangGraph RunnableConfig per request,
                                  streams and reformats graph output into StreamingEvents
  model_service.py               Static registry of the 3 supported models
  supabase_service.py            Optional persistence for comparison sessions/metrics
models/
  research_models.py             Pydantic request/response/event schemas
utils/
  metrics.py                     In-memory + Supabase-backed metrics collection
open_deep_research/               The LangGraph research engine (adapted from
                                  LangChain's open_deep_research project)
  deep_researcher.py             The StateGraph: clarify → brief → research → report
  configuration.py               Runtime configuration (models, token limits, search API)
  prompts.py / utils.py          Prompts and search-tool implementations
database_migration/              Supabase schema (see database_migration/README.md)
scripts/                         Manual debugging scripts (not automated tests)
tests/                           The real, automated pytest suite
```

**Request flow:** `POST /research/stream` → `DeepResearchService.stream_research` builds a `RunnableConfig` carrying the caller's API key → `deep_researcher.astream(...)` runs the LangGraph pipeline → each chunk is converted into a `StreamingEvent` and sent as an SSE `data:` line → on completion, duration/success metrics are recorded (Supabase if configured, otherwise in-memory only, lost on restart).

## Quick Start

### Prerequisites

- Python 3.11+
- Docker (for containerized runs/deployment)
- A GCP account (only if deploying to Cloud Run)

### Local Development

1. **Clone the repository**
   ```bash
   git clone https://github.com/ShauryaaSharma/Vantage-Multi-Model-Deep-Research-Agent-Backend.git
   cd Vantage-Multi-Model-Deep-Research-Agent-Backend
   ```

2. **Create a virtual environment and install dependencies**
   ```bash
   python3 -m venv venv
   source venv/bin/activate   # On macOS/Linux
   # .\venv\Scripts\activate  # On Windows (PowerShell)

   pip install -r requirements.txt
   ```

3. **Set up environment variables (optional)**
   ```bash
   cp .env.example .env
   # Nothing here is required to start the server — see Environment Variables below.
   ```

4. **Run the development server**
   ```bash
   uvicorn main:app --host 0.0.0.0 --port 8080 --reload
   ```

5. **Verify it's running**
   ```bash
   curl http://localhost:8080/health
   ```

## Environment Variables

All of these are optional for local development — by default, the frontend supplies the model API key on every request. See [`.env.example`](.env.example) for the full, documented list:

| Variable | Purpose | Default |
|---|---|---|
| `PORT` | Port the server listens on (Cloud Run sets this automatically) | `8080` |
| `ENVIRONMENT` | `development` enables `uvicorn --reload`; anything else runs as production | `production` |
| `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `GOOGLE_API_KEY` | Fallback provider keys, only used if `GET_API_KEYS_FROM_CONFIG=false` | unset |
| `MOONSHOT_API_KEY`, `ANTHROPIC_BASE_URL` | Kimi K2 uses Anthropic's API shape via Moonshot's endpoint (`https://api.moonshot.ai/anthropic`) | unset |
| `GET_API_KEYS_FROM_CONFIG` | `true` (default) uses the per-request key the frontend sends; set `false` to fall back to the env vars above instead | `true` |
| `TAVILY_API_KEY` | Only relevant if `search_api` is switched to `tavily` — the deployed service currently always uses Anthropic's native web search regardless of this setting | unset |
| `SUPABASE_URL`, `SUPABASE_ANON_KEY` | Enables persistent comparison-session storage; omit to use in-memory storage | unset |

`GET_API_KEYS_FROM_CONFIG` defaults to `"true"` unless you explicitly set it in your environment (via `os.environ.setdefault`, in [`services/deep_research_service.py`](services/deep_research_service.py)) — so setting it to `false` locally or in Cloud Run genuinely takes effect.

## API Endpoints

### Health
```bash
GET  /health   # detailed status
HEAD /health   # for Cloud Run health checks
GET  /         # basic liveness message
```

### Research
```bash
POST /research/stream
Content-Type: application/json

{
  "query": "Your research question",
  "model": "anthropic",   # "openai" | "anthropic" | "kimi"
  "api_key": "your_api_key"
}
```
Returns a `text/event-stream` of JSON `StreamingEvent`s (`session_start`, `stage_start`, `stage_update`, `research_step`, `research_finding`, `research_complete`, `error`, …).

```bash
GET    /models                         # list supported models
GET    /research/history               # recent research runs (in-memory unless Supabase configured)
GET    /research/comparison            # aggregate performance metrics across models
DELETE /research/history/{research_id} # delete a research or comparison record
POST   /research/compare               # run the same query across multiple models in parallel
POST   /research/test                  # debug echo endpoint — not gated by environment, avoid exposing publicly
```

## Testing

The real, automated test suite lives in `tests/` and runs with `pytest` (scoped there by [`pytest.ini`](pytest.ini), so a bare `pytest` from the repo root won't accidentally pick up the manual scripts below):

```bash
pip install -r requirements.txt
pytest
```

`scripts/test_kimi_model.py` and `scripts/test_moonshot_auth.py` are **manual debugging scripts**, not automated tests — each prompts for a live API key via `input()`, makes real network calls, and requires a human to read the printed output. Run them directly when you need to sanity-check the Kimi/Moonshot integration:

```bash
python scripts/test_kimi_model.py
python scripts/test_moonshot_auth.py
```

### Manual API testing
```bash
curl -X GET http://localhost:8080/health

curl -X POST http://localhost:8080/research/stream \
  -H "Content-Type: application/json" \
  -d '{"query": "What are the latest developments in AI?", "model": "anthropic", "api_key": "your_api_key"}'
```

## Database (Optional)

Research history and comparison-session persistence are entirely optional — without Supabase configured, everything is tracked in memory and lost on restart.

To enable it: run [`database_migration/supabase_setup.sql`](database_migration/supabase_setup.sql) once in your Supabase project's SQL Editor (see [`database_migration/README.md`](database_migration/README.md) for exactly what it creates and covers), then set `SUPABASE_URL`/`SUPABASE_ANON_KEY`. Note that only the `/research/compare` comparison-session feature persists to Supabase today — individual `/research/stream` runs are in-memory only regardless of Supabase configuration.

## Docker Deployment

The Dockerfile installs from [`requirements.lock`](requirements.lock) — a fully pinned snapshot of `requirements.txt`, generated from a clean Python 3.11 environment — so container builds are reproducible instead of picking up whatever the latest compatible package versions happen to be on build day.

```bash
docker build -t deep-research-backend .
docker run -p 8080:8080 deep-research-backend
```

If you change `requirements.txt`, regenerate the lock file before deploying:
```bash
python3.11 -m venv /tmp/lockenv && /tmp/lockenv/bin/pip install -r requirements.txt
/tmp/lockenv/bin/pip freeze > requirements.lock
# re-add `; sys_platform == "win32"` to the pywin32 line if it reappears —
# pip freeze drops it, and pywin32 has no Linux build
```

## GCP Cloud Run Deployment

```bash
gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/deep-research-backend

gcloud run deploy deep-research-backend \
  --image gcr.io/YOUR_PROJECT_ID/deep-research-backend \
  --platform managed \
  --region europe-west1 \
  --allow-unauthenticated \
  --memory 4Gi \
  --cpu 2 \
  --timeout 3600s \
  --max-instances 5 \
  --min-instances 1 \
  --concurrency 10
```

Set any of the environment variables above under **Cloud Run → Service → Edit & Deploy New Revision → Variables** as needed.

## Connecting to the Frontend

```bash
# In the frontend's .env.local
NEXT_PUBLIC_BACKEND_URL=http://localhost:8080          # local dev
NEXT_PUBLIC_BACKEND_URL=https://your-backend-url.run.app  # production
```

## Security notes

Read this before deploying anywhere beyond local development:

- **CORS is wide open** (`allow_origins=["*"]` in `main.py`) — any origin can call this API from a browser. Restrict this to your actual frontend domain(s) before a public deploy.
- **No authentication or rate limiting** on any endpoint — `/research/history`, `/research/comparison`, and `DELETE /research/history/{id}` are unauthenticated and world-accessible. If you deploy this publicly, put an API gateway, auth layer, or rate limiter in front of it.
- **`/research/test`** is a debug echo endpoint with no environment gating — don't leave it reachable in a production deployment you care about.
- **API keys**: the caller's provider API key is attached to the request and used directly — it's never logged or written to a database. The frontend's own key-storage tradeoffs are documented in its README.

None of this blocks local development or a personal/trusted deployment — it matters once this is exposed to the public internet.

## Troubleshooting

1. **HTTP 405 in GCP logs** — fixed by the `HEAD /health` route for Cloud Run health checks.
2. **Token limit errors** — this is why the service defaults to GPT-5/Claude 4 Sonnet (128k–200k context) rather than older, smaller-context models.
3. **Kimi K2 connection issues** — verify `ANTHROPIC_BASE_URL=https://api.moonshot.ai/anthropic` and that your Moonshot key is being sent as if it were an Anthropic key (Kimi is routed through Anthropic's API shape).
4. **Streaming cuts off** — check any reverse proxy/load balancer's idle-timeout settings; the graph itself has no artificial cutoff, but proxies often do.

## License

MIT License — see [LICENSE.txt](LICENSE.txt).

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add or update tests in `tests/` for behavior changes
4. Run `pytest` before opening a PR
5. Submit a pull request

---

Built with FastAPI, LangGraph, and LangChain.
