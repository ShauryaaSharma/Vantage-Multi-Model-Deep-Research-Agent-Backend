# Database Migration

`supabase_setup.sql` is the only schema file this project needs. Run it once,
in full, in your Supabase project's SQL Editor.

It creates the two tables `services/supabase_service.py` actually reads and
writes: `comparison_sessions` and `comparison_results`, used by the
`/research/compare` (multi-model comparison) feature.

Individual (single-model) research runs from `/research/stream` are **not**
persisted to Supabase — they're tracked in memory only, in
`utils/metrics.py`, and are lost when the process restarts.
