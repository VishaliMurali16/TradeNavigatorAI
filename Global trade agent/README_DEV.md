# Global Trade Agent — Dev Setup

Flask web app rendering the TradeNavigator AI dashboard (Control Tower + per-agent pages),
with AI narratives from Accenture's AI Refinery SDK (`air`, model `Qwen/Qwen3-32B`).

## Run it

```powershell
# from this folder
.\.venv\Scripts\Activate.ps1      # activate the virtual environment
python app.py                      # serves http://127.0.0.1:5000
```

Or without activating:
```powershell
.\.venv\Scripts\python.exe app.py
```

Then open http://127.0.0.1:5000 in a browser.

## Project map

| File | Role |
|---|---|
| `app.py` | Flask server — Control Tower home, `/agent/<id>` pages, `/api/posture-summary` (AI) |
| `agents_registry.py` | The 7 agents + 3 clusters; each has a `status` (`coming_soon` / `live`) |
| `data_simulator.py` | Synthetic KPIs + alerts (placeholder — swap for real data later) |
| `party_project.py` | Standalone AI Refinery SDK test script |
| `config.yaml` | AI Refinery orchestrator config |
| `.env` | `API_KEY` (and `ACCOUNT`) — **not committed** |
| `requirements.txt` | flask, python-dotenv, airefinery-sdk |

## Where to add agent logic

Every agent is a `coming_soon` stub. To activate one (e.g. FTA):
1. Flip its `status` to `"live"` in `agents_registry.py`.
2. In `app.py` → `agent_detail()`, replace the placeholder body under the
   `AGENT LOGIC PLUG-IN POINT` comment with the agent's real output.
3. Feed that agent's context dict to `generate_ai_explanation(...)` for the AI narrative
   (adjust the prompt in that function to the agent's domain).

## Notes

- Python venv is `.venv/` in this folder (Python 3.14). VS Code auto-selects it via `.vscode/settings.json`.
- The AI call is cached (60s) and always falls back to a templated summary, so the app runs
  even without API connectivity.
- To (re)install deps: `.\.venv\Scripts\python.exe -m pip install -r requirements.txt`
