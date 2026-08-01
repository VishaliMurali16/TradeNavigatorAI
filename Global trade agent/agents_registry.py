AGENTS = [
    {
        "id": "fta_preferential",
        "display_name": "FTA & Preferential Trade Agent",
        "cluster": "Value Capture",
        "cluster_color": "#A100FF",
        "icon": "🤝",
        "tagline": "Identify and apply qualifying Free Trade Agreement benefits across shipments.",
        "status": "coming_soon",
    },
    {
        "id": "ai_classification",
        "display_name": "AI Classification & Origin Agent",
        "cluster": "Value Capture",
        "cluster_color": "#A100FF",
        "icon": "🔍",
        "tagline": "Auto-classify HS codes and determine country of origin with AI precision.",
        "status": "coming_soon",
    },
    {
        "id": "duty_recovery",
        "display_name": "Duty Recovery & Structures Agent",
        "cluster": "Value Capture",
        "cluster_color": "#A100FF",
        "icon": "💰",
        "tagline": "Surface drawback opportunities and optimise duty deferral structures.",
        "status": "coming_soon",
    },
    {
        "id": "compliance_guardian",
        "display_name": "Compliance Guardian Agent",
        "cluster": "Risk & Compliance",
        "cluster_color": "#F76C6C",
        "icon": "🛡️",
        "tagline": "Continuously screen shipments against denied-party lists and sanctions regimes.",
        "status": "coming_soon",
    },
    {
        "id": "esg_compliance",
        "display_name": "ESG Compliance Orchestrator",
        "cluster": "Risk & Compliance",
        "cluster_color": "#F76C6C",
        "icon": "🌿",
        "tagline": "Monitor supply chain ESG obligations — CBAM, EUDR, forced-labour rules.",
        "status": "coming_soon",
    },
    {
        "id": "tariff_shock",
        "display_name": "Tariff Shock & Resilience Agent",
        "cluster": "Agility & Insight",
        "cluster_color": "#12B3A3",
        "icon": "⚡",
        "tagline": "Model tariff-shock scenarios and recommend sourcing pivots in real time.",
        "status": "live",
        # Content rendered is controlled by tariff_shock.enabled in config.yaml alone.
        # Flip enabled=false for instant rollback — no registry change needed.
    },
    {
        "id": "trade_control_tower",
        "display_name": "Trade Control Tower",
        "cluster": "Agility & Insight",
        "cluster_color": "#12B3A3",
        "icon": "🗼",
        "tagline": "Unified visibility across all trade flows, risks, and value opportunities.",
        "status": "coming_soon",
    },
]

CLUSTERS = [
    {"name": "Value Capture",    "color": "#A100FF"},
    {"name": "Risk & Compliance","color": "#F76C6C"},
    {"name": "Agility & Insight","color": "#12B3A3"},
]

def get_agent(agent_id: str) -> dict | None:
    return next((a for a in AGENTS if a["id"] == agent_id), None)

def agents_by_cluster() -> dict:
    result = {}
    for cluster in CLUSTERS:
        result[cluster["name"]] = {
            "color": cluster["color"],
            "agents": [a for a in AGENTS if a["cluster"] == cluster["name"]],
        }
    return result
