import random
from datetime import datetime, timedelta

_ALERT_TEMPLATES = [
    ("fta_preferential",  "high",   "USMCA certificate of origin missing on 3 shipments — duty exposure $42,000"),
    ("ai_classification", "medium", "HS code 8471.30 flagged for reclassification review — 12 line items"),
    ("duty_recovery",     "low",    "Drawback claim window closing in 7 days for Q4 imports — $118,500 at risk"),
    ("compliance_guardian","high",  "Denied-party match detected: consignee 'Orion Trading LLC' — shipment held"),
    ("esg_compliance",    "medium", "EUDR deforestation evidence required for cocoa shipments — deadline T-14 days"),
    ("tariff_shock",      "high",   "Section 301 tariff increase (25%→34%) effective in 30 days — 47 SKUs impacted"),
    ("trade_control_tower","low",   "Monthly trade-posture report ready for CFO review"),
    ("fta_preferential",  "medium", "EU-UK TCA cumulation rules updated — 6 product lines may gain preference"),
    ("compliance_guardian","low",   "ISF filing late for vessel EVER GIVEN II — $5,000 penalty risk"),
    ("duty_recovery",     "medium", "First-sale valuation opportunity identified — potential saving $73,000/year"),
]

_TARIFF_EVENTS = [
    ("tariff_update",    "cleared", "Japan HS 6204.62 tariff rate: 0% → 2.5%", "$1.2M annual impact"),
    ("tariff_update",    "issued",  "EU Anti-Dumping Duty: Stainless Steel 15% effective", "$5.8M exposure"),
    ("tariff_update",    "cleared", "USMCA Rule of Origin cert filed: 847130", "Duty savings $3.2K"),
    ("tariff_update",    "issued",  "China Section 301 list expansion: 45 lines added", "$12.4M impact"),
    ("tariff_update",    "cleared", "India GSP renewal approved through Dec 2025", "16 products included"),
    ("tariff_update",    "issued",  "Vietnam safeguard review initiated: Tableware", "$890K quarterly risk"),
    ("tariff_update",    "cleared", "UK TRQ allocation: Fresh fruits quota filled 78%", "On schedule"),
    ("tariff_update",    "issued",  "Mexico automotive parts dispute: 8% duty possible", "$7.1M exposure"),
    ("tariff_update",    "cleared", "Canada USMCA compliance audit: No issues", "Auto parts cleared"),
    ("tariff_update",    "issued",  "Brazil sugar export tax increase: 4% → 5.5%", "$2.3M impact"),
]

_TARIFF_SOURCES = [
    {"name": "Trade Intelligence API", "status": "Online", "icon": "📡"},
    {"name": "Email Alerts", "status": "Online", "icon": "📧"},
    {"name": "Customs Portal", "status": "Online", "icon": "🏛️"},
    {"name": "Partner Data Feed", "status": "Online", "icon": "🔗"},
    {"name": "Satellite Intelligence", "status": "Online", "icon": "🛰️"},
]

def get_kpis() -> dict:
    return {
        "total_duty_paid_m":      round(random.uniform(4.2, 6.8), 2),
        "fta_capture_rate_pct":   round(random.uniform(61.0, 79.0), 1),
        "drawback_recovered_k":   round(random.uniform(280.0, 420.0), 1),
        "open_compliance_flags":  random.randint(4, 18),
        "active_tariff_alerts":   random.randint(2, 9),
        "value_at_risk_m":        round(random.uniform(1.1, 3.4), 2),
    }

def get_alerts() -> list[dict]:
    now = datetime.utcnow()
    alerts = []
    for i, (agent_id, severity, message) in enumerate(_ALERT_TEMPLATES):
        ts = now - timedelta(hours=random.randint(i, i * 4 + 2), minutes=random.randint(0, 59))
        alerts.append({
            "timestamp": ts.strftime("%Y-%m-%d %H:%M UTC"),
            "agent_id":  agent_id,
            "severity":  severity,
            "message":   message,
        })
    alerts.sort(key=lambda a: a["timestamp"], reverse=True)
    return alerts

def get_tariff_sources() -> list[dict]:
    """Return live source monitoring status for tariff aggregator."""
    return _TARIFF_SOURCES

def get_tariff_feed() -> list[dict]:
    """Return live tariff events feed for the right-side pane."""
    now = datetime.utcnow()
    feed = []
    for i, (event_type, status, headline, detail) in enumerate(_TARIFF_EVENTS):
        # Create realistic timestamps spread across recent hours
        ts = now - timedelta(minutes=random.randint(i * 3, i * 3 + 15))
        feed.append({
            "id": f"tariff_{i}",
            "timestamp": ts.strftime("%H:%M %Z"),
            "time_short": ts.strftime("%H:%M"),
            "headline": headline,
            "detail": detail,
            "status": status,  # "cleared", "issued", "pending"
            "source": random.choice(_TARIFF_SOURCES)["name"],
        })
    feed.sort(key=lambda x: x["timestamp"], reverse=True)
    return feed[:8]  # Return latest 8 events
