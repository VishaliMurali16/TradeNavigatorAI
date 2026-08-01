"""
Connector package — one module per tariff data source.

Public API
----------
    ConnectorError   — raised on network/HTTP failure; import from here or base directly
    BaseConnector    — abstract interface all connectors must implement
    USHTSConnector   — USITC HTS REST endpoint (Phase 1, enabled)
    WITSConnector    — World Bank WITS API (Phase 1, enabled)
    EUTARICConnector — EU TARIC stub (Phase 2)
    MacMapConnector  — MacMap stub (Phase 2)
    SAPGTSConnector  — SAP GTS stub (client-licensed, production only)
"""

from aggregator.connectors.base import BaseConnector, ConnectorError
from aggregator.connectors.eu_taric import EUTARICConnector
from aggregator.connectors.macmap import MacMapConnector
from aggregator.connectors.sap_gts import SAPGTSConnector
from aggregator.connectors.us_hts import USHTSConnector
from aggregator.connectors.wits import WITSConnector

__all__ = [
    "BaseConnector",
    "ConnectorError",
    "USHTSConnector",
    "WITSConnector",
    "EUTARICConnector",
    "MacMapConnector",
    "SAPGTSConnector",
]
