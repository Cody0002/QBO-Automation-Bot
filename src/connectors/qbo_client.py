from __future__ import annotations

import base64
import os
import time
import urllib.parse
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests

from config import settings

# --- HELPER TO UPDATE .ENV FILE ---
def update_env_file(key: str, new_value: str, env_path: str = "config/secrets.env"):
    try:
        with open(env_path, "r") as f:
            lines = f.readlines()
        new_lines = []
        key_found = False
        for line in lines:
            if line.strip().startswith(f"{key}="):
                new_lines.append(f"{key}={new_value}\n")
                key_found = True
            else:
                new_lines.append(line)
        if not key_found:
            new_lines.append(f"\n{key}={new_value}\n")
        with open(env_path, "w") as f:
            f.writelines(new_lines)
    except Exception as e:
        print(f"⚠️ Failed to auto-update .env file: {e}")

@dataclass
class QBOConfig:
    client_id: str
    client_secret: str
    realm_id: str
    refresh_token: str

class QBOClient:
    """QuickBooks Online client (OAuth refresh + Query + POST helpers)."""

    def __init__(self, cfg: Optional[QBOConfig] = None):
        if cfg is None:
            cfg = QBOConfig(
                client_id=os.getenv("QBO_CLIENT_ID", ""),
                client_secret=os.getenv("QBO_CLIENT_SECRET", ""),
                realm_id=os.getenv("QBO_REALM_ID", ""),
                refresh_token=os.getenv("QBO_REFRESH_TOKEN", ""),
            )
        self.cfg = cfg
        self._access_token: Optional[str] = None
        self._access_token_expiry_epoch: float = 0.0

    def _basic_auth_header(self) -> str:
        auth_str = f"{self.cfg.client_id}:{self.cfg.client_secret}"
        return base64.b64encode(auth_str.encode()).decode()

    def refresh_access_token(self) -> str:
        headers = {
            "Authorization": f"Basic {self._basic_auth_header()}",
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {"grant_type": "refresh_token", "refresh_token": self.cfg.refresh_token}
        
        resp = requests.post(settings.QBO_TOKEN_URL, headers=headers, data=data, timeout=60)
        if resp.status_code == 400:
            print("❌ QBO Error: Refresh Token Invalid.")
        resp.raise_for_status()
        payload = resp.json()

        self._access_token = payload["access_token"]
        expires_in = int(payload.get("expires_in", 3600))
        self._access_token_expiry_epoch = time.time() + max(60, expires_in - 120)

        new_refresh_token = payload.get("refresh_token")
        if new_refresh_token and new_refresh_token != self.cfg.refresh_token:
            self.cfg.refresh_token = new_refresh_token
            update_env_file("QBO_REFRESH_TOKEN", new_refresh_token)

        return self._access_token

    def get_access_token(self) -> str:
        if not self._access_token or time.time() >= self._access_token_expiry_epoch:
            return self.refresh_access_token()
        return self._access_token

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.get_access_token()}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _url(self, path: str) -> str:
        return f"{settings.QBO_BASE_URL}{path}"

    def _get(self, path: str) -> Dict[str, Any]:
        url = self._url(path)
        resp = requests.get(url, headers=self._headers(), timeout=60)
        resp.raise_for_status()
        return resp.json()

    def query(self, select_statement: str) -> list[Dict[str, Any]]:
        all_results = []
        start_position = 1
        max_results = 1000

        while True:
            paged_query = f"{select_statement} STARTPOSITION {start_position} MAXRESULTS {max_results}"
            encoded_query = urllib.parse.quote(paged_query)
            path = f"/v3/company/{self.cfg.realm_id}/query?query={encoded_query}&minorversion={settings.QBO_MINOR_VERSION}"
            
            data = self._get(path)
            query_response = data.get("QueryResponse", {})
            entities = []
            for key, value in query_response.items():
                if isinstance(value, list): entities.extend(value)
            
            if not entities: break
            all_results.extend(entities)
            start_position += len(entities)
            if len(entities) < max_results: break
                
        return all_results

    def post(self, path: str, json_body: Dict[str, Any]) -> Dict[str, Any]:
        url = self._url(path)
        resp = requests.post(url, headers=self._headers(), json=json_body, timeout=60)
        
        # --- NEW: DETAILED ERROR REPORTING ---
        if resp.status_code >= 400:
            print(f"❌ QBO API ERROR ({resp.status_code}): {resp.text}")
            
        resp.raise_for_status()
        return resp.json()

    def get_max_journal_number(self, prefix: str) -> int:
        query = f"SELECT DocNumber FROM JournalEntry WHERE DocNumber LIKE '{prefix}%' ORDER BY DocNumber DESC MAXRESULTS 1"
        try:
            results = self.query(query)
            if not results: return 0
            doc_num = str(results[0].get("DocNumber", ""))
            if doc_num.startswith(prefix):
                suffix = doc_num[len(prefix):]
                clean_suffix = "".join(filter(str.isdigit, suffix))
                return int(clean_suffix) if clean_suffix else 0
            return 0
        except Exception as e:
            print(f"⚠️ Failed to fetch max journal number: {e}")
            return 0