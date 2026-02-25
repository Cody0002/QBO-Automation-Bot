from __future__ import annotations

import argparse
import base64
import json
import os
import secrets
import sys
import urllib.parse
import webbrowser
from pathlib import Path

import requests
from dotenv import load_dotenv

from config import settings
from src.connectors.gsheets_client import GSheetsClient


AUTH_BASE_URL = "https://appcenter.intuit.com/connect/oauth2"


def _mask(value: str, keep: int = 6) -> str:
    if not value:
        return ""
    if len(value) <= keep * 2:
        return "*" * len(value)
    return f"{value[:keep]}...{value[-keep:]}"


def _build_auth_url(client_id: str, redirect_uri: str, scope: str, state: str) -> str:
    query = urllib.parse.urlencode(
        {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": scope,
            "state": state,
        }
    )
    return f"{AUTH_BASE_URL}?{query}"


def _parse_callback_url(callback_url: str) -> dict[str, str]:
    parsed = urllib.parse.urlparse(callback_url.strip())
    query = urllib.parse.parse_qs(parsed.query)
    return {k: (v[0] if v else "") for k, v in query.items()}


def _exchange_code_for_tokens(
    client_id: str, client_secret: str, redirect_uri: str, code: str
) -> dict:
    auth_str = f"{client_id}:{client_secret}"
    basic = base64.b64encode(auth_str.encode()).decode()

    headers = {
        "Authorization": f"Basic {basic}",
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
    }

    resp = requests.post(settings.QBO_TOKEN_URL, headers=headers, data=data, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"Token exchange failed [{resp.status_code}]: {resp.text}")
    return resp.json()


def _upsert_master_row(
    gs: GSheetsClient,
    realm_id: str,
    refresh_token: str,
    client_name: str | None,
    spreadsheet_id: str | None,
    status: str,
) -> tuple[str, int]:
    sh = gs.open(settings.MASTER_SHEET_ID)
    ws = sh.worksheet(settings.MASTER_TAB_NAME)

    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("Master Sheet tab has no headers in row 1.")

    required = [
        settings.MST_COL_CLIENT,
        settings.MST_COL_SHEET_ID,
        settings.MST_COL_REALM_ID,
        settings.MST_COL_STATUS,
        settings.MST_COL_REFRESH_TOKEN,
    ]

    missing = [h for h in required if h not in headers]
    if missing:
        raise RuntimeError(f"Missing required Master Sheet columns: {missing}")

    col_idx = {h: headers.index(h) + 1 for h in headers}
    all_values = ws.get_all_values()

    target_row = None
    realm_col = col_idx[settings.MST_COL_REALM_ID] - 1
    for i, row in enumerate(all_values[1:], start=2):
        val = row[realm_col].strip() if realm_col < len(row) else ""
        if val == realm_id:
            target_row = i
            break

    if target_row is None:
        target_row = len(all_values) + 1
        row_payload = [""] * len(headers)
        if client_name:
            row_payload[col_idx[settings.MST_COL_CLIENT] - 1] = client_name
        if spreadsheet_id:
            row_payload[col_idx[settings.MST_COL_SHEET_ID] - 1] = spreadsheet_id
        row_payload[col_idx[settings.MST_COL_REALM_ID] - 1] = realm_id
        row_payload[col_idx[settings.MST_COL_STATUS] - 1] = status
        row_payload[col_idx[settings.MST_COL_REFRESH_TOKEN] - 1] = refresh_token
        ws.append_row(row_payload, value_input_option="USER_ENTERED")
        return ("created", target_row)

    updates = []
    if client_name:
        updates.append((target_row, col_idx[settings.MST_COL_CLIENT], client_name))
    if spreadsheet_id:
        updates.append((target_row, col_idx[settings.MST_COL_SHEET_ID], spreadsheet_id))
    updates.extend(
        [
            (target_row, col_idx[settings.MST_COL_REALM_ID], realm_id),
            (target_row, col_idx[settings.MST_COL_STATUS], status),
            (target_row, col_idx[settings.MST_COL_REFRESH_TOKEN], refresh_token),
        ]
    )
    for r, c, v in updates:
        ws.update_cell(r, c, v)
    return ("updated", target_row)


def main() -> int:
    load_dotenv("config/secrets.env")

    parser = argparse.ArgumentParser(
        description="Initial QBO setup: authorize app, get realmId + refresh token, optionally save to Master Sheet."
    )
    parser.add_argument(
        "--redirect-uri",
        default=os.getenv("QBO_REDIRECT_URI", "http://localhost:8000/callback"),
        help="Redirect URI configured in Intuit developer app.",
    )
    parser.add_argument(
        "--scope",
        default=os.getenv("QBO_OAUTH_SCOPE", "com.intuit.quickbooks.accounting"),
        help="OAuth scope (default: com.intuit.quickbooks.accounting).",
    )
    parser.add_argument("--no-browser", action="store_true", help="Do not auto-open browser.")
    parser.add_argument(
        "--callback-url",
        default="",
        help="Full callback URL. If omitted, script prompts for it.",
    )
    parser.add_argument(
        "--write-token-file",
        default="",
        help="Optional JSON file path to store full token payload.",
    )
    parser.add_argument(
        "--save-master",
        action="store_true",
        help="Save/Update Refresh Token in Master Sheet row by Realm ID.",
    )
    parser.add_argument("--client-name", default="", help="Client Name to write when saving to Master Sheet.")
    parser.add_argument(
        "--spreadsheet-id",
        default="",
        help="Client Spreadsheet ID to write when saving to Master Sheet.",
    )
    parser.add_argument("--status", default="Active", help="Status value for Master Sheet (default: Active).")
    args = parser.parse_args()

    client_id = os.getenv("QBO_CLIENT_ID", "").strip()
    client_secret = os.getenv("QBO_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        print("Missing QBO_CLIENT_ID / QBO_CLIENT_SECRET in config/secrets.env")
        return 1

    state = secrets.token_urlsafe(24)
    auth_url = _build_auth_url(client_id, args.redirect_uri, args.scope, state)

    print("\n1) Open this URL and complete authorization:")
    print(auth_url)
    if not args.no_browser:
        try:
            webbrowser.open(auth_url, new=2)
        except Exception:
            pass

    callback_url = args.callback_url.strip()
    if not callback_url:
        print("\n2) Paste the FULL callback URL you were redirected to:")
        callback_url = input("> ").strip()

    if not callback_url:
        print("No callback URL provided.")
        return 1

    params = _parse_callback_url(callback_url)

    if params.get("state") != state:
        print("State mismatch. Canceling for safety.")
        return 1

    if "error" in params:
        print(f"OAuth error: {params.get('error')} | {params.get('error_description', '')}")
        return 1

    code = params.get("code", "")
    realm_id = params.get("realmId", "")
    if not code:
        print("Missing 'code' in callback URL.")
        return 1
    if not realm_id:
        print("Missing 'realmId' in callback URL.")
        return 1

    token_payload = _exchange_code_for_tokens(client_id, client_secret, args.redirect_uri, code)
    refresh_token = token_payload.get("refresh_token", "")
    access_token = token_payload.get("access_token", "")
    if not refresh_token:
        print("Token exchange succeeded but refresh_token is missing.")
        return 1

    print("\n3) OAuth success")
    print(f"Realm ID      : {realm_id}")
    print(f"Refresh Token : {_mask(refresh_token)}")
    print(f"Access Token  : {_mask(access_token)}")
    print(f"Expires In    : {token_payload.get('expires_in')}")
    print(f"Refresh TTL   : {token_payload.get('x_refresh_token_expires_in')}")

    if args.write_token_file:
        out_path = Path(args.write_token_file)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(token_payload, indent=2), encoding="utf-8")
        print(f"Token payload written to: {out_path}")

    if args.save_master:
        gs = GSheetsClient()
        action, row_num = _upsert_master_row(
            gs=gs,
            realm_id=realm_id,
            refresh_token=refresh_token,
            client_name=args.client_name.strip() or None,
            spreadsheet_id=args.spreadsheet_id.strip() or None,
            status=args.status.strip() or "Active",
        )
        print(f"Master Sheet {action} at row {row_num}.")
    else:
        print(
            "\nNot saved to Master Sheet. Use --save-master if you want this script to write Refresh Token automatically."
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
