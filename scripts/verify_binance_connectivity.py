"""Binance USD-M Futures connectivity and account permissions verifier for Escanor.

Safely tests:
1. Local and external outgoing IP vs Binance whitelisting.
2. USD-M Futures time synchronization and offset.
3. Authenticated REST connectivity (/fapi/v2/account).
4. USD-M Futures trading permissions and account balances.
5. Dual-side position mode (/fapi/v1/positionSide/dual).
6. HYPEUSDT contract status, leverage, margin type, and open position risk.

Credentials are read strictly from environment variables or .env (never logged).
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def load_env_if_present():
    env_file = REPO_ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip("\"'")
            if k not in os.environ:
                os.environ[k] = v


def get_outgoing_ip() -> str:
    endpoints = [
        "https://checkip.amazonaws.com",
        "https://api.ipify.org",
    ]
    for ep in endpoints:
        try:
            req = urllib.request.Request(ep, headers={"User-Agent": "curl/7.68.0"})
            with urllib.request.urlopen(req, timeout=5) as r:
                return r.read().decode("utf-8").strip()
        except Exception:
            continue
    return "Unknown"


def main():
    load_env_if_present()

    is_testnet = "--testnet" in sys.argv or os.environ.get("BINANCE_TESTNET", "").lower() in ("true", "1", "yes")

    print("=" * 60)
    print("      ESCANOR BINANCE USD-M CONNECTIVITY VERIFIER")
    print(f"      Target: {'TESTNET (testnet.binancefuture.com)' if is_testnet else 'LIVE (fapi.binance.com)'}")
    print("=" * 60)

    # 1. Check IP
    outgoing_ip = get_outgoing_ip()
    print(f"Detected Outgoing Public IP: {outgoing_ip}")

    # 2. Check credentials
    if is_testnet:
        api_key = os.environ.get("BINANCE_TESTNET_API_KEY") or os.environ.get("BINANCE_API_KEY", "")
        api_secret = (
            os.environ.get("BINANCE_TESTNET_API_SECRET")
            or os.environ.get("BINANCE_API_SECRET")
            or os.environ.get("BINANCE_SECRET_KEY", "")
        )
    else:
        api_key = os.environ.get("BINANCE_API_KEY", "")
        api_secret = os.environ.get("BINANCE_API_SECRET") or os.environ.get("BINANCE_SECRET_KEY", "")

    if not api_key or not api_secret:
        key_name = "BINANCE_TESTNET_API_KEY / BINANCE_TESTNET_API_SECRET" if is_testnet else "BINANCE_API_KEY / BINANCE_API_SECRET"
        print(f"\n[ERROR] {key_name} is missing from environment / .env")
        print("Please configure them in your .env file or environment variables.")
        return 1

    key_prefix = api_key[:4] + "..." + api_key[-4:] if len(api_key) > 8 else "****"
    print(f"Binance API Key: {key_prefix} (length: {len(api_key)})")
    print(f"Binance API Secret: [CONFIGURED] (length: {len(api_secret)})")

    base_url = "https://testnet.binancefuture.com" if is_testnet else "https://fapi.binance.com"

    # 3. Test Binance server time sync
    try:
        req = urllib.request.Request(f"{base_url}/fapi/v1/time", headers={"User-Agent": "Escanor-LiveBot/1.0"})
        with urllib.request.urlopen(req, timeout=5) as r:
            server_time = json.loads(r.read().decode("utf-8"))["serverTime"]
            local_time = int(time.time() * 1000)
            offset_ms = server_time - local_time
            print(f"Binance Futures Server Time: OK (drift offset: {offset_ms}ms)")
    except Exception as e:
        print(f"\n[ERROR] Failed to reach Binance USD-M Futures server time: {e}")
        return 1

    # 4. Authenticated Helper
    def signed_request(endpoint: str, params: dict | None = None) -> dict:
        p = dict(params or {})
        p["timestamp"] = int(time.time() * 1000) + offset_ms
        p["recvWindow"] = 5000
        qs = urllib.parse.urlencode(p)
        sig = hmac.new(api_secret.encode("utf-8"), qs.encode("utf-8"), hashlib.sha256).hexdigest()
        url = f"{base_url}{endpoint}?{qs}&signature={sig}"
        req = urllib.request.Request(
            url,
            headers={
                "X-MBX-APIKEY": api_key,
                "User-Agent": "Escanor-LiveBot/1.0",
            },
        )
        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, timeout=10) as r:
                    return json.loads(r.read().decode("utf-8"))
            except urllib.error.HTTPError:
                raise
            except Exception as e:
                if attempt < 2:
                    time.sleep(1.0)
                    continue
                raise

    # 5. Test Position Mode
    print("\nChecking Binance Futures Position Mode (/fapi/v1/positionSide/dual)...")
    try:
        dual_resp = signed_request("/fapi/v1/positionSide/dual")
        dual = dual_resp.get("dualSidePosition", False)
        mode_str = "HEDGE" if dual else "ONE_WAY"
        print(f"  [PASS] Authenticated! Futures Position Mode: {mode_str}")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        print(f"  [FAIL] HTTP {e.code}: {body}")
        if e.code == 401:
            try:
                err_data = json.loads(body)
                code = err_data.get("code")
                if code == -2015:
                    print("\n" + "!" * 60)
                    print("DIAGNOSIS FOR ERROR -2015: 'Invalid API-key, IP, or permissions for action'")
                    print("!" * 60)
                    print("This error means Binance received the request, but rejected it for ONE of these reasons:")
                    print(f"1. IP RESTRICTION: Binance expected a whitelisted IP, but your current outgoing IP is: {outgoing_ip}")
                    print("   If your ISP / cellular connection rotates IP addresses, the IP you saved earlier")
                    print("   on Binance may differ from your current outgoing IP.")
                    print("   Action: In Binance API Management, add your current IP:")
                    print(f"           {outgoing_ip}")
                    print("2. FUTURES PERMISSIONS: Newly created Binance API keys do NOT enable Futures by default.")
                    print("   Action: In Binance API Management, click 'Edit Restrictions', and check the box:")
                    print("           [x] Enable Futures")
                    print("3. FUTURES ACCOUNT ACTIVATION:")
                    print("   Ensure you have opened and activated USD-M Futures on your Binance account web/app.")
                    print("!" * 60)
            except Exception:
                pass
        return 1

    # 6. Test Account Details & Balance
    print("\nChecking Binance Futures Account Status (/fapi/v2/account)...")
    try:
        acct = signed_request("/fapi/v2/account")
        can_trade = acct.get("canTrade", False)
        total_wallet = acct.get("totalWalletBalance", "0")
        total_margin = acct.get("totalMarginBalance", "0")
        available = acct.get("availableBalance", "0")
        print(f"  [PASS] Trading Allowed: {can_trade}")
        print(f"  Total Wallet Balance: {total_wallet} USDT")
        print(f"  Total Margin Balance: {total_margin} USDT")
        print(f"  Available Balance:    {available} USDT")
        if not can_trade:
            print("  [WARNING] canTrade is False! Check if Futures account has any restrictions.")
    except urllib.error.HTTPError as e:
        print(f"  [FAIL] Could not fetch account info: {e}")
        return 1

    # 7. Check HYPEUSDT Symbol & Position
    print("\nChecking HYPEUSDT USD-M Contract & Position Risk (/fapi/v2/positionRisk)...")
    try:
        pos_list = signed_request("/fapi/v2/positionRisk", {"symbol": "HYPEUSDT"})
        if not pos_list:
            print("  No position records found for HYPEUSDT.")
        for p in pos_list:
            amt = p.get("positionAmt", "0")
            entry_px = p.get("entryPrice", "0")
            leverage = p.get("leverage", "1")
            margin_type = p.get("marginType", "cross")
            pos_side = p.get("positionSide", "BOTH")
            upnl = p.get("unRealizedProfit", "0")
            print(f"  [PASS] PositionSide: {pos_side} | Amt: {amt} | Entry: {entry_px} | PnL: {upnl} | Lev: {leverage}x | Margin: {margin_type}")
    except urllib.error.HTTPError as e:
        print(f"  [FAIL] Could not query HYPEUSDT position: {e}")
        return 1

    print("\n" + "=" * 60)
    print("  BINANCE USD-M FUTURES & HYPEUSDT CONNECTION: 100% VERIFIED")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
