#!/usr/bin/env python3
"""
Licensed SKU Daily Report
Runs weekdays via GitHub Actions. Queries Plytix for new licensed SKUs,
applies smart detection for unlabeled licensed products, and sends a
Slack DM summary to Lauren Patterson.

Lookback logic:
  - Monday: 3 days (catches Fri/Sat/Sun)
  - Tuesday–Friday: 1 day (catches previous day)
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone

import requests

# ──────────────────────────── CONFIG ────────────────────────────

PLYTIX_API_KEY = os.environ["PLYTIX_API_KEY"]
PLYTIX_API_PASSWORD = os.environ["PLYTIX_API_PASSWORD"]
PLYTIX_AUTH_URL = "https://auth.plytix.com/auth/api/get-token"
PLYTIX_BASE_URL = "https://pim.plytix.com"

SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_LAUREN_USER_ID = "U05AL9QQHT8"

# Central Time
CT = timezone(timedelta(hours=-5))   # CDT (Mar–Nov)
# Note: GitHub Actions cron handles the schedule, so we just need CT for
# date math. CDT is UTC-5, CST is UTC-6. April–November is CDT.
# If this ever matters in winter, swap to -6 or use pytz/zoneinfo.

# Licensed label patterns
LICENSED_PREFIXES = [
    "Disney-", "Marvel-", "Sesame Street-", "Hasbro-",
    "NHL-", "Collegiate-", "Farm Build-Collegiate-",
]
LICENSED_CONTAINS = ["NFL"]

PAGE_SIZE = 100


# ──────────────────────────── PLYTIX AUTH ────────────────────────────

def get_plytix_token():
    """Exchange API key + password for a bearer token."""
    resp = requests.post(PLYTIX_AUTH_URL, json={
        "api_key": PLYTIX_API_KEY,
        "api_password": PLYTIX_API_PASSWORD,
    })
    resp.raise_for_status()
    data = resp.json()
    return data["data"][0]["access_token"]


def plytix_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


# ──────────────────────────── PLYTIX SEARCH ────────────────────────────

def search_products(token, filters, attributes, page=1):
    """Search Plytix products. Returns (products_list, pagination_dict)."""
    body = {
        "filters": filters,
        "attributes": attributes,
        "sort": [{"field": "created", "order": "desc"}],
        "pagination": {"page": page, "page_size": PAGE_SIZE},
    }
    resp = requests.post(
        f"{PLYTIX_BASE_URL}/api/v2/products/search",
        headers=plytix_headers(token),
        json=body,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("products", data.get("data", [])), data.get("pagination", {})


def collect_products_in_window(token, filters, attributes, utc_start, utc_end):
    """
    Paginate through sorted-desc results, collecting products created
    within [utc_start, utc_end). Stop early once we pass the window.
    """
    collected = []
    page = 1
    while True:
        products, pagination = search_products(token, filters, attributes, page)
        if not products:
            break

        found_before_window = False
        for p in products:
            created_str = p.get("created", "")
            if not created_str:
                continue
            created_dt = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
            if created_dt < utc_start:
                found_before_window = True
                break
            if utc_start <= created_dt < utc_end:
                collected.append(p)

        if found_before_window:
            break

        total_pages = pagination.get("total_pages",
                      pagination.get("pages",
                      (pagination.get("count", 0) + PAGE_SIZE - 1) // PAGE_SIZE))
        if page >= total_pages:
            break
        page += 1

    return collected


# ──────────────────────────── SMART DETECTION ────────────────────────────

def check_label_licensed(label):
    """Method A: Check label against known licensed patterns."""
    if not label:
        return None
    for prefix in LICENSED_PREFIXES:
        if label.startswith(prefix):
            return f"Label prefix: {prefix}"
    for term in LICENSED_CONTAINS:
        if term in label:
            return f"Label contains: {term}"
    return None


def check_ornamentation_licensed(token, ornamentation, confirmed_ornamentations):
    """
    Method B: Check if ornamentation matches a known licensed product.
    First check against the confirmed list, then query Plytix.
    """
    if not ornamentation:
        return None

    # Check against confirmed licensed ornamentations from this run
    if ornamentation in confirmed_ornamentations:
        return f"Ornamentation match: {ornamentation} (known licensed)"

    # Query Plytix for any existing product with this ornamentation + licensor
    products, _ = search_products(
        token,
        filters=[
            [{"field": "attributes.ornamentation_family", "operator": "eq", "value": ornamentation}],
            [{"field": "attributes.licensor", "operator": "exists"}],
        ],
        attributes=["sku", "attributes.licensor", "attributes.licensing_organization"],
        page=1,
    )
    if products:
        match = products[0]
        licensor = match.get("attributes", {}).get("licensor", "unknown")
        return f"Ornamentation match: {ornamentation} (existing SKU {match.get('sku')} has licensor: {licensor})"

    return None


# ──────────────────────────── DATE WINDOW ────────────────────────────

def get_lookback_window():
    """
    Calculate the UTC time window for the lookback period.
    Monday: 3 days back (Fri/Sat/Sun). Tue-Fri: 1 day back.
    Returns (utc_start, utc_end, display_string).
    """
    now_ct = datetime.now(CT)
    today_ct = now_ct.replace(hour=0, minute=0, second=0, microsecond=0)

    weekday = today_ct.weekday()  # 0=Mon, 4=Fri
    if weekday == 0:  # Monday
        lookback_days = 3
    else:
        lookback_days = 1

    start_ct = today_ct - timedelta(days=lookback_days)
    end_ct = today_ct

    utc_start = start_ct.astimezone(timezone.utc)
    utc_end = end_ct.astimezone(timezone.utc)

    if lookback_days == 1:
        display = start_ct.strftime("%B %d, %Y")
    else:
        display = f"{start_ct.strftime('%B %d')} – {(end_ct - timedelta(days=1)).strftime('%B %d, %Y')}"

    return utc_start, utc_end, display


# ──────────────────────────── SLACK ────────────────────────────

def send_slack_dm(message):
    """Send a Slack DM to Lauren Patterson."""
    resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={
            "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
            "Content-Type": "application/json",
        },
        json={
            "channel": SLACK_LAUREN_USER_ID,
            "text": message,
        },
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"Slack API error: {data.get('error', 'unknown')}")
    print(f"Slack message sent: {data.get('message', {}).get('ts', 'ok')}")


# ──────────────────────────── MESSAGE FORMATTING ────────────────────────────

def format_message(display_date, confirmed, possibly_licensed):
    """Build the Slack message."""
    lines = [f":label: *Licensed SKU Daily Report — {display_date}*", ""]

    if not confirmed and not possibly_licensed:
        lines.append(f"*New Licensed SKUs Created: 0*")
        lines.append("No new licensed SKUs were created. All clear!")
        lines.append("")
        lines.append("_This report runs weekdays at 8:00 AM CT. Set up by Rachel Toews._")
        return "\n".join(lines)

    # Confirmed licensed
    lines.append(f"*New Licensed SKUs Created: {len(confirmed)}*")
    lines.append("")
    for p in confirmed:
        attrs = p.get("attributes", {})
        sku = p.get("sku", "?")
        config = attrs.get("configuration", "—")
        orn = attrs.get("ornamentation_family", "—")
        licensor = attrs.get("licensor", "—")
        org = attrs.get("licensing_organization", "—")
        lines.append(
            f":small_blue_diamond: *{sku}* — {config} — {orn} — Licensor: {licensor} — Org: {org}"
        )

    lines.append("")

    # Possibly licensed
    if possibly_licensed:
        lines.append(f":warning: *Possibly Licensed SKUs (no licensor set): {len(possibly_licensed)}*")
        lines.append("_These products may need licensing info added:_")
        for item in possibly_licensed:
            lines.append(
                f":small_orange_diamond: *{item['sku']}* — {item['label']} — _Detected: {item['reason']}_"
            )
    else:
        lines.append("_No potentially licensed products detected via smart detection._")

    lines.append("")
    lines.append("_This report runs weekdays at 8:00 AM CT. Set up by Rachel Toews._")
    return "\n".join(lines)


# ──────────────────────────── MAIN ────────────────────────────

def main():
    print("Licensed SKU Daily Report — starting")

    # 1. Calculate lookback window
    utc_start, utc_end, display_date = get_lookback_window()
    print(f"Lookback window: {utc_start} to {utc_end} ({display_date})")

    # 2. Authenticate with Plytix
    token = get_plytix_token()
    print("Plytix auth OK")

    # 3. Find confirmed licensed products (licensor exists)
    confirmed = collect_products_in_window(
        token,
        filters=[[{"field": "attributes.licensor", "operator": "exists"}]],
        attributes=[
            "sku", "label", "attributes.licensor", "attributes.licensing_organization",
            "attributes.ornamentation_family", "attributes.configuration", "created",
        ],
        utc_start=utc_start,
        utc_end=utc_end,
    )
    print(f"Confirmed licensed SKUs in window: {len(confirmed)}")

    # Build set of ornamentations from confirmed products
    confirmed_ornamentations = set()
    for p in confirmed:
        orn = p.get("attributes", {}).get("ornamentation_family")
        if orn:
            confirmed_ornamentations.add(orn)

    # 4. Find unlicensed products created in window (smart detection)
    unlicensed = collect_products_in_window(
        token,
        filters=[[{"field": "attributes.licensor", "operator": "!exists"}]],
        attributes=[
            "sku", "label", "attributes.ornamentation_family",
            "attributes.configuration", "created",
        ],
        utc_start=utc_start,
        utc_end=utc_end,
    )
    print(f"Unlicensed SKUs in window (to check): {len(unlicensed)}")

    # 5. Smart detection
    possibly_licensed = []
    for p in unlicensed:
        label = p.get("label", "")
        sku = p.get("sku", "")
        orn = p.get("attributes", {}).get("ornamentation_family")

        # Method A: label match
        reason = check_label_licensed(label)

        # Method B: ornamentation match
        if not reason:
            reason = check_ornamentation_licensed(token, orn, confirmed_ornamentations)

        if reason:
            possibly_licensed.append({"sku": sku, "label": label, "reason": reason})

    print(f"Possibly licensed (smart detection): {len(possibly_licensed)}")

    # 6. Format and send
    message = format_message(display_date, confirmed, possibly_licensed)
    print(f"\n--- Message preview ---\n{message}\n--- End preview ---\n")
    send_slack_dm(message)
    print("Done!")


if __name__ == "__main__":
    main()
