#!/usr/bin/env python3
"""
The Barringer Brief — GitHub Actions Version
Runs via GitHub Actions cron every morning at 7:00 AM EDT (11:00 UTC).
No macOS dependencies — pure Python stdlib.
Fetches: weather (wttr.in), METAR (aviationweather.gov), markets (Yahoo Finance),
         calendar (Google Calendar iCal URL via GCAL_ICAL_URL secret).
Sends via Resend API directly.
"""

import json, datetime, urllib.request, urllib.error, sys, re, os, email.utils

# ── CONFIG ────────────────────────────────────────────────────────────────────
RECIPIENT      = "jadie2@mac.com"
FROM_ADDRESS   = "The Barringer Brief <brief@thebarringerbrief.com>"
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
GCAL_ICAL_URL  = os.environ.get("GCAL_ICAL_URL", "")   # optional — Google Calendar secret iCal URL
LIVE_URL       = "https://thebarringerbrief.com"

def log(msg):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

# ── WEATHER ───────────────────────────────────────────────────────────────────
def fetch_weather():
    try:
        req = urllib.request.Request(
            "https://wttr.in/Weston,FL?format=j1",
            headers={"User-Agent": "BarringerBrief/1.0"}
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
        cur   = data["current_condition"][0]
        today = data["weather"][0]
        return {
            "temp_f":     cur["temp_F"],
            "feels_like": cur["FeelsLikeF"],
            "desc":       cur["weatherDesc"][0]["value"].strip(),
            "wind_mph":   cur["windspeedMiles"],
            "wind_dir":   cur["winddir16Point"],
            "humidity":   cur["humidity"],
            "uv":         cur["uvIndex"],
            "high":       today["maxtempF"],
            "low":        today["mintempF"],
        }
    except Exception as e:
        log(f"[WEATHER ERROR] {e}")
        return None

# ── METAR ─────────────────────────────────────────────────────────────────────
def fetch_metar():
    try:
        req = urllib.request.Request(
            "https://aviationweather.gov/api/data/metar?ids=KFLL,KFXE,KPMP&format=json",
            headers={"User-Agent": "BarringerBrief/1.0"}
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
        results = {}
        for s in data:
            results[s["icaoId"]] = {
                "cat":   s.get("fltCat", "VFR"),
                "temp":  round(s["temp"]) if s.get("temp") is not None else "--",
                "cover": s.get("cover", "CLR"),
                "wspd":  s.get("wspd", 0),
                "wdir":  s.get("wdir", 0),
            }
        return results
    except Exception as e:
        log(f"[METAR ERROR] {e}")
        return {}

# ── MARKETS (Yahoo Finance — no API key needed) ───────────────────────────────
def fetch_markets():
    tickers = ["SPY", "QQQ", "NVDA", "AAPL", "BTC-USD", "CL=F"]
    markets = {}
    for sym in tickers:
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=1d"
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json"
            })
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read())
            meta  = data["chart"]["result"][0]["meta"]
            price = meta.get("regularMarketPrice", 0)
            prev  = meta.get("chartPreviousClose", price)
            chg   = price - prev
            pct   = (chg / prev * 100) if prev else 0
            if price > 10000:
                price_str = f"{price:,.0f}"
            else:
                price_str = f"{price:.2f}"
            markets[sym] = {
                "price":  price_str,
                "change": f"{chg:+.2f}",
                "pct":    f"{pct:.2f}",
            }
        except Exception as e:
            log(f"[MARKET ERROR] {sym}: {e}")
    return markets

# ── CALENDAR (Google Calendar iCal) ──────────────────────────────────────────
def fetch_calendar():
    """
    Fetch next 7 days of events from a Google Calendar secret iCal URL.
    Set the GCAL_ICAL_URL GitHub secret to enable this.
    Falls back gracefully if not configured.
    """
    if not GCAL_ICAL_URL:
        log("[CALENDAR] GCAL_ICAL_URL not set — skipping calendar")
        return []
    try:
        req = urllib.request.Request(
            GCAL_ICAL_URL,
            headers={"User-Agent": "BarringerBrief/1.0"}
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            raw = r.read().decode("utf-8", errors="replace")
        return _parse_ical(raw)
    except Exception as e:
        log(f"[CALENDAR ERROR] {e}")
        return []

def _parse_ical(raw):
    """Minimal iCal parser — extracts VEVENT blocks for the next 7 days."""
    now   = datetime.datetime.now(datetime.timezone.utc)
    end   = now + datetime.timedelta(days=7)
    events = []

    # Split into VEVENT blocks
    blocks = re.findall(r"BEGIN:VEVENT(.*?)END:VEVENT", raw, re.DOTALL)
    for block in blocks:
        def get(key):
            m = re.search(rf"^{key}[;:][^\r\n]*", block, re.MULTILINE)
            if not m: return ""
            # Strip the key: prefix, handle DTSTART;TZID=... forms
            val = m.group(0)
            val = re.sub(rf"^{key}[^:]*:", "", val)
            return val.strip()

        summary  = _unescape(get("SUMMARY"))
        location = _unescape(get("LOCATION"))
        dtstart  = get("DTSTART")

        if not dtstart or not summary:
            continue

        dt = _parse_dt(dtstart)
        if dt is None:
            continue

        # Normalise to UTC for comparison
        if dt.tzinfo is None:
            dt_utc = dt.replace(tzinfo=datetime.timezone.utc)
        else:
            dt_utc = dt.astimezone(datetime.timezone.utc)

        if now <= dt_utc <= end:
            # Convert to Eastern for display
            edt_offset = datetime.timezone(datetime.timedelta(hours=-4))  # EDT
            dt_edt = dt_utc.astimezone(edt_offset)
            time_str = dt_edt.strftime("%A, %b %-d · %-I:%M %p EDT")
            events.append({
                "title":     summary,
                "time":      time_str,
                "location":  location,
                "attendees": [],
            })

    # Sort chronologically
    def sort_key(e):
        try:
            parts = e["time"].split(" · ")
            return parts[1] if len(parts) > 1 else ""
        except:
            return ""

    events.sort(key=lambda e: e["time"])
    # Deduplicate
    seen, unique = set(), []
    for e in events:
        key = e["title"] + e["time"]
        if key not in seen:
            seen.add(key)
            unique.append(e)
    return unique[:8]

def _unescape(s):
    return s.replace("\\n", "\n").replace("\\,", ",").replace("\\;", ";").replace("\\\\", "\\")

def _parse_dt(s):
    """Parse iCal DTSTART value — handles DATE-TIME and DATE formats."""
    s = s.strip()
    # Strip TZID prefix if present (already extracted value only)
    try:
        if "T" in s:
            s = s.rstrip("Z")
            return datetime.datetime.strptime(s[:15], "%Y%m%dT%H%M%S")
        else:
            d = datetime.datetime.strptime(s[:8], "%Y%m%d")
            return d.replace(hour=0, minute=0, second=0)
    except:
        return None

# ── WEATHER ICON ─────────────────────────────────────────────────────────────
def weather_icon(desc):
    d = (desc or "").lower()
    if any(x in d for x in ['thunder','storm']): return '⛈️'
    if any(x in d for x in ['snow','blizzard','flurr']): return '❄️'
    if any(x in d for x in ['rain','shower','drizzle']): return '🌧️'
    if any(x in d for x in ['fog','mist','haze']): return '🌫️'
    if any(x in d for x in ['partly cloudy','partly']): return '⛅️'
    if any(x in d for x in ['mostly cloudy','cloudy','overcast']): return '☁️'
    if any(x in d for x in ['clear','sunny','sun']): return '☀️'
    if 'wind' in d: return '💨'
    return '🌤️'

# ── METAR HTML ────────────────────────────────────────────────────────────────
def metar_rows_html(metar):
    rows = ""
    for apt in ["KFLL","KFXE","KPMP"]:
        m = metar.get(apt)
        if not m: continue
        cat = m["cat"]
        cat_color = {"VFR":"#34C759","MVFR":"#007AFF","IFR":"#CC0000","LIFR":"#5856D6"}.get(cat,"#8A8A8E")
        rows += f"""<tr>
          <td style="font-family:'Courier New',monospace;font-size:11px;font-weight:700;color:#1D1D1F;padding:5px 14px 5px 0;width:48px;">{apt}</td>
          <td style="font-family:'Courier New',monospace;font-size:10px;color:#3A3A3C;padding:5px 14px 5px 0;">{m['temp']}°C &nbsp;{m['cover']} &nbsp;{m['wdir']}/{m['wspd']}kt</td>
          <td style="padding:5px 0;"><span style="font-family:Arial,sans-serif;font-size:9px;font-weight:700;color:{cat_color};">● {cat}</span></td>
        </tr>"""
    return rows or "<tr><td style='font-size:11px;color:#8A8A8E;'>METAR unavailable</td></tr>"

# ── BUILD EMAIL ───────────────────────────────────────────────────────────────
def build_email_html(weather, metar, markets, calendar_events, date_str):

    # Weather block
    if weather:
        wx_html = f"""
        <td style="vertical-align:top;padding-right:24px;border-right:1px solid #E5E5EA;">
          <div style="font-family:Georgia,serif;font-size:52px;font-weight:200;color:#1D1D1F;line-height:1;letter-spacing:-2px;">{weather['temp_f']}<span style="font-size:16px;color:#8A8A8E;font-weight:300;">°F</span></div>
          <div style="font-family:Arial,sans-serif;font-size:13px;font-weight:600;color:#1D1D1F;margin-top:6px;">{weather_icon(weather['desc'])} {weather['desc']}</div>
          <div style="font-family:'Courier New',monospace;font-size:9px;color:#3A3A3C;margin-top:6px;line-height:2.0;">
            Hi {weather['high']}° / Lo {weather['low']}° &nbsp;·&nbsp; UV {weather['uv']}<br>
            Wind {weather['wind_mph']} mph {weather['wind_dir']}<br>
            Feels like {weather['feels_like']}°F
          </div>
          <div style="font-family:'Courier New',monospace;font-size:8px;color:#8A8A8E;margin-top:8px;letter-spacing:0.14em;text-transform:uppercase;">Weston, FL</div>
        </td>"""
    else:
        wx_html = "<td style='color:#8A8A8E;font-size:12px;padding-right:24px;border-right:1px solid #E5E5EA;'>Weather unavailable</td>"

    # Markets
    tickers = [("SPY","SPY"),("QQQ","QQQ"),("NVDA","NVDA"),("AAPL","AAPL"),("BTC-USD","BTC"),("CL=F","OIL")]
    mkt_cells = ""
    for sym, label in tickers:
        m = markets.get(sym, {})
        price = m.get("price","--")
        pct   = m.get("pct","")
        try:
            pf    = float(str(pct).replace("%","").replace("+",""))
            color = "#34C759" if pf >= 0 else "#CC0000"
            arr   = "↑" if pf >= 0 else "↓"
            ps    = f"{arr}{abs(pf):.2f}%"
        except:
            color, ps = "#8A8A8E", "--"
        mkt_cells += f"""<td align="center" style="padding:0 8px;">
          <div style="font-family:'Courier New',monospace;font-size:8px;color:#8A8A8E;letter-spacing:0.08em;margin-bottom:4px;">{label}</div>
          <div style="font-family:Arial,sans-serif;font-size:15px;font-weight:700;color:#1D1D1F;letter-spacing:-0.5px;">${price}</div>
          <div style="font-family:Arial,sans-serif;font-size:10px;font-weight:600;color:{color};margin-top:2px;">{ps}</div>
        </td>"""

    # Calendar
    if calendar_events:
        cal_rows = ""
        for ev in calendar_events:
            att  = ev.get("attendees", [])
            loc  = ev.get("location","")
            parts = ev["time"].split(" · ") if " · " in ev["time"] else [ev["time"], ""]
            day_part  = parts[0]
            time_part = parts[1] if len(parts) > 1 else ""
            day_abbr  = day_part[:3].upper() if day_part else ""
            day_num   = day_part.split()[-1] if day_part else ""
            att_html  = f"<div style='font-family:Arial,sans-serif;font-size:10px;color:#8A8A8E;margin-top:2px;'>👥 {' &nbsp;·&nbsp; '.join(att)}</div>" if att else ""
            loc_part  = f"  ·  📍 {loc}" if loc else ""
            cal_rows += f"""
            <tr><td style="padding:14px 0;border-bottom:1px solid #E5E5EA;">
              <table width="100%" cellpadding="0" cellspacing="0"><tr>
                <td width="52" valign="top" style="padding-right:16px;">
                  <div style="width:44px;border-radius:10px;overflow:hidden;border:1px solid #E5E5EA;box-shadow:0 1px 3px rgba(0,0,0,0.10);">
                    <div style="background:#CC0000;padding:3px 4px;text-align:center;">
                      <div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.08em;color:#fff;text-transform:uppercase;">{day_abbr}</div>
                    </div>
                    <div style="background:#fff;padding:4px 4px 5px;text-align:center;">
                      <div style="font-family:Arial,sans-serif;font-size:22px;font-weight:200;color:#1D1D1F;line-height:1;">{day_num}</div>
                    </div>
                  </div>
                </td>
                <td valign="top">
                  <div style="font-family:Georgia,serif;font-size:14px;font-weight:700;color:#1D1D1F;line-height:1.3;margin-bottom:4px;">{ev['title']}</div>
                  <div style="font-family:Arial,sans-serif;font-size:11px;color:#3A3A3C;margin-bottom:2px;">🕐 {time_part}{loc_part}</div>
                  {att_html}
                </td>
              </tr></table>
            </td></tr>"""
        cal_html = f'<table width="100%" cellpadding="0" cellspacing="0">{cal_rows}</table>'
    else:
        cal_html = "<div style='font-family:Arial,sans-serif;font-size:12px;color:#8A8A8E;padding:8px 0;'>No events scheduled this week.</div>"

    def section(num, tag, tag_color, headline, deck, rows_html):
        return f"""
  <tr><td style="height:2px;background:#F5F5F7;"></td></tr>
  <tr><td style="background:#fff;border:1px solid #E5E5EA;padding:28px 28px 24px;">
    <table cellpadding="0" cellspacing="0" style="margin-bottom:18px;"><tr>
      <td style="font-family:Georgia,serif;font-size:56px;font-weight:900;color:{tag_color};line-height:1;opacity:0.30;padding-right:12px;vertical-align:middle;">{num:02d}</td>
      <td style="vertical-align:middle;border-left:2px solid {tag_color};padding-left:10px;">
        <div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.2em;text-transform:uppercase;color:{tag_color};">{tag}</div>
      </td>
    </tr></table>
    <div style="font-family:Georgia,serif;font-size:20px;font-weight:700;color:#1D1D1F;line-height:1.3;margin-bottom:6px;">{headline}</div>
    <div style="font-family:Arial,sans-serif;font-size:12px;color:#8A8A8E;font-style:italic;margin-bottom:18px;padding-bottom:16px;border-bottom:1px solid #E5E5EA;line-height:1.6;">{deck}</div>
    <table width="100%" cellpadding="0" cellspacing="0">{rows_html}</table>
  </td></tr>"""

    def row(label, body, label_color="#CC0000", last=False):
        border = "" if last else "border-bottom:1px solid #E5E5EA;"
        return f"""<tr><td style="padding:13px 0;{border}">
          <div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.16em;text-transform:uppercase;color:{label_color};margin-bottom:5px;">{label}</div>
          <div style="font-family:Arial,sans-serif;font-size:12px;color:#3A3A3C;line-height:1.75;">{body}</div>
        </td></tr>"""

    signal = (
        row("What Happened", "The DOJ released three previously withheld FBI 302 documents from the Epstein files. Journalists disproved \"duplicative\" claims in hours. At least <strong>37 pages remain missing.</strong> House Oversight voted — bipartisan — to subpoena AG Pam Bondi.") +
        row("Why It Matters", "This isn't a Trump story. It's a document-management story. Bipartisan subpoenas on document releases are rare — that signals institutional anxiety about what's still in those files.") +
        row("The System Underneath", "Document declassification is a power tool. The Epstein Files Act was meant to force transparency. What we're watching is the government's interpretive immune system working against it.") +
        row("What to Watch", "Will AG Bondi comply or assert executive privilege? Watch for a legal challenge, a quiet partial release, and another document gap. Miami Herald's Julie K. Brown is the primary source.", last=True)
    )
    system = (
        row("The Incentives", "OPOs held geographic monopolies and reported their own denominators. The 2022 CMS shift to death-certificate metrics was the first real accountability mechanism. In 2025, donations declined for the first time in 14 years after ~20,000 people removed themselves from registries.") +
        row("The Leverage Points", "1. DCD + NRP normalization — now 49% of all donors. 2. CMS decertification of Tier 2/3 OPOs, late 2026. 3. Trust repair — March 2026 CMS guidance banning OPO coercion. <em>Any system where the actor controls the denominator will optimize for the denominator, not the outcome.</em>", last=True)
    )
    operator = (
        row("The Principle", "High-competence people measure themselves against a higher internal standard. Average performers compare to the crowd and conclude they're above it. Exceptional performers compare to the ideal and find themselves lacking.", label_color="#FF9500") +
        row("How to Apply It", "When you feel underqualified — for a pitch, a role, a risk — ask: <em>Am I measuring myself against the crowd, or against an ideal?</em> The internal critic that makes you good at the work is the same voice that makes you hesitant to claim it.", label_color="#FF9500", last=True)
    )
    medical = (
        row("The Clinical Reality", "DCD requires a withdrawal of life support decision <em>before</em> donation is discussed. The OPO cannot be in the room. The physician declaring death cannot be on the transplant team. These firewalls exist for a reason.", label_color="#007AFF") +
        row("The Teaching Point", "Brain death ≠ circulatory death. The March 2026 CMS guidance prohibiting OPOs from influencing withdrawal timing is not housekeeping — it's the system trying to re-establish a firewall that had eroded under volume pressure.", label_color="#007AFF", last=True)
    )
    power = (
        row("The Real Leverage Point", "The Strait of Hormuz. 20% of global oil through a 21-mile channel. Iran doesn't need to win militarily — it only needs to keep the strait disrupted long enough to push Brent above $120 and fracture U.S. ally support. The real war is in the shipping lanes.") +
        row("What to Watch", "Does the Interim Leadership Council request ceasefire through Qatar/Russia? Does Brent break $110? Does China press the Taiwan Strait to test U.S. attention bandwidth?") +
        row("Live Data", "Brent: $98.71 (+3.11%) &nbsp;·&nbsp; Strait of Hormuz: blocked &nbsp;·&nbsp; Iran War Day 15", label_color="#8A8A8E", last=True)
    )
    build = (
        row("What's Happening", "Claude Code is now #1 AI coding tool — overtaking Copilot and Cursor in 8 months. Anthropic ships 60–100 internal releases per day. The COBOL announcement: Claude converts 60-year-old banking infrastructure to modern code. IBM lost $40B in one session.", label_color="#34C759") +
        row("Leverage Point for T-1 Med", "The constraint is no longer 'can we write the code.' It's <em>'do we understand the problem well enough to tell Claude what to build?'</em> Medical background + systems thinking + AI tooling = a very small Venn diagram. That's your moat.", label_color="#34C759", last=True)
    )

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>The Barringer Brief — {date_str}</title>
</head>
<body style="margin:0;padding:0;background:#F5F5F7;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#F5F5F7;padding:28px 0;">
<tr><td align="center">
<table width="620" cellpadding="0" cellspacing="0" style="max-width:620px;width:100%;">

  <!-- HEADER -->
  <tr><td style="background:#0A2342;padding:10px 24px;border-radius:8px 8px 0 0;">
    <table width="100%" cellpadding="0" cellspacing="0"><tr>
      <td style="font-family:Georgia,serif;font-size:12px;font-weight:700;color:#fff;">The Barringer Brief</td>
      <td align="right" style="font-family:'Courier New',monospace;font-size:8px;color:rgba(255,255,255,0.85);letter-spacing:0.08em;text-transform:uppercase;">{date_str}</td>
      <td align="right" style="padding-left:12px;"><span style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;background:#CC0000;color:#fff;padding:3px 8px;border-radius:3px;letter-spacing:0.06em;text-transform:uppercase;">DAILY BRIEF</span></td>
    </tr></table>
  </td></tr>

  <!-- MASTHEAD -->
  <tr><td style="background:#fff;padding:32px 28px 22px;text-align:center;border-left:1px solid #E5E5EA;border-right:1px solid #E5E5EA;">
    <div style="font-family:Georgia,serif;font-size:42px;font-weight:900;color:#1D1D1F;letter-spacing:-1.5px;line-height:1;">The Barringer Brief</div>
    <div style="font-family:Arial,sans-serif;font-size:9px;letter-spacing:0.32em;text-transform:uppercase;color:#8A8A8E;margin-top:12px;">Intelligence &nbsp;·&nbsp; Systems &nbsp;·&nbsp; Medicine &nbsp;·&nbsp; Power</div>
    <div style="width:24px;height:2px;background:#CC0000;margin:14px auto 12px;"></div>
    <div style="font-family:Arial,sans-serif;font-size:12px;color:#8A8A8E;">Good Morning, Jadie &nbsp;·&nbsp; {date_str}</div>
  </td></tr>

  <!-- WEATHER + METAR -->
  <tr><td style="background:#F5F5F7;border:1px solid #E5E5EA;border-top:none;padding:20px 28px;">
    <div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.2em;text-transform:uppercase;color:#3A3A3C;margin-bottom:14px;padding-bottom:10px;border-bottom:1px solid #E5E5EA;">Live Conditions</div>
    <table width="100%" cellpadding="0" cellspacing="0"><tr>
      {wx_html}
      <td style="vertical-align:top;padding-left:24px;">
        <div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.16em;text-transform:uppercase;color:#3A3A3C;margin-bottom:10px;">METAR</div>
        <table cellpadding="0" cellspacing="0">{metar_rows_html(metar)}</table>
      </td>
    </tr></table>
  </td></tr>

  <!-- MARKETS -->
  <tr><td style="background:#fff;border:1px solid #E5E5EA;border-top:none;padding:16px 16px;">
    <div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.2em;text-transform:uppercase;color:#8A8A8E;margin-bottom:14px;padding-bottom:10px;border-bottom:1px solid #E5E5EA;">Markets</div>
    <table width="100%" cellpadding="0" cellspacing="0"><tr>{mkt_cells}</tr></table>
  </td></tr>

  <!-- YOUR WEEK -->
  <tr><td style="background:#fff;border:1px solid #E5E5EA;border-top:none;padding:20px 28px 16px;">
    <div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.2em;text-transform:uppercase;color:#8A8A8E;margin-bottom:14px;padding-bottom:10px;border-bottom:1px solid #E5E5EA;">Your Week · Calendar</div>
    {cal_html}
  </td></tr>

  {section(1, "The Signal · Lead Story", "#CC0000",
    "The Epstein Files Weren't Released. They Were Managed.",
    "The DOJ called them \"duplicative.\" Journalists called that a lie. Here's the system underneath the story.",
    signal)}

  {section(2, "System Breakdown · Incentives", "#CC0000",
    "How Organ Procurement Actually Works — And Why It's Structurally Broken",
    "49,065 transplants in 2025 was a record. But the system left tens of thousands of usable organs discarded.",
    system)}

  {section(3, "Operator Insight · Mental Leverage", "#FF9500",
    "Why Exceptional People Systematically Underestimate Themselves",
    "Intelligence agencies have known for decades: the most capable candidates are most likely to self-select out.",
    operator)}

  {section(4, "Medical Frontier · ICU · Transplant", "#007AFF",
    "Why DCD Timing Is the Most Ethically Loaded Moment in Medicine",
    "Most people think organ donation decisions are made at death. They're made in the hours before it.",
    medical)}

  {section(5, "Power Map · Geopolitics", "#CC0000",
    "The Iran War: A Systems Map of Who Holds the Leverage",
    "Trump: 'most intense day of strikes' coming Tuesday. Here's who holds what card.",
    power)}

  <!-- MARKET OUTLOOK TABLE -->
  <tr><td style="height:2px;background:#F5F5F7;"></td></tr>
  <tr><td style="background:#fff;border:1px solid #E5E5EA;padding:28px 28px 24px;">
    <table cellpadding="0" cellspacing="0" style="margin-bottom:18px;"><tr>
      <td style="font-family:Georgia,serif;font-size:56px;font-weight:900;color:#1D1D1F;line-height:1;opacity:0.30;padding-right:12px;vertical-align:middle;">06</td>
      <td style="vertical-align:middle;border-left:2px solid #8A8A8E;padding-left:10px;">
        <div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.2em;text-transform:uppercase;color:#8A8A8E;">Market Outlook · Week of {date_str}</div>
      </td>
    </tr></table>
    <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
      <tr style="border-bottom:1px solid #E5E5EA;">
        <td style="font-family:'Courier New',monospace;font-size:8px;font-weight:700;color:#8A8A8E;padding:0 14px 8px 0;width:68px;">DAY</td>
        <td style="font-family:'Courier New',monospace;font-size:8px;font-weight:700;color:#8A8A8E;padding:0 14px 8px 0;">EVENT</td>
        <td style="font-family:'Courier New',monospace;font-size:8px;font-weight:700;color:#8A8A8E;padding:0 0 8px;">SIGNAL</td>
      </tr>
      <tr style="border-bottom:1px solid #E5E5EA;"><td style="font-family:'Courier New',monospace;font-size:9px;color:#8A8A8E;padding:9px 14px 9px 0;vertical-align:top;">MON 16</td><td style="font-family:Arial,sans-serif;font-size:11px;font-weight:600;color:#1D1D1F;padding:9px 14px 9px 0;vertical-align:top;">Nvidia GTC begins</td><td style="font-family:Arial,sans-serif;font-size:11px;color:#3A3A3C;padding:9px 0;vertical-align:top;line-height:1.5;">AI narrative vs. Iran anxiety — keynote is the swing factor</td></tr>
      <tr style="border-bottom:1px solid #E5E5EA;"><td style="font-family:'Courier New',monospace;font-size:9px;color:#8A8A8E;padding:9px 14px 9px 0;vertical-align:top;">TUE 17</td><td style="font-family:Arial,sans-serif;font-size:11px;font-weight:600;color:#1D1D1F;padding:9px 14px 9px 0;vertical-align:top;">ADP Employment</td><td style="font-family:Arial,sans-serif;font-size:11px;color:#3A3A3C;padding:9px 0;vertical-align:top;line-height:1.5;">Labor softening = dovish pressure on Fed</td></tr>
      <tr style="border-bottom:1px solid #E5E5EA;"><td style="font-family:'Courier New',monospace;font-size:9px;color:#8A8A8E;padding:9px 14px 9px 0;vertical-align:top;">WED 18</td><td style="font-family:Arial,sans-serif;font-size:11px;font-weight:600;color:#1D1D1F;padding:9px 14px 9px 0;vertical-align:top;">FOMC + Dot Plot</td><td style="font-family:Arial,sans-serif;font-size:11px;color:#3A3A3C;padding:9px 0;vertical-align:top;line-height:1.5;">Hawkish hold likely. Any cut reduction in dot plot = selloff</td></tr>
      <tr style="border-bottom:1px solid #E5E5EA;"><td style="font-family:'Courier New',monospace;font-size:9px;color:#8A8A8E;padding:9px 14px 9px 0;vertical-align:top;">THU 19</td><td style="font-family:Arial,sans-serif;font-size:11px;font-weight:600;color:#1D1D1F;padding:9px 14px 9px 0;vertical-align:top;">Micron · Nike · FedEx</td><td style="font-family:Arial,sans-serif;font-size:11px;color:#3A3A3C;padding:9px 0;vertical-align:top;line-height:1.5;">Micron = AI memory bellwether. FedEx = Iran supply chain</td></tr>
      <tr><td style="font-family:'Courier New',monospace;font-size:9px;color:#8A8A8E;padding:9px 14px 9px 0;vertical-align:top;">FRI 20</td><td style="font-family:Arial,sans-serif;font-size:11px;font-weight:600;color:#1D1D1F;padding:9px 14px 9px 0;vertical-align:top;">BoE · Global PMIs</td><td style="font-family:Arial,sans-serif;font-size:11px;color:#3A3A3C;padding:9px 0;vertical-align:top;line-height:1.5;">Will Europe blink on rate holds given energy shock?</td></tr>
    </table>
    <div style="margin-top:14px;padding-top:14px;border-top:1px solid #E5E5EA;font-family:Arial,sans-serif;font-size:11px;color:#3A3A3C;line-height:1.7;">
      <strong style="color:#1D1D1F;">Risk:</strong> Brent $110 → EU central banks re-price → Fed paralyzed → stagflation base case.<br>
      <strong style="color:#1D1D1F;">Wildcard:</strong> Nvidia GTC. One strong Blackwell Ultra demo reprices the entire AI trade.
    </div>
  </td></tr>

  <!-- SOUTH FLORIDA -->
  <tr><td style="height:2px;background:#F5F5F7;"></td></tr>
  <tr><td style="background:#fff;border:1px solid #E5E5EA;padding:28px 28px 24px;">
    <table cellpadding="0" cellspacing="0" style="margin-bottom:18px;"><tr>
      <td style="font-family:Georgia,serif;font-size:56px;font-weight:900;color:#5856D6;line-height:1;opacity:0.30;padding-right:12px;vertical-align:middle;">07</td>
      <td style="vertical-align:middle;border-left:2px solid #5856D6;padding-left:10px;">
        <div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.2em;text-transform:uppercase;color:#5856D6;">South Florida · This Weekend</div>
      </td>
    </tr></table>
    <table width="100%" cellpadding="0" cellspacing="0"><tr>
      <td width="50%" valign="top" style="padding-right:16px;">
        <div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.14em;text-transform:uppercase;color:#5856D6;margin-bottom:10px;">Date Night</div>
        <div style="font-family:Georgia,serif;font-size:13px;font-weight:700;color:#1D1D1F;margin-bottom:3px;">Hell's Kitchen — The Musical</div>
        <div style="font-family:Arial,sans-serif;font-size:11px;color:#3A3A3C;line-height:1.6;margin-bottom:10px;">Broward Center. Tony Award-winner built on Alicia Keys' catalog. Through Mar 22.</div>
        <div style="font-family:Georgia,serif;font-size:13px;font-weight:700;color:#1D1D1F;margin-bottom:3px;">Candlelight: Beatles Tribute</div>
        <div style="font-family:Arial,sans-serif;font-size:11px;color:#3A3A3C;line-height:1.6;">Sun Mar 15. Hotel Colonnade, Coral Gables.</div>
      </td>
      <td width="50%" valign="top" style="padding-left:16px;border-left:1px solid #E5E5EA;">
        <div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.14em;text-transform:uppercase;color:#34C759;margin-bottom:10px;">Sports + Outdoors</div>
        <div style="font-family:Georgia,serif;font-size:13px;font-weight:700;color:#1D1D1F;margin-bottom:3px;">Miami Open Tennis</div>
        <div style="font-family:Arial,sans-serif;font-size:11px;color:#3A3A3C;line-height:1.6;margin-bottom:10px;">Mar 15–29. Hard Rock Stadium. Top ATP + WTA.</div>
        <div style="font-family:Georgia,serif;font-size:13px;font-weight:700;color:#1D1D1F;margin-bottom:3px;">Calle Ocho Music Festival</div>
        <div style="font-family:Arial,sans-serif;font-size:11px;color:#3A3A3C;line-height:1.6;">Today. Little Havana. World's largest Latin music festival. Free.</div>
      </td>
    </tr>
    <tr><td colspan="2" style="padding-top:14px;">
      <div style="background:#F5F5F7;border:1px solid #E5E5EA;border-radius:4px;padding:10px 14px;">
        <span style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.14em;text-transform:uppercase;color:#1D1D1F;">Weston &nbsp;&nbsp;</span>
        <span style="font-family:Arial,sans-serif;font-size:11px;color:#3A3A3C;">Water Matters Day — today. Next: Symphony in the Park "Sounds of Spring" — Fri Mar 28, 6:30 PM, free.</span>
      </div>
    </td></tr></table>
  </td></tr>

  {section(8, "Build Log · T-1 Med · AI Tooling", "#34C759",
    "Claude Code Is Eating Software Engineering",
    "When IBM lost $40B in one session after an Anthropic blog post, that was a signal. Here's how to read it.",
    build)}

  <!-- AVIATION -->
  <tr><td style="height:2px;background:#F5F5F7;"></td></tr>
  <tr><td style="background:#fff;border:1px solid #E5E5EA;padding:28px 28px 24px;">
    <table cellpadding="0" cellspacing="0" style="margin-bottom:18px;"><tr>
      <td style="font-family:Georgia,serif;font-size:56px;font-weight:900;color:#007AFF;line-height:1;opacity:0.30;padding-right:12px;vertical-align:middle;">09</td>
      <td style="vertical-align:middle;border-left:2px solid #007AFF;padding-left:10px;">
        <div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.2em;text-transform:uppercase;color:#007AFF;">Aviation · IFR Track</div>
      </td>
    </tr></table>
    <table width="100%" cellpadding="0" cellspacing="0"><tr>
      <td width="50%" valign="top" style="padding-right:16px;">
        <div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.14em;text-transform:uppercase;color:#007AFF;margin-bottom:10px;">Go / No-Go</div>
        <table width="100%" cellpadding="0" cellspacing="0">
          <tr><td style="padding:8px 0;border-bottom:1px solid #E5E5EA;">
            <div style="font-family:'Courier New',monospace;font-size:8px;color:#8A8A8E;margin-bottom:2px;">TODAY</div>
            <div style="font-family:Arial,sans-serif;font-size:12px;font-weight:700;color:#34C759;">GO — VFR</div>
            <div style="font-family:Arial,sans-serif;font-size:10px;color:#3A3A3C;">Winds light · vis 10SM</div>
          </td></tr>
          <tr><td style="padding:8px 0;border-bottom:1px solid #E5E5EA;">
            <div style="font-family:'Courier New',monospace;font-size:8px;color:#8A8A8E;margin-bottom:2px;">TOMORROW</div>
            <div style="font-family:Arial,sans-serif;font-size:12px;font-weight:700;color:#CC0000;">NO-GO</div>
            <div style="font-family:Arial,sans-serif;font-size:10px;color:#3A3A3C;">Ceilings 1,500 ft · embedded convection · 50 mph gusts</div>
          </td></tr>
          <tr><td style="padding:8px 0;">
            <div style="font-family:'Courier New',monospace;font-size:8px;color:#8A8A8E;margin-bottom:2px;">MONDAY</div>
            <div style="font-family:Arial,sans-serif;font-size:12px;font-weight:700;color:#34C759;">GO</div>
            <div style="font-family:Arial,sans-serif;font-size:10px;color:#3A3A3C;">System clears · good week for IFR dual</div>
          </td></tr>
        </table>
      </td>
      <td width="50%" valign="top" style="padding-left:16px;border-left:1px solid #E5E5EA;">
        <div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.14em;text-transform:uppercase;color:#007AFF;margin-bottom:10px;">Alternate Minimums</div>
        <table cellpadding="0" cellspacing="0" width="100%">
          <tr><td style="font-family:'Courier New',monospace;font-size:12px;font-weight:700;color:#1D1D1F;padding:5px 14px 5px 0;">KFLL</td><td style="font-family:'Courier New',monospace;font-size:11px;color:#8A8A8E;">600-2</td></tr>
          <tr><td style="font-family:'Courier New',monospace;font-size:12px;font-weight:700;color:#1D1D1F;padding:5px 14px 5px 0;">KPMP</td><td style="font-family:'Courier New',monospace;font-size:11px;color:#8A8A8E;">800-2</td></tr>
          <tr><td style="font-family:'Courier New',monospace;font-size:12px;font-weight:700;color:#1D1D1F;padding:5px 14px 5px 0;">KFXE</td><td style="font-family:'Courier New',monospace;font-size:11px;color:#8A8A8E;">900-2</td></tr>
        </table>
        <div style="margin-top:12px;padding:10px 12px;background:#F5F5F7;border:1px solid #E5E5EA;border-radius:4px;">
          <div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.1em;text-transform:uppercase;color:#FF9500;margin-bottom:4px;">Decision Brief</div>
          <div style="font-family:Arial,sans-serif;font-size:10px;color:#3A3A3C;line-height:1.6;">Sunday triggers the 1-2-3 rule at KFLL. Pull the RNAV 28L plate. Brief the missed approach out loud — the DPE will ask you cold.</div>
        </div>
      </td>
    </tr></table>
  </td></tr>

  <!-- FOOTER -->
  <tr><td style="height:8px;background:#F5F5F7;"></td></tr>
  <tr><td style="background:#0A2342;padding:24px 28px;text-align:center;border-radius:0 0 8px 8px;">
    <div style="font-family:Georgia,serif;font-size:16px;font-weight:900;color:#fff;letter-spacing:-0.5px;">The Barringer Brief</div>
    <div style="font-family:Arial,sans-serif;font-size:8px;letter-spacing:0.24em;text-transform:uppercase;color:rgba(255,255,255,0.7);margin-top:6px;">Intelligence · Systems · Medicine · Power</div>
    <div style="width:20px;height:1px;background:#CC0000;margin:12px auto;"></div>
    <a href="{LIVE_URL}" style="font-family:Arial,sans-serif;font-size:10px;font-weight:600;color:#fff;text-decoration:none;letter-spacing:0.04em;">{LIVE_URL.replace('https://','')}</a>
  </td></tr>

</table></td></tr></table>
</body></html>"""

# ── SEND VIA RESEND ───────────────────────────────────────────────────────────
def send_email(subject, html):
    if not RESEND_API_KEY:
        log("[ERROR] RESEND_API_KEY environment variable not set.")
        return None
    payload = json.dumps({
        "from":    FROM_ADDRESS,
        "to":      [RECIPIENT],
        "subject": subject,
        "html":    html,
    }).encode()
    req = urllib.request.Request(
        "https://api.resend.com/emails",
        data=payload,
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type":  "application/json",
        },
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            result = json.loads(r.read())
            return result.get("id")
    except urllib.error.HTTPError as e:
        err = e.read().decode()
        log(f"[RESEND ERROR] {e.code}: {err}")
        return None
    except Exception as e:
        log(f"[SEND ERROR] {e}")
        return None

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    now      = datetime.datetime.now()
    date_str = now.strftime("%A, %B %-d, %Y")
    subject  = f"The Barringer Brief — {date_str}"

    log(f"Building The Barringer Brief for {date_str}")

    weather         = fetch_weather()
    metar           = fetch_metar()
    markets         = fetch_markets()
    calendar_events = fetch_calendar()

    log(f"  Weather:  {'OK' if weather else 'FAILED'}")
    log(f"  METAR:    {len(metar)} airports")
    log(f"  Markets:  {len(markets)} tickers")
    log(f"  Calendar: {len(calendar_events)} events")

    html     = build_email_html(weather, metar, markets, calendar_events, date_str)
    email_id = send_email(subject, html)

    if email_id:
        log(f"  ✓ Email sent to {RECIPIENT} (id: {email_id})")
    else:
        log("  ✗ Email FAILED")
        sys.exit(1)

if __name__ == "__main__":
    main()
