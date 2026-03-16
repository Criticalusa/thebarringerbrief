#!/usr/bin/env python3
"""
The Barringer Brief — Local Mac Version
Runs via LaunchAgent every morning at 7:00 AM.
No external dependencies — uses only Python stdlib + subprocess (curl for Resend).
Fetches: weather, METAR, TAF, markets, calendar, news RSS, social, Reddit.
Sends via Resend API directly.
"""

import json, datetime, urllib.request, urllib.error, sys, subprocess, re, os
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime

# ── CONFIG ────────────────────────────────────────────────────────────────────
RECIPIENT      = "jadie2@mac.com"
FROM_ADDRESS   = "The Barringer Brief <brief@thebarringerbrief.com>"
RESEND_API_KEY = "re_bwoh8Rgi_JrK9uufjUQXyvHuP3YQ3EpYd"
LIVE_URL       = "https://thebarringerbrief.com"
LOG_FILE       = os.path.expanduser("~/Library/Logs/barringer-brief.log")

# ── LOGGING ───────────────────────────────────────────────────────────────────
def log(msg):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except:
        pass

# ── HELPERS ──────────────────────────────────────────────────────────────────
def time_ago(date_str):
    """Parse RSS date string, return '2h ago', '35m ago', '1d ago' etc."""
    if not date_str:
        return ""
    try:
        # Try RFC 2822 first: "Mon, 16 Mar 2026 12:00:00 +0000"
        dt = parsedate_to_datetime(date_str)
    except Exception:
        try:
            # Try ISO 8601: "2026-03-16T12:00:00Z"
            cleaned = date_str.replace("Z", "+00:00")
            dt = datetime.datetime.fromisoformat(cleaned)
        except Exception:
            return ""
    try:
        now = datetime.datetime.now(datetime.timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        secs = int((now - dt).total_seconds())
        if secs < 0:
            secs = 0
        if secs < 60:
            return "just now"
        mins = secs // 60
        if mins < 60:
            return str(mins) + "m ago"
        hours = mins // 60
        if hours < 24:
            return str(hours) + "h ago"
        days = hours // 24
        return str(days) + "d ago"
    except Exception:
        return ""


def time_ago_dt(dt):
    """Return time ago from a datetime object."""
    if dt is None:
        return ""
    try:
        now = datetime.datetime.now(datetime.timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        secs = int((now - dt).total_seconds())
        if secs < 0:
            secs = 0
        if secs < 60:
            return "just now"
        mins = secs // 60
        if mins < 60:
            return str(mins) + "m ago"
        hours = mins // 60
        if hours < 24:
            return str(hours) + "h ago"
        days = hours // 24
        return str(days) + "d ago"
    except Exception:
        return ""


def truncate(text, max_len=200):
    """Truncate text to max_len chars, end with '...'."""
    if not text:
        return ""
    text = text.strip()
    if len(text) <= max_len:
        return text
    return text[:max_len - 3].rsplit(" ", 1)[0] + "..."


def strip_html(text):
    """Remove HTML tags from RSS descriptions."""
    if not text:
        return ""
    return re.sub(r'<[^>]+>', '', text).strip()


def _fetch_rss(url, source_name, timeout=10):
    """Fetch and parse an RSS feed, returning list of item dicts."""
    items = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
    except Exception as e:
        log(f"[RSS ERROR] {source_name}: fetch failed: {e}")
        return items
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        log(f"[RSS ERROR] {source_name}: malformed XML: {e}")
        return items
    except Exception as e:
        log(f"[RSS ERROR] {source_name}: XML parse error: {e}")
        return items
    for item in root.findall(".//item"):
        title_el = item.find("title")
        link_el = item.find("link")
        pub_el = item.find("pubDate")
        desc_el = item.find("description")
        title = title_el.text.strip() if title_el is not None and title_el.text else ""
        link = link_el.text.strip() if link_el is not None and link_el.text else ""
        pub_date = pub_el.text.strip() if pub_el is not None and pub_el.text else ""
        desc = desc_el.text.strip() if desc_el is not None and desc_el.text else ""
        if not title:
            continue
        dt = None
        if pub_date:
            try:
                dt = parsedate_to_datetime(pub_date)
            except Exception:
                try:
                    cleaned = pub_date.replace("Z", "+00:00")
                    dt = datetime.datetime.fromisoformat(cleaned)
                except Exception:
                    pass
        items.append({
            "title": title,
            "link": link,
            "source": source_name,
            "pub_date": pub_date,
            "dt": dt,
            "description": strip_html(desc),
        })
    return items


def _sort_and_limit(items, limit):
    """Sort items by dt descending (None last), return top N."""
    with_dt = [i for i in items if i.get("dt") is not None]
    without_dt = [i for i in items if i.get("dt") is None]
    with_dt.sort(key=lambda x: x["dt"], reverse=True)
    return (with_dt + without_dt)[:limit]


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

# ── TAF ──────────────────────────────────────────────────────────────────────
def fetch_taf():
    """Fetch TAF for KFLL to build Go/No-Go recommendation."""
    try:
        req = urllib.request.Request(
            "https://aviationweather.gov/api/data/taf?ids=KFLL&format=json",
            headers={"User-Agent": "BarringerBrief/1.0"}
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
        if not data:
            return None
        taf = data[0]
        raw_text = taf.get("rawTAF", "")
        # Parse forecast groups for wind/vis/ceiling
        def _safe_num(val, default=0):
            """Coerce a value to float, returning default if conversion fails."""
            if val is None:
                return default
            try:
                return float(val)
            except (ValueError, TypeError):
                return default

        forecasts = []
        for fcst in taf.get("fcsts", taf.get("forecast", [])):
            wind_spd = _safe_num(fcst.get("wspd"), 0)
            wind_gust = _safe_num(fcst.get("wgst"), 0)
            vis = _safe_num(fcst.get("visib"), 6)
            # Ceiling: lowest broken or overcast layer
            ceil = 99999
            for cld in fcst.get("clouds", []):
                cover = cld.get("cover", "")
                base = _safe_num(cld.get("base"), 99999)
                if cover in ("BKN", "OVC") and base < ceil:
                    ceil = base
            forecasts.append({
                "wspd": wind_spd,
                "wgst": wind_gust,
                "vis": vis,
                "ceil": ceil,
            })
        # Determine Go/No-Go for next ~12h (first few forecast periods)
        worst_ceil = 99999
        worst_vis = 99
        worst_wind = 0
        worst_gust = 0
        for f in forecasts[:4]:
            if f["ceil"] < worst_ceil:
                worst_ceil = f["ceil"]
            if f["vis"] < worst_vis:
                worst_vis = f["vis"]
            if f["wspd"] > worst_wind:
                worst_wind = f["wspd"]
            if f["wgst"] > worst_gust:
                worst_gust = f["wgst"]
        # Decision logic
        if worst_ceil < 500 or worst_vis < 1 or worst_gust > 35:
            status = "NO-GO"
            status_color = "#CC0000"
            reason = ""
            reasons = []
            if worst_ceil < 500:
                reasons.append("Ceilings below 500 ft")
            if worst_vis < 1:
                reasons.append("Visibility below 1 SM")
            if worst_gust > 35:
                reasons.append("Gusts " + str(worst_gust) + " kt")
            reason = " / ".join(reasons)
        elif worst_ceil < 1000 or worst_vis < 3 or worst_gust > 25:
            status = "MARGINAL"
            status_color = "#FF9500"
            reasons = []
            if worst_ceil < 1000:
                reasons.append("Ceilings " + str(worst_ceil) + " ft")
            if worst_vis < 3:
                reasons.append("Vis " + str(worst_vis) + " SM")
            if worst_gust > 25:
                reasons.append("Gusts " + str(worst_gust) + " kt")
            reason = " / ".join(reasons)
        else:
            status = "GO — VFR"
            status_color = "#34C759"
            reason = "Winds " + str(worst_wind) + " kt"
            if worst_gust > 0:
                reason += " G" + str(worst_gust)
            reason += " / Vis " + str(worst_vis) + "+ SM"
        return {
            "status": status,
            "status_color": status_color,
            "reason": reason,
            "raw": raw_text,
            "worst_ceil": worst_ceil,
            "worst_vis": worst_vis,
            "worst_wind": worst_wind,
            "worst_gust": worst_gust,
        }
    except Exception as e:
        log(f"[TAF ERROR] {e}")
        return None

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
            elif price > 100:
                price_str = f"{price:.2f}"
            else:
                price_str = f"{price:.2f}"
            markets[sym] = {
                "price": price_str,
                "change": f"{chg:+.2f}",
                "pct": f"{pct:.2f}",
            }
        except Exception as e:
            log(f"[MARKET ERROR] {sym}: {e}")
    return markets

# ── CALENDAR (macOS via AppleScript) ─────────────────────────────────────────
def fetch_calendar():
    """Fetch next 7 days of calendar events via faster AppleScript."""
    script = '''
    set output to ""
    set d1 to current date
    set d2 to d1 + (7 * days)
    tell application "Calendar"
        repeat with c in (every calendar)
            repeat with e in (every event of c whose start date >= d1 and start date < d2)
                set t to summary of e
                set s to start date of e
                set output to output & t & "~" & (s as string) & "||"
            end repeat
        end repeat
    end tell
    return output
    '''
    try:
        result = subprocess.run(["osascript", "-e", script],
            capture_output=True, text=True, timeout=45)
        if result.returncode != 0 or not result.stdout.strip():
            return []
        events = []
        seen = set()
        for block in result.stdout.strip().split("||"):
            block = block.strip()
            if not block or "~" not in block:
                continue
            parts = block.split("~", 1)
            title = parts[0].strip()
            time_raw = parts[1].strip() if len(parts) > 1 else ""
            if title in seen:
                continue
            seen.add(title)
            try:
                clean = time_raw.replace(" at ", " ")
                dt = None
                for fmt in ["%A, %B %d, %Y %I:%M:%S %p", "%A, %B %d, %Y %I:%M %p",
                            "%A, %B %d, %Y %I %p", "%A, %B %d, %Y"]:
                    try:
                        dt = datetime.datetime.strptime(clean, fmt)
                        break
                    except:
                        continue
                time_str = dt.strftime("%A, %b %-d · %-I:%M %p") if dt else time_raw
            except:
                time_str = time_raw
            events.append({"title": title, "time": time_str, "location": "", "attendees": []})
        events.sort(key=lambda e: e["time"])
        return events[:8]
    except Exception as ex:
        log(f"[CALENDAR ERROR] {ex}")
        return []

# ── TOP NEWS (RSS) ───────────────────────────────────────────────────────────
def fetch_top_news():
    """Fetch top 10 stories across major political/general news RSS feeds."""
    feeds = [
        ("https://feeds.npr.org/1001/rss.xml", "NPR News"),
        ("https://feeds.npr.org/1014/rss.xml", "NPR Politics"),
        ("https://thehill.com/feed/", "The Hill"),
        ("https://rss.politico.com/politics-news.xml", "Politico"),
        ("https://feeds.bbci.co.uk/news/rss.xml", "BBC News"),
        ("https://www.theguardian.com/us-news/rss", "Guardian US"),
    ]
    all_items = []
    for url, source in feeds:
        all_items.extend(_fetch_rss(url, source))
    return _sort_and_limit(all_items, 10)

# ── HEALTH NEWS (RSS) ────────────────────────────────────────────────────────
def fetch_health_news():
    """Fetch top 5 health/medical stories."""
    feeds = [
        ("https://www.statnews.com/feed/", "STAT News"),
        ("https://rss.politico.com/healthcare.xml", "Politico Health"),
        ("https://www.healthaffairs.org/rss/current", "Health Affairs"),
        ("https://kffhealthnews.org/feed/", "Kaiser Health"),
    ]
    all_items = []
    for url, source in feeds:
        all_items.extend(_fetch_rss(url, source))
    return _sort_and_limit(all_items, 5)

# ── AI / TECH NEWS (RSS) ─────────────────────────────────────────────────────
def fetch_ai_news():
    """Fetch top 5 AI/tech stories."""
    feeds = [
        ("https://techcrunch.com/category/artificial-intelligence/feed/", "TechCrunch AI"),
        ("https://www.technologyreview.com/feed/", "MIT Tech Review"),
        ("https://www.theverge.com/ai-artificial-intelligence/rss/index.xml", "The Verge AI"),
    ]
    all_items = []
    for url, source in feeds:
        all_items.extend(_fetch_rss(url, source))
    return _sort_and_limit(all_items, 5)

# ── SOUTH FLORIDA NEWS (RSS) ─────────────────────────────────────────────────
def fetch_sfla_news():
    """Fetch top 5 South Florida local stories."""
    feeds = [
        ("https://www.wlrn.org/rss.xml", "WLRN"),
        ("https://www.nbcmiami.com/feed/", "NBC6 Miami"),
        ("https://www.cbsnews.com/miami/feed/", "CBS Miami"),
        ("https://wsvn.com/feed/", "WSVN"),
    ]
    all_items = []
    for url, source in feeds:
        all_items.extend(_fetch_rss(url, source))
    return _sort_and_limit(all_items, 5)

# ── SOCIAL SIGNAL (Nitter RSS) ──────────────────────────────────────────────
def fetch_social_signal():
    """Fetch latest posts from social accounts via Nitter RSS."""
    accounts = {
        "Aaron Parnas":    "AaronParnas",
        "MeidasTouch":     "MeidasTouch",
        "David Pakman":    "dpakman",
        "James Li":        "5149jamesli",
        "Mehdi Hasan":     "mehdirhasan",
        "Prof. Jiang":     "xueqinjiang",
        "Scott Galloway":  "profgalloway",
        "Jessica Tarlov":  "JessicaTarlov",
        "Sam Seder":       "SamSeder",
        "Seth Abramson":   "SethAbramson",
    }
    nitter_bases = [
        "https://nitter.net",
        "https://nitter.privacydev.net",
        "https://nitter.poast.org",
    ]
    results = []
    for name, handle in accounts.items():
        post = None
        for base in nitter_bases:
            try:
                url = base + "/" + handle + "/rss"
                req = urllib.request.Request(url, headers={"User-Agent": "BarringerBrief/1.0"})
                with urllib.request.urlopen(req, timeout=10) as r:
                    raw = r.read()
                root = ET.fromstring(raw)
                item = root.find(".//item")
                if item is not None:
                    title_el = item.find("title")
                    desc_el = item.find("description")
                    pub_el = item.find("pubDate")
                    text = ""
                    if desc_el is not None and desc_el.text:
                        text = desc_el.text.strip()
                    elif title_el is not None and title_el.text:
                        text = title_el.text.strip()
                    text = strip_html(text)
                    if len(text) > 160:
                        text = text[:157] + "..."
                    dt = None
                    try:
                        pub_str = pub_el.text.strip() if pub_el is not None and pub_el.text else ""
                        if pub_str:
                            dt = parsedate_to_datetime(pub_str)
                    except Exception:
                        pass
                    post = {
                        "handle": handle,
                        "name": name,
                        "text": text,
                        "dt": dt,
                        "initial": name[0].upper(),
                    }
                break
            except Exception as e:
                log(f"[SOCIAL ERROR] {handle}@{base}: {e}")
                continue
        if post:
            results.append(post)
        # If all instances failed, skip gracefully
    return results

# ── REDDIT PULSE (JSON API) ─────────────────────────────────────────────────
def fetch_reddit_pulse():
    """Fetch top posts from subreddits using Reddit's public JSON API."""
    subreddits = [
        ("politics", "r/politics"),
        ("medicine", "r/medicine"),
        ("transplant", "r/transplant"),
        ("florida", "r/Florida"),
        ("wallstreetbets", "r/wallstreetbets"),
        ("ChatGPT", "r/ChatGPT"),
    ]
    results = []
    for sub, label in subreddits:
        try:
            url = "https://www.reddit.com/r/" + sub + "/hot.json?limit=3"
            req = urllib.request.Request(url, headers={"User-Agent": "BarringerBrief/1.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read())
            posts = data.get("data", {}).get("children", [])
            top_post = None
            for p in posts:
                pd = p.get("data", {})
                if pd.get("stickied", False):
                    continue
                top_post = {
                    "title": pd.get("title", ""),
                    "score": pd.get("score", 0),
                    "comments": pd.get("num_comments", 0),
                    "permalink": "https://reddit.com" + pd.get("permalink", ""),
                    "subreddit": label,
                }
                break
            if not top_post and posts:
                pd = posts[0].get("data", {})
                top_post = {
                    "title": pd.get("title", ""),
                    "score": pd.get("score", 0),
                    "comments": pd.get("num_comments", 0),
                    "permalink": "https://reddit.com" + pd.get("permalink", ""),
                    "subreddit": label,
                }
            if top_post:
                results.append(top_post)
            else:
                results.append({
                    "title": "(no posts available)",
                    "score": 0,
                    "comments": 0,
                    "permalink": "",
                    "subreddit": label,
                })
        except Exception as e:
            log(f"[REDDIT ERROR] {label}: {e}")
            results.append({
                "title": "(unavailable)",
                "score": 0,
                "comments": 0,
                "permalink": "",
                "subreddit": label,
            })
    return results

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
        rows += (
            '<tr>'
            '<td style="font-family:\'Courier New\',monospace;font-size:11px;font-weight:700;color:#1D1D1F;padding:5px 14px 5px 0;width:48px;">' + apt + '</td>'
            '<td style="font-family:\'Courier New\',monospace;font-size:10px;color:#3A3A3C;padding:5px 14px 5px 0;">' + str(m['temp']) + '°C &nbsp;' + str(m['cover']) + ' &nbsp;' + str(m['wdir']) + '/' + str(m['wspd']) + 'kt</td>'
            '<td style="padding:5px 0;"><span style="font-family:Arial,sans-serif;font-size:9px;font-weight:700;color:' + cat_color + ';">● ' + cat + '</span></td>'
            '</tr>'
        )
    return rows or "<tr><td style='font-size:11px;color:#8A8A8E;'>METAR unavailable</td></tr>"

# ── NEWS ROW BUILDER ─────────────────────────────────────────────────────────
def build_news_rows(items, label_color="#CC0000"):
    """Build HTML rows from a list of RSS news items."""
    rows = ""
    for idx, item in enumerate(items):
        title = item.get("title", "")
        link = item.get("link", "")
        source = item.get("source", "")
        pub = item.get("pub_date", "")
        desc = truncate(item.get("description", ""), 180)
        ago = time_ago(pub)
        is_last = idx == len(items) - 1
        border = "" if is_last else "border-bottom:1px solid #E5E5EA;"
        link_open = ""
        link_close = ""
        if link:
            link_open = '<a href="' + link + '" style="color:#1D1D1F;text-decoration:none;">'
            link_close = '</a>'
        ago_span = ""
        if ago:
            ago_span = ' <span style="font-family:Arial,sans-serif;font-size:9px;color:#8A8A8E;margin-left:6px;">' + ago + '</span>'
        desc_div = ""
        if desc:
            desc_div = '<div style="font-family:Arial,sans-serif;font-size:11px;color:#3A3A3C;line-height:1.6;margin-top:4px;">' + desc + '</div>'
        rows += (
            '<tr><td style="padding:12px 0;' + border + '">'
            '<div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;color:' + label_color + ';margin-bottom:4px;">'
            + source + ago_span + '</div>'
            '<div style="font-family:Georgia,serif;font-size:14px;font-weight:700;color:#1D1D1F;line-height:1.4;">'
            + link_open + title + link_close + '</div>'
            + desc_div +
            '</td></tr>'
        )
    return rows


# ── BUILD EMAIL ───────────────────────────────────────────────────────────────
def build_email_html(weather, metar, taf, markets, calendar_events, date_str,
                     top_news, health_news, ai_news, sfla_news,
                     social_signal, reddit_pulse):

    # Weather block
    if weather:
        wx_html = (
            '<td style="vertical-align:top;padding-right:24px;border-right:1px solid #E5E5EA;">'
            '<div style="font-family:Georgia,serif;font-size:52px;font-weight:200;color:#1D1D1F;line-height:1;letter-spacing:-2px;">'
            + weather['temp_f'] + '<span style="font-size:16px;color:#8A8A8E;font-weight:300;">°F</span></div>'
            '<div style="font-family:Arial,sans-serif;font-size:13px;font-weight:600;color:#1D1D1F;margin-top:6px;">'
            + weather_icon(weather['desc']) + ' ' + weather['desc'] + '</div>'
            '<div style="font-family:\'Courier New\',monospace;font-size:9px;color:#3A3A3C;margin-top:6px;line-height:2.0;">'
            'Hi ' + weather['high'] + '° / Lo ' + weather['low'] + '° &nbsp;·&nbsp; UV ' + weather['uv'] + '<br>'
            'Wind ' + weather['wind_mph'] + ' mph ' + weather['wind_dir'] + '<br>'
            'Feels like ' + weather['feels_like'] + '°F'
            '</div>'
            '<div style="font-family:\'Courier New\',monospace;font-size:8px;color:#8A8A8E;margin-top:8px;letter-spacing:0.14em;text-transform:uppercase;">Weston, FL</div>'
            '</td>'
        )
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
            ps    = arr + str(abs(pf)) + "%"
        except:
            color, ps = "#8A8A8E", "--"
        mkt_cells += (
            '<td align="center" style="padding:0 8px;">'
            '<div style="font-family:\'Courier New\',monospace;font-size:8px;color:#8A8A8E;letter-spacing:0.08em;margin-bottom:4px;">' + label + '</div>'
            '<div style="font-family:Arial,sans-serif;font-size:15px;font-weight:700;color:#1D1D1F;letter-spacing:-0.5px;">$' + price + '</div>'
            '<div style="font-family:Arial,sans-serif;font-size:10px;font-weight:600;color:' + color + ';margin-top:2px;">' + ps + '</div>'
            '</td>'
        )

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
            att_html  = ""
            if att:
                att_html = '<div style="font-family:Arial,sans-serif;font-size:10px;color:#8A8A8E;margin-top:2px;">' + " &nbsp;·&nbsp; ".join(att) + '</div>'
            loc_part  = "  ·  " + loc if loc else ""
            cal_rows += (
                '<tr><td style="padding:14px 0;border-bottom:1px solid #E5E5EA;">'
                '<table width="100%" cellpadding="0" cellspacing="0"><tr>'
                '<td width="52" valign="top" style="padding-right:16px;">'
                '<div style="width:44px;border-radius:10px;overflow:hidden;border:1px solid #E5E5EA;box-shadow:0 1px 3px rgba(0,0,0,0.10);">'
                '<div style="background:#CC0000;padding:3px 4px;text-align:center;">'
                '<div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.08em;color:#fff;text-transform:uppercase;">' + day_abbr + '</div>'
                '</div>'
                '<div style="background:#fff;padding:4px 4px 5px;text-align:center;">'
                '<div style="font-family:Arial,sans-serif;font-size:22px;font-weight:200;color:#1D1D1F;line-height:1;">' + day_num + '</div>'
                '</div></div></td>'
                '<td valign="top">'
                '<div style="font-family:Georgia,serif;font-size:14px;font-weight:700;color:#1D1D1F;line-height:1.3;margin-bottom:4px;">' + ev['title'] + '</div>'
                '<div style="font-family:Arial,sans-serif;font-size:11px;color:#3A3A3C;margin-bottom:2px;">' + time_part + loc_part + '</div>'
                + att_html +
                '</td></tr></table></td></tr>'
            )
        cal_html = '<table width="100%" cellpadding="0" cellspacing="0">' + cal_rows + '</table>'
    else:
        cal_html = "<div style='font-family:Arial,sans-serif;font-size:12px;color:#8A8A8E;padding:8px 0;'>No events scheduled this week.</div>"

    # ── Section helper (kept as-is) ──────────────────────────────────────
    def section(num, tag, tag_color, headline, deck, rows_html):
        return (
            '<tr><td style="height:2px;background:#F5F5F7;"></td></tr>'
            '<tr><td style="background:#fff;border:1px solid #E5E5EA;padding:28px 28px 24px;">'
            '<table cellpadding="0" cellspacing="0" style="margin-bottom:18px;"><tr>'
            '<td style="font-family:Georgia,serif;font-size:56px;font-weight:900;color:' + tag_color + ';line-height:1;opacity:0.30;padding-right:12px;vertical-align:middle;">' + str(num).zfill(2) + '</td>'
            '<td style="vertical-align:middle;border-left:2px solid ' + tag_color + ';padding-left:10px;">'
            '<div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.2em;text-transform:uppercase;color:' + tag_color + ';">' + tag + '</div>'
            '</td></tr></table>'
            '<div style="font-family:Georgia,serif;font-size:20px;font-weight:700;color:#1D1D1F;line-height:1.3;margin-bottom:6px;">' + headline + '</div>'
            '<div style="font-family:Arial,sans-serif;font-size:12px;color:#8A8A8E;font-style:italic;margin-bottom:18px;padding-bottom:16px;border-bottom:1px solid #E5E5EA;line-height:1.6;">' + deck + '</div>'
            '<table width="100%" cellpadding="0" cellspacing="0">' + rows_html + '</table>'
            '</td></tr>'
        )

    def row(label, body, label_color="#CC0000", last=False):
        border = "" if last else "border-bottom:1px solid #E5E5EA;"
        return (
            '<tr><td style="padding:13px 0;' + border + '">'
            '<div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.16em;text-transform:uppercase;color:' + label_color + ';margin-bottom:5px;">' + label + '</div>'
            '<div style="font-family:Arial,sans-serif;font-size:12px;color:#3A3A3C;line-height:1.75;">' + body + '</div>'
            '</td></tr>'
        )

    # ── SECTION 1: The Signal (Lead Story) ───────────────────────────────
    if top_news and len(top_news) >= 2:
        lead = top_news[0]
        also = top_news[1]
        lead_title = lead.get("title", "")
        lead_source = lead.get("source", "")
        lead_desc = truncate(lead.get("description", ""), 300)
        lead_link = lead.get("link", "")
        lead_ago = time_ago(lead.get("pub_date", ""))
        also_title = also.get("title", "")
        also_source = also.get("source", "")
        also_desc = truncate(also.get("description", ""), 200)
        also_link = also.get("link", "")
        lead_link_a = ""
        lead_link_end = ""
        if lead_link:
            lead_link_a = '<a href="' + lead_link + '" style="color:#CC0000;text-decoration:none;font-size:11px;">Read more &rarr;</a>'
        also_link_a = ""
        if also_link:
            also_link_a = ' <a href="' + also_link + '" style="color:#CC0000;text-decoration:none;font-size:11px;">Read &rarr;</a>'
        signal_rows = (
            row("Top Story · " + lead_source, '<strong>' + lead_title + '</strong><br>' + lead_desc + '<br>' + lead_link_a) +
            row("Also Developing · " + also_source, '<strong>' + also_title + '</strong> — ' + also_desc + also_link_a, last=True)
        )
        sec_1_headline = lead_title
        if len(sec_1_headline) > 80:
            sec_1_headline = sec_1_headline[:77] + "..."
        sec_1_deck = "Live from " + lead_source + ". " + lead_ago + "." if lead_ago else "Live from " + lead_source + "."
    elif top_news and len(top_news) == 1:
        lead = top_news[0]
        lead_title = lead.get("title", "")
        lead_desc = truncate(lead.get("description", ""), 300)
        signal_rows = row("Top Story · " + lead.get("source", ""), '<strong>' + lead_title + '</strong><br>' + lead_desc, last=True)
        sec_1_headline = lead_title
        sec_1_deck = "Live from " + lead.get("source", "") + "."
    else:
        signal_rows = row("Status", "News feeds currently unavailable. Check back later.", last=True)
        sec_1_headline = "Top Stories"
        sec_1_deck = "Fetching live headlines..."

    _expr_1 = section(1, "The Signal · Lead Story", "#CC0000", sec_1_headline, sec_1_deck, signal_rows)

    # ── SECTION 2: Breaking News (digest of stories 3-7) ─────────────────
    if top_news and len(top_news) > 2:
        digest_items = top_news[2:7]
        breaking_rows = build_news_rows(digest_items, "#CC0000")
    else:
        breaking_rows = '<tr><td style="padding:12px 0;"><div style="font-family:Arial,sans-serif;font-size:12px;color:#8A8A8E;">No additional stories available.</div></td></tr>'

    _expr_2 = section(2, "Breaking · News Digest", "#CC0000",
        "Breaking News",
        "Headlines from NPR, BBC, Politico, The Hill, Guardian.",
        breaking_rows)

    # ── SECTION 3: Social Signal ─────────────────────────────────────────
    if social_signal:
        social_cards_list = []
        avatar_colors = ["#007AFF", "#5856D6", "#34C759", "#FF9500", "#CC0000", "#FF2D55", "#AF52DE", "#00C7BE", "#8A8A8E"]
        for idx, post in enumerate(social_signal):
            p_initial = post.get("initial", "?")
            p_handle = post.get("handle", "")
            p_name = post.get("name", "")
            p_text = post.get("text", "")
            p_ago = time_ago_dt(post.get("dt"))
            a_color = avatar_colors[idx % len(avatar_colors)]
            time_span = ""
            if p_ago:
                time_span = ' <span style="font-family:Arial,sans-serif;font-size:9px;color:#8A8A8E;">&middot; ' + p_ago + '</span>'
            social_cards_list.append(
                '<tr><td style="padding:10px 0;border-bottom:1px solid #E5E5EA;">'
                '<table cellpadding="0" cellspacing="0"><tr>'
                '<td width="40" valign="top" style="padding-right:12px;">'
                '<div style="width:36px;height:36px;border-radius:18px;background:' + a_color + ';text-align:center;line-height:36px;font-family:Arial,sans-serif;font-size:16px;font-weight:700;color:#fff;">'
                + p_initial + '</div></td>'
                '<td valign="top">'
                '<div style="font-family:Arial,sans-serif;font-size:12px;font-weight:700;color:#1D1D1F;">'
                + p_name + ' <span style="font-family:\'Courier New\',monospace;font-size:10px;font-weight:400;color:#8A8A8E;">@' + p_handle + '</span>'
                + time_span + '</div>'
                '<div style="font-family:Arial,sans-serif;font-size:12px;color:#3A3A3C;line-height:1.5;margin-top:4px;">'
                + p_text + '</div>'
                '</td></tr></table></td></tr>'
            )
        sec_social_rows = "".join(social_cards_list)
        social_inner = '<table width="100%" cellpadding="0" cellspacing="0">' + sec_social_rows + '</table>'
    else:
        social_inner = '<div style="font-family:Arial,sans-serif;font-size:12px;color:#8A8A8E;">Social feeds unavailable. Nitter instances may be down.</div>'

    _expr_3 = section(3, "Social Signal · Voices", "#007AFF",
        "Social Signal",
        "Latest from key political and media voices.",
        '<tr><td>' + social_inner + '</td></tr>')

    # ── SECTION 4: Medical Frontier ──────────────────────────────────────
    if health_news:
        health_rows = build_news_rows(health_news, "#007AFF")
    else:
        health_rows = '<tr><td style="padding:12px 0;"><div style="font-family:Arial,sans-serif;font-size:12px;color:#8A8A8E;">Health news feeds unavailable.</div></td></tr>'

    _expr_4 = section(4, "Medical Frontier · Health", "#007AFF",
        "Medical & Health News",
        "Live from STAT News, Politico Health, Health Affairs, and Kaiser Health.",
        health_rows)

    # ── SECTION 5: Build Log / AI & Tech ─────────────────────────────────
    if ai_news:
        ai_rows = build_news_rows(ai_news, "#34C759")
    else:
        ai_rows = '<tr><td style="padding:12px 0;"><div style="font-family:Arial,sans-serif;font-size:12px;color:#8A8A8E;">AI/Tech news feeds unavailable.</div></td></tr>'

    _expr_5 = section(5, "Build Log · AI & Tech", "#34C759",
        "AI & Technology",
        "Live from TechCrunch AI, MIT Tech Review, and The Verge.",
        ai_rows)

    # ── SECTION 6: Market Outlook (dynamic week table) ───────────────────
    now = datetime.datetime.now()
    today_weekday = now.weekday()  # 0=Monday
    # Find Monday of this week
    monday = now - datetime.timedelta(days=today_weekday)
    day_names = ["MON", "TUE", "WED", "THU", "FRI"]
    market_week_rows = ""
    for i in range(5):
        d = monday + datetime.timedelta(days=i)
        day_label = day_names[i] + " " + str(d.day)
        market_week_rows += (
            '<tr style="border-bottom:1px solid #E5E5EA;">'
            '<td style="font-family:\'Courier New\',monospace;font-size:9px;color:#8A8A8E;padding:9px 14px 9px 0;vertical-align:top;">' + day_label + '</td>'
            '<td style="font-family:Arial,sans-serif;font-size:11px;font-weight:600;color:#1D1D1F;padding:9px 14px 9px 0;vertical-align:top;">Market Open</td>'
            '<td style="font-family:Arial,sans-serif;font-size:11px;color:#3A3A3C;padding:9px 0;vertical-align:top;line-height:1.5;">Regular session 9:30 AM — 4:00 PM ET</td>'
            '</tr>'
        )

    sec_6_market = (
        '<tr><td style="height:2px;background:#F5F5F7;"></td></tr>'
        '<tr><td style="background:#fff;border:1px solid #E5E5EA;padding:28px 28px 24px;">'
        '<table cellpadding="0" cellspacing="0" style="margin-bottom:18px;"><tr>'
        '<td style="font-family:Georgia,serif;font-size:56px;font-weight:900;color:#1D1D1F;line-height:1;opacity:0.30;padding-right:12px;vertical-align:middle;">06</td>'
        '<td style="vertical-align:middle;border-left:2px solid #8A8A8E;padding-left:10px;">'
        '<div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.2em;text-transform:uppercase;color:#8A8A8E;">Market Outlook · Week of ' + date_str + '</div>'
        '</td></tr></table>'
        '<table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">'
        '<tr style="border-bottom:1px solid #E5E5EA;">'
        '<td style="font-family:\'Courier New\',monospace;font-size:8px;font-weight:700;color:#8A8A8E;padding:0 14px 8px 0;width:68px;">DAY</td>'
        '<td style="font-family:\'Courier New\',monospace;font-size:8px;font-weight:700;color:#8A8A8E;padding:0 14px 8px 0;">EVENT</td>'
        '<td style="font-family:\'Courier New\',monospace;font-size:8px;font-weight:700;color:#8A8A8E;padding:0 0 8px;">SIGNAL</td>'
        '</tr>'
        + market_week_rows +
        '</table></td></tr>'
    )

    # ── SECTION 7: South Florida ─────────────────────────────────────────
    if sfla_news:
        sfla_rows = build_news_rows(sfla_news, "#5856D6")
    else:
        sfla_rows = '<tr><td style="padding:12px 0;"><div style="font-family:Arial,sans-serif;font-size:12px;color:#8A8A8E;">South Florida news feeds unavailable.</div></td></tr>'

    _expr_7 = section(7, "South Florida · Local", "#5856D6",
        "South Florida News",
        "Live from WLRN, NBC6, CBS Miami, and WSVN.",
        sfla_rows)

    # ── SECTION 8: Reddit Pulse ──────────────────────────────────────────
    if reddit_pulse:
        reddit_rows_list = []
        for rp in reddit_pulse:
            r_sub = rp.get("subreddit", "")
            r_title = rp.get("title", "")
            r_score = rp.get("score", 0)
            r_comments = rp.get("comments", 0)
            r_link = rp.get("permalink", "")
            if r_score >= 1000:
                score_str = str(round(r_score / 1000, 1)) + "k"
            else:
                score_str = str(r_score)
            link_open = ""
            link_close = ""
            if r_link:
                link_open = '<a href="' + r_link + '" style="color:#1D1D1F;text-decoration:none;">'
                link_close = '</a>'
            reddit_rows_list.append(
                '<tr><td style="padding:12px 0;border-bottom:1px solid #E5E5EA;">'
                '<table width="100%" cellpadding="0" cellspacing="0"><tr>'
                '<td width="60" valign="top" style="padding-right:12px;">'
                '<div style="background:#FF9500;border-radius:4px;padding:4px 8px;text-align:center;">'
                '<div style="font-family:Arial,sans-serif;font-size:12px;font-weight:700;color:#fff;">&#9650; ' + score_str + '</div>'
                '</div></td>'
                '<td valign="top">'
                '<div style="font-family:\'Courier New\',monospace;font-size:9px;font-weight:700;letter-spacing:0.08em;color:#FF9500;margin-bottom:3px;">'
                + r_sub + '</div>'
                '<div style="font-family:Arial,sans-serif;font-size:13px;font-weight:600;color:#1D1D1F;line-height:1.4;">'
                + link_open + r_title + link_close + '</div>'
                '<div style="font-family:Arial,sans-serif;font-size:10px;color:#8A8A8E;margin-top:3px;">'
                + str(r_comments) + ' comments</div>'
                '</td></tr></table></td></tr>'
            )
        reddit_inner = "".join(reddit_rows_list)
    else:
        reddit_inner = '<tr><td style="padding:12px 0;"><div style="font-family:Arial,sans-serif;font-size:12px;color:#8A8A8E;">Reddit feeds unavailable.</div></td></tr>'

    _expr_8 = section(8, "Reddit Pulse · Community", "#FF9500",
        "Reddit Pulse",
        "Top posts from key communities.",
        reddit_inner)

    # ── SECTION 9: Aviation ──────────────────────────────────────────────
    # Go/No-Go from TAF
    if taf:
        taf_status = taf.get("status", "UNKNOWN")
        taf_color = taf.get("status_color", "#8A8A8E")
        taf_reason = taf.get("reason", "")
        go_nogo_html = (
            '<div style="font-family:Arial,sans-serif;font-size:12px;font-weight:700;color:' + taf_color + ';">' + taf_status + '</div>'
            '<div style="font-family:Arial,sans-serif;font-size:10px;color:#3A3A3C;">' + taf_reason + '</div>'
        )
    else:
        go_nogo_html = (
            '<div style="font-family:Arial,sans-serif;font-size:12px;font-weight:700;color:#8A8A8E;">TAF unavailable</div>'
            '<div style="font-family:Arial,sans-serif;font-size:10px;color:#8A8A8E;">Check aviationweather.gov for latest forecast</div>'
        )

    metar_table = metar_rows_html(metar)

    sec_9_aviation = (
        '<tr><td style="height:2px;background:#F5F5F7;"></td></tr>'
        '<tr><td style="background:#fff;border:1px solid #E5E5EA;padding:28px 28px 24px;">'
        '<table cellpadding="0" cellspacing="0" style="margin-bottom:18px;"><tr>'
        '<td style="font-family:Georgia,serif;font-size:56px;font-weight:900;color:#007AFF;line-height:1;opacity:0.30;padding-right:12px;vertical-align:middle;">09</td>'
        '<td style="vertical-align:middle;border-left:2px solid #007AFF;padding-left:10px;">'
        '<div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.2em;text-transform:uppercase;color:#007AFF;">Aviation · IFR Track</div>'
        '</td></tr></table>'
        '<table width="100%" cellpadding="0" cellspacing="0"><tr>'
        '<td width="50%" valign="top" style="padding-right:16px;">'
        '<div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.14em;text-transform:uppercase;color:#007AFF;margin-bottom:10px;">Go / No-Go (KFLL TAF)</div>'
        '<table width="100%" cellpadding="0" cellspacing="0">'
        '<tr><td style="padding:8px 0;border-bottom:1px solid #E5E5EA;">'
        '<div style="font-family:\'Courier New\',monospace;font-size:8px;color:#8A8A8E;margin-bottom:2px;">NEXT 12 HOURS</div>'
        + go_nogo_html +
        '</td></tr></table>'
        '<div style="margin-top:12px;font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.14em;text-transform:uppercase;color:#007AFF;margin-bottom:10px;">METAR</div>'
        '<table cellpadding="0" cellspacing="0">' + metar_table + '</table>'
        '</td>'
        '<td width="50%" valign="top" style="padding-left:16px;border-left:1px solid #E5E5EA;">'
        '<div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.14em;text-transform:uppercase;color:#007AFF;margin-bottom:10px;">Alternate Minimums</div>'
        '<table cellpadding="0" cellspacing="0" width="100%">'
        '<tr><td style="font-family:\'Courier New\',monospace;font-size:12px;font-weight:700;color:#1D1D1F;padding:5px 14px 5px 0;">KFLL</td><td style="font-family:\'Courier New\',monospace;font-size:11px;color:#8A8A8E;">600-2</td></tr>'
        '<tr><td style="font-family:\'Courier New\',monospace;font-size:12px;font-weight:700;color:#1D1D1F;padding:5px 14px 5px 0;">KPMP</td><td style="font-family:\'Courier New\',monospace;font-size:11px;color:#8A8A8E;">800-2</td></tr>'
        '<tr><td style="font-family:\'Courier New\',monospace;font-size:12px;font-weight:700;color:#1D1D1F;padding:5px 14px 5px 0;">KFXE</td><td style="font-family:\'Courier New\',monospace;font-size:11px;color:#8A8A8E;">900-2</td></tr>'
        '</table>'
        '</td></tr></table>'
        '</td></tr>'
    )

    # ── ASSEMBLE FULL HTML ───────────────────────────────────────────────
    live_url_display = LIVE_URL.replace('https://', '')
    html = (
        '<!DOCTYPE html>'
        '<html lang="en"><head><meta charset="UTF-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1.0">'
        '<title>The Barringer Brief — ' + date_str + '</title>'
        '</head>'
        '<body style="margin:0;padding:0;background:#F5F5F7;">'
        '<table width="100%" cellpadding="0" cellspacing="0" style="background:#F5F5F7;padding:28px 0;">'
        '<tr><td align="center">'
        '<table width="620" cellpadding="0" cellspacing="0" style="max-width:620px;width:100%;">'

        # HEADER
        '<tr><td style="background:#0A2342;padding:10px 24px;border-radius:8px 8px 0 0;">'
        '<table width="100%" cellpadding="0" cellspacing="0"><tr>'
        '<td style="font-family:Georgia,serif;font-size:12px;font-weight:700;color:#fff;">The Barringer Brief</td>'
        '<td align="right" style="font-family:\'Courier New\',monospace;font-size:8px;color:rgba(255,255,255,0.85);letter-spacing:0.08em;text-transform:uppercase;">' + date_str + '</td>'
        '<td align="right" style="padding-left:12px;"><span style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;background:#CC0000;color:#fff;padding:3px 8px;border-radius:3px;letter-spacing:0.06em;text-transform:uppercase;">DAILY BRIEF</span></td>'
        '</tr></table></td></tr>'

        # MASTHEAD
        '<tr><td style="background:#fff;padding:32px 28px 22px;text-align:center;border-left:1px solid #E5E5EA;border-right:1px solid #E5E5EA;">'
        '<div style="font-family:Georgia,serif;font-size:42px;font-weight:900;color:#1D1D1F;letter-spacing:-1.5px;line-height:1;">The Barringer Brief</div>'
        '<div style="font-family:Arial,sans-serif;font-size:9px;letter-spacing:0.32em;text-transform:uppercase;color:#8A8A8E;margin-top:12px;">Intelligence &nbsp;·&nbsp; Systems &nbsp;·&nbsp; Medicine &nbsp;·&nbsp; Power</div>'
        '<div style="width:24px;height:2px;background:#CC0000;margin:14px auto 12px;"></div>'
        '<div style="font-family:Arial,sans-serif;font-size:12px;color:#8A8A8E;">Good Morning, Jadie &nbsp;·&nbsp; ' + date_str + '</div>'
        '</td></tr>'

        # WEATHER + METAR
        '<tr><td style="background:#F5F5F7;border:1px solid #E5E5EA;border-top:none;padding:20px 28px;">'
        '<div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.2em;text-transform:uppercase;color:#3A3A3C;margin-bottom:14px;padding-bottom:10px;border-bottom:1px solid #E5E5EA;">Live Conditions</div>'
        '<table width="100%" cellpadding="0" cellspacing="0"><tr>'
        + wx_html +
        '<td style="vertical-align:top;padding-left:24px;">'
        '<div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.16em;text-transform:uppercase;color:#3A3A3C;margin-bottom:10px;">METAR</div>'
        '<table cellpadding="0" cellspacing="0">' + metar_table + '</table>'
        '</td></tr></table></td></tr>'

        # MARKETS
        '<tr><td style="background:#fff;border:1px solid #E5E5EA;border-top:none;padding:16px 16px;">'
        '<div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.2em;text-transform:uppercase;color:#8A8A8E;margin-bottom:14px;padding-bottom:10px;border-bottom:1px solid #E5E5EA;">Markets</div>'
        '<table width="100%" cellpadding="0" cellspacing="0"><tr>' + mkt_cells + '</tr></table>'
        '</td></tr>'

        # YOUR WEEK
        '<tr><td style="background:#fff;border:1px solid #E5E5EA;border-top:none;padding:20px 28px 16px;">'
        '<div style="font-family:Arial,sans-serif;font-size:8px;font-weight:700;letter-spacing:0.2em;text-transform:uppercase;color:#8A8A8E;margin-bottom:14px;padding-bottom:10px;border-bottom:1px solid #E5E5EA;">Your Week · Calendar</div>'
        + cal_html +
        '</td></tr>'

        # SECTIONS
        + _expr_1
        + _expr_2
        + _expr_3
        + _expr_4
        + _expr_5
        + sec_6_market
        + _expr_7
        + _expr_8
        + sec_9_aviation +

        # FOOTER
        '<tr><td style="height:8px;background:#F5F5F7;"></td></tr>'
        '<tr><td style="background:#0A2342;padding:24px 28px;text-align:center;border-radius:0 0 8px 8px;">'
        '<div style="font-family:Georgia,serif;font-size:16px;font-weight:900;color:#fff;letter-spacing:-0.5px;">The Barringer Brief</div>'
        '<div style="font-family:Arial,sans-serif;font-size:8px;letter-spacing:0.24em;text-transform:uppercase;color:rgba(255,255,255,0.7);margin-top:6px;">Intelligence · Systems · Medicine · Power</div>'
        '<div style="width:20px;height:1px;background:#CC0000;margin:12px auto;"></div>'
        '<a href="' + LIVE_URL + '" style="font-family:Arial,sans-serif;font-size:10px;font-weight:600;color:#fff;text-decoration:none;letter-spacing:0.04em;">' + live_url_display + '</a>'
        '</td></tr>'

        '</table></td></tr></table>'
        '</body></html>'
    )
    return html

# ── SEND VIA RESEND (uses curl — bypasses urllib TLS fingerprint blocks) ─────
def send_email(subject, html):
    payload = json.dumps({
        "from":    FROM_ADDRESS,
        "to":      [RECIPIENT],
        "subject": subject,
        "html":    html,
    })
    try:
        result = subprocess.run(
            [
                "curl", "-s", "-X", "POST",
                "https://api.resend.com/emails",
                "-H", "Authorization: Bearer " + RESEND_API_KEY,
                "-H", "Content-Type: application/json",
                "-d", payload,
            ],
            capture_output=True, text=True, timeout=20
        )
        if result.returncode != 0:
            log(f"[RESEND ERROR] curl failed: {result.stderr}")
            return None
        data = json.loads(result.stdout)
        if "id" in data:
            return data["id"]
        log(f"[RESEND ERROR] {result.stdout}")
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
    taf             = fetch_taf()
    markets         = fetch_markets()
    calendar_events = fetch_calendar()
    top_news        = fetch_top_news()
    health_news     = fetch_health_news()
    ai_news         = fetch_ai_news()
    sfla_news       = fetch_sfla_news()
    social_signal   = fetch_social_signal()
    reddit_pulse    = fetch_reddit_pulse()

    log(f"  Weather:  {'OK' if weather else 'FAILED'}")
    log(f"  METAR:    {len(metar)} airports")
    log(f"  TAF:      {'OK' if taf else 'FAILED'}")
    log(f"  Markets:  {len(markets)} tickers")
    log(f"  Calendar: {len(calendar_events)} events")
    log(f"  News:     {len(top_news)} stories")
    log(f"  Health:   {len(health_news)} stories")
    log(f"  AI/Tech:  {len(ai_news)} stories")
    log(f"  SFLA:     {len(sfla_news)} stories")
    log(f"  Social:   {len(social_signal)} accounts")
    log(f"  Reddit:   {len(reddit_pulse)} subreddits")

    html     = build_email_html(weather, metar, taf, markets, calendar_events, date_str,
                                top_news, health_news, ai_news, sfla_news,
                                social_signal, reddit_pulse)
    email_id = send_email(subject, html)

    if email_id:
        log(f"  Email sent to {RECIPIENT} (id: {email_id})")
    else:
        log("  Email FAILED — check ~/Library/Logs/barringer-brief.log")
        sys.exit(1)

if __name__ == "__main__":
    main()
