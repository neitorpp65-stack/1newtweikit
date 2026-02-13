# static.py
# Módulo responsable por abrir los searches/tabs, normalizar y verificar (pre-extract).
from __future__ import annotations

# === BLOCK 9 START: IMPORTS & HELPERS ===
import time
import re
from urllib.parse import quote, urlparse
from typing import Dict, Any, List, Optional
# === BLOCK 9 END ===


# === BLOCK 9A START: util helpers for navigation/resolve ===
def is_url_key(k: Optional[str]) -> bool:
    return bool(k and (k.lower().startswith("http://") or k.lower().startswith("https://")))

def is_id_url(k: Optional[str]) -> bool:
    return bool(k and "/i/user/" in k.lower())

def build_nav_url(key: Optional[str], users_exclude: Optional[List[str]] = None) -> str:
    if not key:
        return "https://x.com"
    k = key.strip()
    if is_url_key(k):
        return k
    excl = ""
    if users_exclude:
        excl = " ".join(f"-from:{u}" for u in users_exclude)
    q = (k + " " + excl).strip()
    return f"https://x.com/search?q={quote(q)}&f=live"

def _fallback_resolve_username(driver, candidate_url=None) -> Optional[str]:
    """
    Intenta obtener username de location, canonical o meta og:url.
    Devuelve username o None.
    """
    try:
        cur = ""
        try:
            cur = (driver.execute_script("return document.location.href;") or "") or ""
        except Exception:
            cur = candidate_url or ""
        # try canonical
        try:
            can = driver.execute_script("var l = document.querySelector('link[rel=\"canonical\"]'); return l ? l.href : '';")
            if can:
                cur = can
        except Exception:
            pass
        # try og:url
        try:
            og = driver.execute_script("var m = document.querySelector('meta[property=\"og:url\"]'); return m ? m.content : '';")
            if og:
                cur = og
        except Exception:
            pass

        if cur:
            p = urlparse(cur)
            parts = [x for x in p.path.split("/") if x]
            if parts:
                cand = parts[0]
                if re.match(r'^[A-Za-z0-9_]{1,15}$', cand) and cand.lower() not in ("i","home","explore","search","notifications","settings"):
                    return cand
    except Exception:
        pass
    return None
# === BLOCK 9A END ===


# === BLOCK 9B START: main entry (start_search_phase) ===
def start_search_phase(state: Dict[str, Any], keys: Optional[List[str]] = None, sleep_cfg: Optional[Dict[str, Any]] = None, timers: Optional[Dict[str, Any]] = None):
    """
    state: dict provided by main.block8_startup_and_summary
    keys: SEARCH_KEYS from main (list)
    sleep_cfg: SLEEP_CFG from main
    timers: TIMERS from main

    - Abre num_tabs pestañas, resuelve usernames cuando la key es url/id-url,
      navega a /with_replies si procede, espera post_load_tab/post_load_replies,
      verifica mismatch (profile vs search) y guarda estado en state['resolved'].
    - Al finalizar se queda en prompt esperando 'next' / 'status' / 'exit'.
    """
    log = state.get("log", globals().get("log")) or globals().get("print")
    if not callable(log):
        def log(*a, **k): print(*a)

    driver = state.get("driver")
    if not driver:
        log("[STATIC] No driver in state. Aborting start_search_phase.")
        return

    num_tabs = int(state.get("num_tabs", 1) or 1)
    assigned_users = state.get("assigned_users", []) or []
    inst_id = state.get("inst_id", 1)

    keys = keys or []
    sleep_cfg = sleep_cfg or {}
    timers = timers or {}

    post_load_tab = float(sleep_cfg.get("post_load_tab", timers.get("SLEEP_CHROME", 14) if timers else 14))
    post_load_replies = float(sleep_cfg.get("post_load_replies", post_load_tab))

    log(f"[STATIC][INST{inst_id}] start_search_phase: opening up to {num_tabs} tabs (keys_available={len(keys)})")

    resolved: Dict[int, Dict[str, Any]] = {}

    def pick_key_for_tab(i: int) -> str:
        if not keys:
            return ""
        return keys[i % len(keys)]

    for idx in range(num_tabs):
        raw_key = pick_key_for_tab(idx)
        nav = build_nav_url(raw_key, assigned_users)
        resolved[idx] = {'is_profile': False, 'username': None, 'replies_url': None, 'raw_key': raw_key, 'verified': False, 'cur': None}
        try:
            uname = None
            if is_id_url(raw_key) or is_url_key(raw_key):
                # Open raw key first to allow redirect and resolution
                try:
                    log(f"[STATIC][INST{inst_id}] TAB{idx}: opening raw url {raw_key}")
                    driver.get(raw_key)
                except Exception as e:
                    log(f"[STATIC][INST{inst_id}] TAB{idx}: driver.get(raw_key) err: {e}")
                time.sleep(post_load_tab)
                try:
                    uname = _fallback_resolve_username(driver, raw_key)
                except Exception:
                    uname = None
                if uname:
                    parsed = urlparse(raw_key)
                    scheme = parsed.scheme or "https"
                    host = parsed.netloc or "x.com"
                    replies = f"{scheme}://{host.rstrip('/')}/{uname}/with_replies"
                    resolved[idx].update({'is_profile': True, 'username': uname, 'replies_url': replies})
                    nav = replies
                    log(f"[STATIC][INST{inst_id}] TAB{idx}: resolved username -> {uname} -> will navigate to {replies}")
                else:
                    nav = build_nav_url(raw_key, assigned_users)
                    log(f"[STATIC][INST{inst_id}] TAB{idx}: username not resolved -> fallback nav {nav}")
            else:
                nav = build_nav_url(raw_key, assigned_users)
                log(f"[STATIC][INST{inst_id}] TAB{idx}: search key -> nav {nav}")
        except Exception as e:
            log(f"[STATIC][INST{inst_id}] TAB{idx} resolution err: {e}")

        # Open/navigate tab
        try:
            if idx == 0:
                log(f"[STATIC][INST{inst_id}] TAB0 -> loading {nav}")
                try: driver.get(nav)
                except Exception as e: log(f"[STATIC][INST{inst_id}] TAB0 get err: {e}")
                time.sleep(post_load_tab)
            else:
                try:
                    driver.execute_script("window.open('about:blank');")
                except Exception:
                    log(f"[STATIC][INST{inst_id}] TAB{idx} could not open blank (script). Trying get on new handle.")
                time.sleep(0.08)
                try:
                    driver.switch_to.window(driver.window_handles[-1])
                except Exception:
                    log(f"[STATIC][INST{inst_id}] TAB{idx} switch_to new window failed -> continuing with current handle")
                log(f"[STATIC][INST{inst_id}] TAB{idx} -> loading {nav}")
                try: driver.get(nav)
                except Exception as e: log(f"[STATIC][INST{inst_id}] TAB{idx} get err: {e}")
                time.sleep(post_load_tab)
        except Exception as e:
            log(f"[STATIC][INST{inst_id}] Error opening tab{idx}: {e}")

        # Verify current url matches expected (profile / replies or search)
        try:
            cur = (driver.execute_script("return document.location.href;") or "").lower()
        except Exception:
            try:
                cur = driver.current_url or ""
            except Exception:
                cur = ""
        cur = (cur or "").lower()
        want = (nav or "").lower()
        ok = False
        try:
            if resolved[idx].get('is_profile') and resolved[idx].get('replies_url'):
                uname_l = (resolved[idx].get('username') or "").lower()
                if "/with_replies" in cur and uname_l and uname_l in cur:
                    ok = True
                else:
                    # try explicit open of replies and wait
                    try:
                        driver.get(resolved[idx]['replies_url'])
                    except Exception:
                        pass
                    time.sleep(post_load_replies)
                    cur2 = (driver.execute_script("return document.location.href;") or "").lower()
                    if "/with_replies" in (cur2 or "") and uname_l and uname_l in (cur2 or ""):
                        ok = True
            else:
                # search verification: check "search" in URL or path from want appears
                if "search" in cur:
                    ok = True
                else:
                    try:
                        parsed_want = urlparse(want)
                        if parsed_want.path and parsed_want.path in cur:
                            ok = True
                    except:
                        pass
        except Exception:
            ok = False

        resolved[idx]['verified'] = bool(ok)
        resolved[idx]['cur'] = cur
        log(f"[STATIC][INST{inst_id}] TAB{idx} verify -> cur='{(cur or '')[:160]}' expected='{(want or '')[:160]}' -> ok={ok}")

    # Save resolved into state for later stages
    state['resolved'] = resolved

    # Summary log
    log(f"[STATIC][INST{inst_id}] All tabs processed. Summary:")
    for i, info in resolved.items():
        log(f"  TAB{i}: raw_key={info.get('raw_key')} username={info.get('username')} verified={info.get('verified')} cur={str(info.get('cur'))[:120]}")

    # Interactive pause: wait for 'next' or 'status' or 'exit'
    try:
        while True:
            cmd = input(f"[STATIC][INST{inst_id}] Tabs ready. type 'next' to continue, 'status' or 'exit': ").strip().lower()
            if cmd in ("", "status"):
                verified_list = [(i, r.get('verified')) for i, r in resolved.items()]
                print(f"[STATIC][INST{inst_id}] status -> verified per tab: {verified_list}")
                continue
            if cmd == "next":
                log(f"[STATIC][INST{inst_id}] Received 'next' — continue to next phase (not implemented here).")
                break
            if cmd == "exit":
                log(f"[STATIC][INST{inst_id}] Exit requested. Leaving tabs open for main to handle shutdown.")
                break
            log(f"[STATIC][INST{inst_id}] Unknown command: {cmd}")
    except KeyboardInterrupt:
        log(f"[STATIC][INST{inst_id}] Keyboard interrupt, leaving tabs open.")
# === BLOCK 9B END ===
