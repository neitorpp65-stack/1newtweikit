#!/usr/bin/env python3
# main.py (single-file orchestrator with BLOCK markers)
from __future__ import annotations

# === BLOCK 0 START: IMPORTS & GLOBALS ===
import os
import sys
import time
import json
import pickle
import shutil
import traceback
import platform
import threading
import atexit
from typing import Optional, Dict, Any, List, Tuple
from urllib.parse import quote, urlparse, urlunparse, unquote_plus, parse_qs
from pathlib import Path
import importlib
import importlib.util
# selenium / psutil (guarded)
try:
    import psutil
except Exception:
    psutil = None

try:
    import selenium
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, WebDriverException
    from selenium.webdriver.common.action_chains import ActionChains
except Exception:
    webdriver = Service = Options = By = Keys = WebDriverWait = EC = TimeoutException = None
    WebDriverException = ActionChains = None
# === BLOCK 0 END ===


# === BLOCK 1 START: CONSTANTS / PATHS / SETTINGS / TIMERS / SLEEP_CFG / USERS / SEARCH_KEYS ===
PAGE_LOAD_TIMEOUT_DEFAULT = int(os.environ.get("PAGE_LOAD_TIMEOUT", 60))
WEBDRIVER_WAIT_BASE_DEFAULT = float(os.environ.get("WEBDRIVER_WAIT_BASE", 0.01))

USERS = ["rony24124", "rosi24124", "juli24124", "jimbo24124"]
SEARCH_KEYS = [
    "1moonmanystars",
    "itsreika0 OR 1moonmanystars OR WALTATMARS OR mstifltor OR Snowy28Z0 OR ELVATILLO7U7 OR WhoIsKasaneTeto OR YOSHIO_HOSHINO",
    "list:1872905568597217525",
    "list:1835269014873907226",
    "catbriar77"
]
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 4000))

SETTINGS = {
    "HEADLESS_MODE": False,
    "BINARY_CONVERSION": True,
    "multi_instance": 1,
    "num_tabs": 2,
    "HIDE_CHROME_INFOBAR": True,
    "SPEED_UP": True,
    "WEBDRIVER_MODE": "both",
    "WEBDRIVER_PRE_SLEEP": 0.5,
    "Duplicar": 0,
    "EXTRACTION_LIMIT": 3,
    "ENABLE_DARK_MODE": True,
    "reply_intent": False,
    "FALLBACK_FIRST": "newtweet",
    "CPU_LIMIT_PERCENT": int(os.environ.get("CPU_LIMIT_PERCENT", 10)),
    "ENABLE_THROTTLING": True,
    "THROTTLE_CYCLE_MS": int(os.environ.get("THROTTLE_CYCLE_MS", 100)),
    "THROTTLE_REFRESH_CYCLES": int(os.environ.get("THROTTLE_REFRESH_CYCLES", 20)),
    "OVERRIDE_SLEEP": float(os.environ.get("OVERRIDE_SLEEP", 0.0)),
    "USE_CLIPBOARD_PASTE": True,
    "POST_PASTE_SLEEP": 0.3,
    "ERROR_RETRY_CLICK": True,
    "REPLY_TAB_SUSPEND": True,
    "ERROR_RETRY_MAX_ATTEMPTS": 3,
    "ERROR_RETRY_FALLBACK_RELOAD": True,
    "CHUNK_SIZE": CHUNK_SIZE,
}

TIMERS = {
    "PAGE_LOAD_TIMEOUT": PAGE_LOAD_TIMEOUT_DEFAULT,
    "GET_RETRIES": 3,
    "GET_RETRY_DELAY": 0.01,
    "WEBDRIVER_WAIT_BASE": WEBDRIVER_WAIT_BASE_DEFAULT,
    "WEBDRIVER_PRE_SLEEP": 0.03,
    "FALLBACK_COMPOSER_WAIT": 0.5,
    "SEND_BUTTON_WAIT": 0.3,
    "SEND_BUTTON_ATTEMPTS": 5,
    "COMPOSER_DISAPPEAR_TIMEOUT": 0.5,
    "CONNECTION_RETRIES": 2,
    "CONNECTION_RETRY_DELAY": 0.01,
    "RESTART_INTERVAL": 1250,
    "SLEEP_DELAY": 0.1,
    "SLEEP_CHROME": 14,
    "EXTRA_SCROLLS": 3,
    "SCROLL_DOWN": 580,
    "WAIT_SCROLL": 0.01,
    "PERIOD": 15.0,
    "ACTION_COOLDOWN": 0.01,
    "INITIAL_PURGE_INTERVAL": 2600,
    "SCROLL_UP_INTVL": 4,
    "INACTIVITY_ADD": 0.01,
    "INACTIVITY_TRIGGER": 2300,
    "INTENT_COOLDOWN": 0.01,
    "EXTRA_SLEEP_POST_LAUNCH": 0.01,
    "CLIPBOARD_TIMEOUT": 1.01,
    "LETTER_BY_LETTER_DELAY": 0.0035,
}

SLEEP_CFG = {
    "post_kill": 0.01,
    "wait_redirect": 5.01,
    "post_load_tab": TIMERS["SLEEP_CHROME"],
    "post_load_replies": TIMERS["SLEEP_CHROME"],
    "post_extract": 0.01,
    "post_scroll": max(0.01, TIMERS.get("WAIT_SCROLL", 0.01)),
    "post_action_cooldown": TIMERS.get("ACTION_COOLDOWN", 0.01),
    "period_base": TIMERS.get("PERIOD", 15.0),
}

BASE_DIR = Path(__file__).resolve().parent
IS_WINDOWS = platform.system() == "Windows"
IS_TERMUX = os.path.exists("/data/data/com.termux")
SCRIPTS_DIR = (BASE_DIR / "scriptson") if IS_WINDOWS else (Path("/storage/emulated/0/scripts") if IS_TERMUX else Path.home() / "scripts")
SCRIPTS_DIR.mkdir(parents=True, exist_ok=True)

CHROME_ROOT = BASE_DIR / "chrome"
CHROME_DRIVER = str(CHROME_ROOT / "chromedriver.exe") if IS_WINDOWS else "/data/data/com.termux/files/usr/bin/chromedriver"
CHROME_BINARY = str(CHROME_ROOT / "Antimalware.exe") if IS_WINDOWS else None

CHROME_PROFILE_DIRS = {}
if IS_WINDOWS:
    for u in USERS:
        p = SCRIPTS_DIR / f"{u}_chrome"; p.mkdir(parents=True, exist_ok=True); CHROME_PROFILE_DIRS[u] = str(p)
else:
    PROFILE_BASE = BASE_DIR / "chrome_native_profiles"
    PROFILE_BASE.mkdir(parents=True, exist_ok=True)
    for u in USERS:
        p = PROFILE_BASE / u; p.mkdir(parents=True, exist_ok=True); CHROME_PROFILE_DIRS[u] = str(p)

LINKS_DIR = SCRIPTS_DIR / "links"; LINKS_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_DIR = SCRIPTS_DIR / "backup"; BACKUP_DIR.mkdir(parents=True, exist_ok=True)

JSON_PATHS = {
    "user": str(SCRIPTS_DIR / "user_messages.json"),
    "random": str(SCRIPTS_DIR / "mensajes_aleatorios.json"),
    "binary": str(SCRIPTS_DIR / "binaryaleatorio.json")
}
POOL_PATH = str(BASE_DIR / "msg_pools.pkl")
USED_COUNTS_FMT = str(LINKS_DIR / "used_counts_inst{inst}.pkl")
ROT_PATH = str(SCRIPTS_DIR / "rotation_state.json")
LOG_PATH = str(BASE_DIR / "bot.log")
CACHE_STORAGE_PATH = str(SCRIPTS_DIR / "storage_cache.json")
CACHE_DIR = str(SCRIPTS_DIR / "chrome_cache") if IS_WINDOWS else "/data/data/com.termux/files/home/chrome_cache"
os.makedirs(CACHE_DIR, exist_ok=True)
# === BLOCK 1 END ===


# === BLOCK 1B START: CPU LIMITER (BES-LIKE) ===
_CPU_LIMITERS: Dict[int, "ChromeCpuLimiter"] = {}
_CPU_LIMITERS_LOCK = threading.Lock()


class ChromeCpuLimiter:
    """CPU limiter tipo BES: suspende/reanuda procesos Chrome por duty-cycle."""

    def __init__(self, root_pid: int, limit_percent: int = 10, cycle_ms: int = 100, inst_id: int = 1):
        self.root_pid = int(root_pid)
        self.limit_percent = max(1, min(100, int(limit_percent)))
        self.cycle_ms = max(30, int(cycle_ms))
        self.inst_id = inst_id
        self._stop_evt = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._processes: Dict[int, Any] = {}
        self._refresh_countdown = 0

    def _discover(self):
        if not psutil:
            return
        found: Dict[int, Any] = {}
        try:
            root = psutil.Process(self.root_pid)
            candidates = [root] + root.children(recursive=True)
        except Exception:
            candidates = []
        for proc in candidates:
            try:
                name = (proc.name() or "").lower()
                if "chrome" in name or "chromium" in name:
                    found[proc.pid] = proc
            except Exception:
                continue
        self._processes = found

    def _suspend(self):
        for pid, proc in list(self._processes.items()):
            try:
                if proc.is_running():
                    proc.suspend()
            except Exception:
                self._processes.pop(pid, None)

    def _resume(self):
        for pid, proc in list(self._processes.items()):
            try:
                if proc.is_running():
                    proc.resume()
            except Exception:
                self._processes.pop(pid, None)

    def _run(self):
        self._discover()
        active_ms = max(5, int(self.cycle_ms * (self.limit_percent / 100.0)))
        sleep_ms = max(0, self.cycle_ms - active_ms)
        refresh_every = max(3, int(SETTINGS.get("THROTTLE_REFRESH_CYCLES", 20)))
        while not self._stop_evt.is_set():
            self._refresh_countdown -= 1
            if self._refresh_countdown <= 0:
                self._discover()
                self._refresh_countdown = refresh_every
            if sleep_ms > 0:
                self._suspend()
                self._stop_evt.wait(sleep_ms / 1000.0)
                self._resume()
            self._stop_evt.wait(active_ms / 1000.0)
        self._resume()

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name=f"cpu-limiter-inst{self.inst_id}", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_evt.set()
        self._resume()


def start_cpu_limiter(driver_pid: Optional[int], inst_id: int):
    if not psutil or not driver_pid or not SETTINGS.get("ENABLE_THROTTLING"):
        return
    percent = max(1, min(100, int(SETTINGS.get("CPU_LIMIT_PERCENT", 10))))
    cycle_ms = max(30, int(SETTINGS.get("THROTTLE_CYCLE_MS", 100)))
    with _CPU_LIMITERS_LOCK:
        old = _CPU_LIMITERS.pop(inst_id, None)
        if old:
            old.stop()
        limiter = ChromeCpuLimiter(driver_pid, percent, cycle_ms, inst_id)
        limiter.start()
        _CPU_LIMITERS[inst_id] = limiter
    log(f"[MASTER][INST{inst_id}] CPU limiter activo (objetivo~{percent}% ciclo={cycle_ms}ms, tipo BES)")


def stop_cpu_limiter(inst_id: int):
    with _CPU_LIMITERS_LOCK:
        lim = _CPU_LIMITERS.pop(inst_id, None)
    if lim:
        lim.stop()


def stop_all_cpu_limiters():
    with _CPU_LIMITERS_LOCK:
        items = list(_CPU_LIMITERS.items())
        _CPU_LIMITERS.clear()
    for _, lim in items:
        lim.stop()


atexit.register(stop_all_cpu_limiters)
# === BLOCK 1B END ===


# === BLOCK 2 START: LOGGING & FILE HELPERS ===
def _append_log_file(line: str):
    try:
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        with open(LOG_PATH, "a", encoding="utf-8") as fh:
            fh.write(f"{ts} {line}\n")
    except Exception:
        pass

def log(s: str):
    try:
        print(s); sys.stdout.flush()
    except:
        pass
    try:
        _append_log_file(s)
    except:
        pass

def load_json(path: Optional[str]) -> List[Any]:
    if not path or not os.path.exists(path):
        log(f"[LOAD] JSON not found: {path}")
        return []
    try:
        data = json.load(open(path, "r", encoding="utf-8-sig"))
        log(f"[LOAD] JSON {os.path.basename(path)} loaded ({len(data) if isinstance(data, list) else 'obj'})")
        return data
    except Exception as e:
        log(f"[WARN] load_json {path}: {e}")
        return []

def tab_paths(inst: int, tab: int) -> Tuple[str, str]:
    pkl = str(LINKS_DIR / f"all_links_inst{inst}_{tab}.pkl")
    txt = str(LINKS_DIR / f"used_links_inst{inst}_{tab}.txt")
    return pkl, txt
# === BLOCK 2 END ===


# === BLOCK 3 START: TIMING HELPERS (scaled_sleep, wdwait, enforce_period) ===
def compute_inactivity_add(global_no_links_start=None):
    try:
        g = global_no_links_start
        if g and (time.time() - g) >= int(TIMERS.get("INACTIVITY_TRIGGER", 300)):
            return float(TIMERS.get("INACTIVITY_ADD", 0))
    except Exception:
        pass
    return 0.0

def scaled_sleep(base_seconds, turbo_factor=1.0, mult=1.0, why="", inst=1, allow_override: bool=True):
    inac_add = compute_inactivity_add(None)
    try:
        base = float(base_seconds or 0)
        sec = base * float(mult) + float(inac_add or 0.0)
    except Exception:
        sec = float(base_seconds or 0) + float(inac_add or 0.0)
    override = float(SETTINGS.get("OVERRIDE_SLEEP", 0.0) or 0.0)
    apply_override = False
    if allow_override and override > 0.01:
        if base_seconds not in (TIMERS.get("SLEEP_CHROME"), TIMERS.get("PERIOD")):
            apply_override = True
    if apply_override:
        sec = float(override)
        why = f"{why} (override to {sec:.2f}s)" if why else f"override to {sec:.2f}s"
    if sec <= 0.0005:
        return
    log(f"[INST{inst}] SLEEP {why}: {sec:.2f}s")
    try:
        time.sleep(sec)
    except Exception:
        pass

def wdwait(driver, timeout):
    mode = SETTINGS.get('WEBDRIVER_MODE', 'fast')
    if isinstance(mode, bool):
        mode = 'fast' if mode else 'sleep'
    mode = str(mode).lower()
    inac_add = compute_inactivity_add(None)
    base_t = float(timeout or TIMERS.get('WEBDRIVER_WAIT_BASE', WEBDRIVER_WAIT_BASE_DEFAULT))
    wd_timeout = max(0.1, base_t + float(inac_add or 0.0))
    if mode == 'fast':
        log(f"[WDWAIT] mode=fast -> timeout={wd_timeout}s")
    elif mode == 'sleep':
        base_sleep = float(TIMERS.get('WEBDRIVER_PRE_SLEEP', 0.5))
        log(f"[WDWAIT] mode=sleep -> pre-sleep {base_sleep + inac_add if inac_add else base_sleep} s then webdriver")
        scaled_sleep(base_sleep, 0.01, 0.01, "pre-WebDriverWait sleep", 0)
    elif mode == 'both':
        base_sleep = float(TIMERS.get('WEBDRIVER_PRE_SLEEP', 0.05))
        log(f"[WDWAIT] mode=both -> pre-sleep {base_sleep + inac_add if inac_add else base_sleep} s then webdriver")
        scaled_sleep(base_sleep, 0.01, 0.01, "pre-WebDriverWait sleep (both mode)", 0)
    if WebDriverWait is None:
        raise RuntimeError("Selenium WebDriverWait not available")
    return WebDriverWait(driver, wd_timeout)

def enforce_period(action_start_ts, inst=1):
    try:
        inactivity_add = compute_inactivity_add(None)
        target = float(TIMERS.get('PERIOD', SLEEP_CFG['period_base'])) + float(inactivity_add or 0.0)
        elapsed = time.time() - float(action_start_ts or 0)
        if elapsed < target:
            rem = target - elapsed
            log(f"[INST{inst}] Period sleep remaining: {rem:.2f}s")
            scaled_sleep(rem, 0.01, 0.01, "period remaining", inst, allow_override=False)
        else:
            log(f"[INST{inst}] Period OK: used {elapsed:.2f}s")
    except Exception as e:
        log(f"[INST{inst}] enforce_period err: {e}")
# === BLOCK 3 END ===


# === BLOCK 4 START: FILE I/O & ROTATION (load/save pickles, used_counts, rotation) ===
def load_rotation_state():
    try:
        if os.path.exists(ROT_PATH):
            return json.load(open(ROT_PATH, "r", encoding="utf-8"))
    except:
        pass
    return {}

def save_rotation_state(state: dict):
    try:
        json.dump(state, open(ROT_PATH, "w", encoding="utf-8"), indent=2)
    except:
        pass

def load_links_tab(inst, tab):
    pkl, _ = tab_paths(inst, tab)
    if not os.path.exists(pkl):
        log(f"[LOAD] inst{inst} tab{tab}: {pkl} not found -> empty")
        return {}
    try:
        d = pickle.load(open(pkl, "rb")) or {}
        log(f"[LOAD] inst{inst} tab{tab}: loaded {len(d)} entries")
    except Exception as e:
        log(f"[WARN] load_links_tab err: {e}")
        return {}
    now = time.time(); out = {}
    for k, v in d.items():
        try:
            if isinstance(v, dict):
                if not v.get('commented') and now - v.get('ts', 0) > TIMERS["INITIAL_PURGE_INTERVAL"]:
                    continue
                out[k] = v
            else:
                ts = float(v or 0)
                if now - ts < TIMERS["INITIAL_PURGE_INTERVAL"]:
                    out[k] = {'ts': ts, 'id': None, 'commented': False, 'attempts': 0, 'last_result': None}
        except:
            continue
    try:
        pickle.dump(out, open(pkl, "wb"))
    except:
        pass
    return out

def save_links_tab(data, inst, tab):
    pkl, _ = tab_paths(inst, tab)
    try:
        pickle.dump(data, open(pkl, "wb"))
    except Exception as e:
        log(f"[WARN] save_links_tab err: {e}")

def append_used_txt(link, inst, tab):
    _, txt = tab_paths(inst, tab)
    try:
        tid = get_tweet_id_from_url(link)
        id_or_link = str(tid or link)
        with open(txt, "a", encoding="utf-8") as f:
            f.write(id_or_link + "\n")
    except Exception as e:
        log(f"[WARN] append_used_txt err: {e}")

def load_used_counts_for_instance(inst):
    pklf = USED_COUNTS_FMT.format(inst=inst)
    if os.path.exists(pklf):
        try:
            return pickle.load(open(pklf, "rb")) or {}
        except Exception as e:
            log(f"[WARN] load_used_counts_for_instance err: {e}")
    counts = {}
    for t in range(int(SETTINGS.get("num_tabs", 1))):
        _, txt = tab_paths(inst, t)
        if not os.path.exists(txt): continue
        try:
            with open(txt, "r", encoding="utf-8") as fh:
                for ln in fh:
                    s = ln.strip()
                    if not s: continue
                    counts[s] = counts.get(s, 0) + 1
        except:
            continue
    try:
        pickle.dump(counts, open(pklf, "wb"))
    except:
        pass
    return counts

def save_used_counts_for_instance(inst, counts):
    pklf = USED_COUNTS_FMT.format(inst=inst)
    try:
        pickle.dump(dict(counts), open(pklf, "wb"))
    except Exception as e:
        log(f"[WARN] save_used_counts_for_instance err: {e}")
# === BLOCK 4 END ===


# === BLOCK 5 START: SMALL UTILITIES (normalize, id extraction) ===
def get_tweet_id_from_url(url: str) -> Optional[str]:
    try:
        if not url: return None
        p = urlparse(url).path; parts = [x for x in p.split("/") if x]
        for i in range(len(parts)-1):
            if parts[i] in ("status","statuses") and parts[i+1].isdigit(): return parts[i+1]
        for i in range(len(parts)-2):
            if parts[i] == "web" and parts[i+1] == "status" and parts[i+2].isdigit(): return parts[i+2]
        for part in reversed(parts):
            if part.isdigit() and len(part) >= 6: return part
    except: pass
    return None

def normalize_tweet_url(url: str) -> str:
    try:
        if not url: return ""
        parsed = urlparse(url.strip())
        scheme = parsed.scheme.lower() if parsed.scheme else "https"
        netloc = (parsed.netloc or "").lower()
        if netloc.startswith("www."): netloc = netloc[4:]
        if netloc.count('.') >= 2 and netloc.split('.',1)[0] in ("mobile","m"):
            netloc = netloc.split('.',1)[1]
        path = parsed.path or ""
        if path.endswith("/"): path = path[:-1]
        canonical = urlunparse((scheme, netloc, path, "", "", ""))
        return canonical
    except:
        return url or ""
# === BLOCK 5 END ===


# === BLOCK 6 START: CHROME OPTIONS & DRIVER CREATION ===
def get_chrome_options(profile_dir: Optional[str]):
    if Options is None:
        log("[ERROR] selenium Options not available")
        return None
    opts = Options()
    try:
        opts.add_experimental_option("excludeSwitches", ["enable-logging"]); opts.add_argument("--log-level=3"); opts.add_argument("--disable-gpu")
    except Exception:
        pass
    if SETTINGS.get("HEADLESS_MODE"):
        try:
            opts.add_argument("--headless=new")
        except Exception:
            opts.add_argument("--headless")
        opts.add_argument("--window-size=800,600")
        opts.add_argument("--no-sandbox")
    if SETTINGS.get("HIDE_CHROME_INFOBAR"): opts.add_argument("--disable-infobars")
    if SETTINGS.get("ENABLE_DARK_MODE"):
        try:
            opts.add_argument("--force-dark-mode")
            opts.add_argument("--enable-features=WebUIDarkMode")
            opts.add_argument("--blink-settings=darkMode=1")
        except: pass
    if CHROME_BINARY:
        try: opts.binary_location = CHROME_BINARY
        except: pass
    if profile_dir:
        try: opts.add_argument(f"--user-data-dir={profile_dir}"); opts.add_argument("--profile-directory=Default")
        except: pass
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-plugins-discovery")
    opts.add_argument("--process-per-site")
    opts.add_argument("--renderer-process-limit=1")
    opts.add_argument("--no-proxy-server")
    return opts

def create_driver(opts, inst_id=1):
    try:
        if webdriver is None:
            log("[ERROR] selenium webdriver not available; cannot create driver")
            return None
        if CHROME_DRIVER and os.path.exists(CHROME_DRIVER):
            try:
                driver = webdriver.Chrome(service=Service(CHROME_DRIVER), options=opts)
            except Exception:
                driver = webdriver.Chrome(options=opts)
        else:
            driver = webdriver.Chrome(options=opts)
        time.sleep(0.2)
        try:
            driver.set_page_load_timeout(TIMERS["PAGE_LOAD_TIMEOUT"])
        except Exception:
            pass
        log(f"[MASTER][INST{inst_id}] Chrome started (driver object ready)")
        scaled_sleep(TIMERS.get("EXTRA_SLEEP_POST_LAUNCH", 0.01), 0.01, 0.01, "post launch stabilize", inst_id)
        driver_pid = None
        try:
            driver_pid = driver.service.process.pid if hasattr(driver, "service") and driver.service else None
        except Exception:
            driver_pid = None
        if SETTINGS.get("CPU_LIMIT_PERCENT", 0) > 0 and psutil and driver_pid:
            try:
                start_cpu_limiter(driver_pid, inst_id)
            except Exception as e:
                log(f"[WARN][INST{inst_id}] no se pudo iniciar CPU limiter: {e}")
        if psutil:
            try:
                current_process = psutil.Process(os.getpid())
                try:
                    current_process.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
                except Exception:
                    pass
            except Exception:
                pass
        return driver
    except Exception as e:
        log(f"[ERROR] create_driver failed (inst {inst_id}): {e}")
        traceback.print_exc()
        return None
# === BLOCK 6 END ===


# === BLOCK 7 START: POSITIONING & STORAGE HELPERS ===
def _get_windows_work_area():
    try:
        import ctypes
        from ctypes import wintypes
        SPI_GETWORKAREA = 0x0030
        class RECT(ctypes.Structure):
            _fields_ = [('left', wintypes.LONG), ('top', wintypes.LONG), ('right', wintypes.LONG), ('bottom', wintypes.LONG)]
        rect = RECT()
        res = ctypes.windll.user32.SystemParametersInfoW(SPI_GETWORKAREA, 0, ctypes.byref(rect), 0)
        if res:
            return (int(rect.left), int(rect.top), int(rect.right), int(rect.bottom))
    except Exception:
        pass
    return None

def position_window(driver, inst_id=1):
    try:
        total_instances = int(SETTINGS.get("multi_instance", 1) or 1)
        sw = sh = 0
        work_left = work_top = work_right = work_bottom = 0
        work_area = None
        if IS_WINDOWS:
            wa = _get_windows_work_area()
            if wa:
                work_left, work_top, work_right, work_bottom = wa
                sw = work_right - work_left
                sh = work_bottom - work_top
                work_area = True
        if not work_area:
            try:
                import tkinter as tk
                root = tk.Tk(); root.withdraw()
                sw = root.winfo_screenwidth(); sh = root.winfo_screenheight()
                try: root.destroy()
                except: pass
            except Exception:
                sw, sh = 1280, 720
            work_left = 0; work_top = 0
        if total_instances == 1:
            win_w = sw // 2; win_h = sh; win_x = work_left; win_y = work_top
        elif total_instances == 2:
            win_w = sw // 2; win_h = sh
            if inst_id == 1:
                win_x = work_left; win_y = work_top
            else:
                win_x = work_left + sw - win_w; win_y = work_top
        else:
            horiz_margin = int(sw * 0.00)
            vert_margin_top = int(sh * 0.00)
            usable_w = max(300, sw - (2 * horiz_margin))
            usable_h = max(300, sh - (vert_margin_top + 0))
            per_inst_w = max(480, min(int(usable_w / max(1, total_instances)), 1200))
            cols = min(total_instances, max(1, sw // per_inst_w))
            rows = (total_instances + cols - 1) // cols
            col_index = (inst_id - 1) % cols
            row_index = (inst_id - 1) // cols
            win_w = per_inst_w
            win_h = int(usable_h / rows)
            win_x = work_left + horiz_margin + col_index * (win_w)
            win_y = work_top + vert_margin_top + row_index * (win_h)
        try:
            driver.set_window_rect(int(win_x), int(win_y), int(win_w), int(win_h))
            log(f"[MASTER][INST{inst_id}] window positioned x={int(win_x)} y={int(win_y)} w={int(win_w)} h={int(win_h)}")
        except Exception:
            try:
                driver.set_window_position(int(win_x), int(win_y)); driver.set_window_size(int(win_w), int(win_h))
                log(f"[MASTER][INST{inst_id}] window positioned (fallback)")
            except Exception as e:
                log(f"[MASTER][INST{inst_id}] position window failed: {e}")
    except Exception as e:
        log(f"[MASTER][INST{inst_id}] position error: {e}")

def save_page_storage(driver):
    try:
        if driver is None: return
        local_storage = driver.execute_script("return JSON.stringify(window.localStorage);")
        session_storage = driver.execute_script("return JSON.STRINGIFY(window.sessionStorage);")
        cache = {"local": json.loads(local_storage or "{}"), "session": json.loads(session_storage or "{}")}
        with open(CACHE_STORAGE_PATH, "w", encoding="utf-8") as f:
            json.dump(cache, f)
        log("Saved page storage for cache")
    except Exception as e:
        log(f"Save storage err: {e}")

def restore_page_storage(driver):
    try:
        if not os.path.exists(CACHE_STORAGE_PATH):
            return
        with open(CACHE_STORAGE_PATH, "r", encoding="utf-8") as f:
            cache = json.load(f)
        for key, value in cache.get("local", {}).items():
            try:
                driver.execute_script("window.localStorage.setItem(arguments[0], arguments[1]);", key, json.dumps(value) if not isinstance(value, str) else value)
            except Exception:
                pass
        for key, value in cache.get("session", {}).items():
            try:
                driver.execute_script("window.sessionStorage.setItem(arguments[0], arguments[1]);", key, json.dumps(value) if not isinstance(value, str) else value)
            except Exception:
                pass
        log("Restored page storage from cache.")
    except Exception as e:
        log(f"Restore storage err: {e}")
# === BLOCK 7 END ===


# === BLOCK 8 START: STARTUP ORCHESTRATION & SUMMARY (pre-search) ===
def block8_startup_and_summary(inst_id: int = 1) -> Dict[str, Any]:
    log(f"[MASTER][INST{inst_id}] BLOQUE8: startup beginning")

    num_tabs = int(SETTINGS.get("num_tabs", 1) or 1)

    # pick users (simple grouping/rotation)
    assigned_users = []
    try:
        groups = [USERS[i:i+2] for i in range(0, len(USERS), 2)]
        idx = (inst_id - 1) % max(1, len(groups))
        assigned_users = groups[idx] if groups and idx < len(groups) else USERS[:2]
    except Exception:
        assigned_users = USERS[:1] if USERS else []

    rotation_state = {}
    try:
        rotation_state = load_rotation_state()
    except Exception:
        rotation_state = {}
    prev = int(rotation_state.get(str(inst_id), 0) or 0)
    new_index = (prev + 1) % max(1, len(assigned_users)) if assigned_users else 0
    rotation_state[str(inst_id)] = new_index
    try:
        save_rotation_state(rotation_state)
    except:
        pass

    primary_user = assigned_users[new_index] if assigned_users and len(assigned_users) > new_index else (USERS[0] if USERS else None)
    profile_dir = CHROME_PROFILE_DIRS.get(primary_user)

    log(f"[MASTER][INST{inst_id}] users={assigned_users} primary={primary_user} profile={profile_dir}")

    # Load JSON pools
    MSG_USER = MSG_RAND = MSG_BIN = []
    try:
        if JSON_PATHS:
            MSG_USER = load_json(JSON_PATHS.get("user"))
            MSG_RAND = load_json(JSON_PATHS.get("random"))
            MSG_BIN  = load_json(JSON_PATHS.get("binary"))
    except Exception as e:
        log(f"[MASTER][INST{inst_id}] load jsons err: {e}")

    # Load saved links and used counts
    all_links = {}
    used_counts = {}
    try:
        for t in range(num_tabs):
            try:
                all_links[t] = load_links_tab(inst_id, t) or {}
            except Exception as e:
                log(f"[MASTER][INST{inst_id}] load_links_tab error tab{t}: {e}")
                all_links[t] = {}
        used_counts = load_used_counts_for_instance(inst_id) or {}
        used_counts = {str(k): int(v) for k, v in (used_counts or {}).items()}
    except Exception as e:
        log(f"[MASTER][INST{inst_id}] load used counts err: {e}")

    # Create driver
    opts = None
    driver = None
    try:
        opts = get_chrome_options(profile_dir)
    except Exception as e:
        log(f"[MASTER][INST{inst_id}] get_chrome_options err: {e}")
        opts = None
    try:
        driver = create_driver(opts, inst_id) if opts is not None else create_driver(None, inst_id)
    except Exception as e:
        log(f"[MASTER][INST{inst_id}] create_driver err: {e}")
        driver = None

    # Position and restore storage
    try:
        if driver:
            position_window(driver, inst_id)
    except Exception as e:
        log(f"[MASTER][INST{inst_id}] position_window err: {e}")

    try:
        if driver:
            restore_page_storage(driver)
    except Exception as e:
        log(f"[MASTER][INST{inst_id}] restore_page_storage err: {e}")

    # Summary
    log(f"[MASTER][INST{inst_id}] BLOQUE8 finished (pre-search state):")
    log(f"  num_tabs = {num_tabs}")
    log(f"  assigned_users = {assigned_users}")
    log(f"  primary_user = {primary_user}")
    log(f"  profile_dir = {profile_dir}")
    log(f"  driver_created = {bool(driver)}")
    log(f"  jsons: MSG_USER={len(MSG_USER or [])} MSG_RAND={len(MSG_RAND or [])} MSG_BIN={len(MSG_BIN or [])}")
    log(f"  loaded_links_per_tab = {[ (t, len(all_links.get(t, {}))) for t in sorted(all_links.keys()) ]}")
    log(f"  used_counts_total = {len(used_counts)}")

    return {
        "inst_id": inst_id,
        "num_tabs": num_tabs,
        "assigned_users": assigned_users,
        "rotation_state": rotation_state,
        "rotation_index": new_index,
        "primary_user": primary_user,
        "profile_dir": profile_dir,
        "MSG_USER": MSG_USER,
        "MSG_RAND": MSG_RAND,
        "MSG_BIN": MSG_BIN,
        "all_links": all_links,
        "used_counts": used_counts,
        "driver": driver,
        "opts": opts,
    }
# === BLOCK 8 END ===


# === BLOCK G START: GLOBAL ERROR HANDLER ===
def is_driver_fatal_error(exc: Exception) -> bool:
    msg = (str(exc) or "").lower()
    fatal_keywords = [
        "invalid session", "no such session", "session not created",
        "tab crashed", "target closed", "disconnected", "chrome not reachable",
        "chromedriver", "failed to create session", "read timed out", "unknown error"
    ]
    for kw in fatal_keywords:
        if kw in msg:
            return True
    try:
        if WebDriverException is not None and isinstance(exc, WebDriverException):
            return True
    except Exception:
        pass
    return False

def _safe_save_state_generic(driver, inst_id, all_links, used_counts):
    try:
        if driver:
            try:
                save_page_storage(driver)
            except Exception:
                pass
    except Exception:
        pass
    try:
        save_used_counts_for_instance(inst_id, used_counts)
    except Exception:
        pass
    try:
        for t, d in (all_links or {}).items():
            try:
                save_links_tab(d or {}, inst_id, t)
            except Exception:
                pass
    except Exception:
        pass

def global_error_handler(func):
    def wrapper(*args, **kwargs):
        inst_id = kwargs.get("inst_id", (args[0] if args else 1))
        driver = kwargs.get("driver", None)
        all_links = kwargs.get("all_links", {}) or {}
        used_counts = kwargs.get("used_counts", {}) or {}
        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            log(f"[MASTER][INST{inst_id}] interrupted by user")
            raise
        except Exception as e:
            log(f"[MASTER][INST{inst_id}] Global exception captured: {e}")
            traceback.print_exc()
            try:
                if is_driver_fatal_error(e):
                    log(f"[MASTER][INST{inst_id}] Fatal driver error detected -> saving state and restarting")
                    _safe_save_state_generic(driver, inst_id, all_links, used_counts)
                    try:
                        if driver:
                            try: driver.quit()
                            except: pass
                    except:
                        pass
                    time.sleep(0.25)
                    try:
                        os.execv(sys.executable, [sys.executable] + sys.argv)
                    except Exception as ee:
                        log(f"[MASTER][INST{inst_id}] execv failed: {ee}")
                        raise
                else:
                    log(f"[MASTER][INST{inst_id}] Non-fatal -> saving state and re-raising")
                    _safe_save_state_generic(driver, inst_id, all_links, used_counts)
            except Exception as ee:
                log(f"[MASTER][INST{inst_id}] Error during global exception handling: {ee}")
            raise
    return wrapper
# === BLOCK G END ===


# === ENTRYPOINT ===
@global_error_handler
def run_main_instance(inst_id: int = 1):
    state = block8_startup_and_summary(inst_id=inst_id)

    log(f"[MASTER][INST{inst_id}] Startup complete. Continuing to static module to open searches (if available).")

    # Robust import of static.py from same folder (BASE_DIR)
    STATIC_PATH = BASE_DIR / "static.py"
    static = None
    try:
        if STATIC_PATH.exists():
            spec = importlib.util.spec_from_file_location("static", str(STATIC_PATH))
            module = importlib.util.module_from_spec(spec)
            sys.modules["static"] = module
            spec.loader.exec_module(module)
            static = module
        else:
            # fallback to normal import (if module is in path)
            static = importlib.import_module("static")
            importlib.reload(static)
    except Exception as e:
        log(f"[MASTER][INST{inst_id}] ERROR importing static: {e}")
        log(f"[MASTER][INST{inst_id}] Falling back to interactive READY prompt.")
        # fallback interactive prompt
        try:
            while True:
                cmd = input(f"[MASTER][INST{inst_id}] Ready (static import failed). Type 'status' or 'exit': ").strip().lower()
                if cmd in ("", "status"):
                    log(f"[MASTER][INST{inst_id}] STATUS: driver_created={bool(state.get('driver'))} primary_user={state.get('primary_user')}")
                    continue
                if cmd == "exit":
                    log(f"[MASTER][INST{inst_id}] Exiting by user command.")
                    try:
                        drv = state.get("driver")
                        if drv:
                            try: drv.quit()
                            except: pass
                    except:
                        pass
                    sys.exit(0)
                log(f"[MASTER][INST{inst_id}] Comando no reconocido: {cmd}")
        except KeyboardInterrupt:
            log(f"[MASTER][INST{inst_id}] Interrupción por teclado, cerrando.")
            try:
                drv = state.get("driver")
                if drv:
                    try: drv.quit()
                    except: pass
            except:
                pass
        return state

    # attempt to find a neutral entry function in static
    entry_fn = getattr(static, "start_search_phase", None) or getattr(static, "open_searches", None) or getattr(static, "start", None)

    if not entry_fn:
        log(f"[MASTER][INST{inst_id}] static module imported but no known entry function (start_search_phase/open_searches/start).")
        log(f"[MASTER][INST{inst_id}] Falling back to interactive READY prompt.")
        try:
            while True:
                cmd = input(f"[MASTER][INST{inst_id}] Ready. Type 'status' or 'exit': ").strip().lower()
                if cmd in ("", "status"):
                    log(f"[MASTER][INST{inst_id}] STATUS: driver_created={bool(state.get('driver'))} primary_user={state.get('primary_user')}")
                    continue
                if cmd == "exit":
                    log(f"[MASTER][INST{inst_id}] Exiting by user command.")
                    try:
                        drv = state.get("driver")
                        if drv:
                            try: drv.quit()
                            except: pass
                    except:
                        pass
                    sys.exit(0)
                log(f"[MASTER][INST{inst_id}] Comando no reconocido: {cmd}")
        except KeyboardInterrupt:
            log(f"[MASTER][INST{inst_id}] Interrupción por teclado, cerrando.")
            try:
                drv = state.get("driver")
                if drv:
                    try: drv.quit()
                    except: pass
            except:
                pass
        return state

    # Call static entry function, prefer signature (state, keys=..., sleep_cfg=..., timers=...)
    try:
        entry_fn(state, keys=SEARCH_KEYS, sleep_cfg=SLEEP_CFG, timers=TIMERS)
    except TypeError:
        try:
            entry_fn(state)
        except Exception as e:
            log(f"[MASTER][INST{inst_id}] static entry function raised: {e}")
            traceback.print_exc()
            return state
    except Exception as e:
        log(f"[MASTER][INST{inst_id}] static entry function raised: {e}")
        traceback.print_exc()
        return state

    log(f"[MASTER][INST{inst_id}] Returned from static entry (static is now handling the next phase).")
    return state
# === END ENTRYPOINT ===


if __name__ == "__main__":
    inst = 1
    try:
        if len(sys.argv) > 1:
            inst = int(sys.argv[1])
    except:
        inst = 1
    try:
        run_main_instance(inst_id=inst)
    except Exception as e:
        log(f"[MASTER][INST{inst}] run_main_instance finished with unhandled exception: {e}")
        sys.exit(1)
