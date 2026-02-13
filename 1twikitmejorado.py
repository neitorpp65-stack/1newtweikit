import os
import sys
import time
import json
import asyncio
import platform
import subprocess
import random
import re
import shutil
import threading
import atexit
from datetime import datetime, timezone
from urllib.parse import quote, unquote
from pathlib import Path

# Selenium Imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException
from webdriver_manager.chrome import ChromeDriverManager 
from twikit import Client

try:
    import psutil
except Exception:
    psutil = None

# ==========================
# RUTAS SISTEMA BASE
# ==========================
IS_WINDOWS = platform.system() == "Windows"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ==========================
# CONFIGURACION DE USUARIO (EDITAR AQUI)
# ==========================

# --- TARGETS Y USUARIOS ---
SEARCH_TARGETS = ["runster_webster OR 1moonmanystars", "Jinxautista OR Plaga_V2P"]
TARGET = " | ".join(SEARCH_TARGETS)
URL_INICIAL = "https://x.com/notifications"
USERS = ["sd50565", "juli24124", "mario152567", "rony24124"]
EXCLUDE_USERS = ["rosi24124"]

# --- CONFIGURACION DE LOGS Y FILTROS (NUEVO) ---
PRINT_DETAILED_LOGS = True      # True: logs detallados de extracción.
PRINT_TEXT_WITH_LINK = True     # True: muestra "| TEXTO: ..." junto al link extraído.

# Filtro único para búsqueda. Ejemplos: "lang:es", "-filter:replies", "filter:replies"
SEARCH_FILTER_QUERY = "lang:es"

# --- LIMITES DE EXTRACCION ---
MAX_TWEETS_FETCH = 30  # Maximo 30 tweets (evita leer tweets viejos masivos)
MAX_TWEET_AGE_HOURS = 10  # No guardar/citar tweets con más de esta antigüedad.

# --- MODO DE EJECUCION ---
MODO_EJECUCION = "HIBRIDO"   
ROTAR_EN_HIBRIDO = False     
MODO_ACCION = "CITA"         
TIPO_CITA = "REPLY"
HEADLESS = False               
MODO_INSTANCIAS_SIMULTANEO = True 

# --- AJUSTE DE INSTANCIAS ---
NUM_INSTANCIAS = 1     
TABS_POR_INSTANCIA = 1 

# --- CONTENIDO ---
CONVERTIR_BINARIO = True
TEXTO_FALLBACK = "Interesting!"

# --- TIEMPOS (SEGUNDOS) ---
PERIOD = 55
POLL_INTERVAL = 20
FETCH_POLL_SECONDS = POLL_INTERVAL
TWEETS_POR_CICLO = 4
SLEEP_BASE = 3.3
SLEEP_CARGA = SLEEP_BASE
SLEEP_ESCRITURA = SLEEP_BASE
SLEEP_POST_CLICK = SLEEP_BASE
SLEEP_REINTENTO = SLEEP_BASE
SLEEP_RECOVERY = SLEEP_BASE
SLEEP_COOKIES = SLEEP_BASE
SLEEP_COUNTDOWN_TICK = 1.0
REINTENTOS_ENVIO = 3
REINTENTOS_CONFIRMACION_ENVIO = 4
REPLY_INTENT_INITIAL_SLEEP = 1.5

# Control de throttling CPU en Chrome (CDP Emulation.setCPUThrottlingRate)
CPU_THROTTLE_MODE = "MODO2"  # modo1=solo al primer click de enviar en reply | modo2=siempre activo (global) | modo3=apagado
CPU_THROTTLE_RATE = 10         # porcentaje objetivo máximo de CPU para Chrome (~10 recomendado)

REPLY_MODE_ALIASES = {"REPLY", "RESPUESTA", "RESPONDER"}

# --- REINICIO ---
AUTO_RESTART = True
TIEMPO_RESTART_HIBRIDO = 15 # Minutos

# ==========================
# CALCULO DE RUTAS (NO TOCAR)
# ==========================
def _resolve_python_root():
    if not IS_WINDOWS:
        return BASE_DIR

    script_path = Path(BASE_DIR).resolve()
    home = Path.home()
    candidates = [
        script_path,
        script_path.parent,
        home / "Desktop" / "Python",
    ]

    for candidate in candidates:
        if (candidate / "chrome").exists() or (candidate / "scriptson").exists() or (candidate / "finished").exists():
            return str(candidate)
    return str(script_path)

PYTHON_ROOT = _resolve_python_root()

if IS_WINDOWS:
    FINISHED_DIR = os.path.join(PYTHON_ROOT, "finished")
    PROFILE_BASE = os.path.join(PYTHON_ROOT, "scriptson")
    CHROME_DIR = os.path.join(PYTHON_ROOT, "chrome")
    CHROME_DRIVER_CANDIDATES = [
        os.path.join(CHROME_DIR, "chromedriver.exe"),
        os.path.join(CHROME_DIR, "antimalware.exe"),
    ]
    CHROME_DRIVER = next((p for p in CHROME_DRIVER_CANDIDATES if os.path.exists(p)), CHROME_DRIVER_CANDIDATES[0])
else:
    FINISHED_DIR = "/storage/emulated/0/scripts/finished"
    PROFILE_BASE = os.path.join(os.environ.get("HOME", ""), "chrome_native_profiles")
    CHROME_DIR = "/data/data/com.termux/files/usr/bin"
    CHROME_DRIVER = os.path.join(CHROME_DIR, "chromedriver")

# Prioridad al archivo binaryaleatorio.json
BINARY_MESSAGES_CANDIDATES = [
    os.path.join(FINISHED_DIR, "binaryaleatorio.json"),
    os.path.join(FINISHED_DIR, "binaryaleatorio.txt"),
    os.path.join(FINISHED_DIR, "mensajes.json"),
]

GLOBAL_QUEUE_FILE = os.path.join(FINISHED_DIR, "cola_links_global.json")
GLOBAL_USED_FILE = os.path.join(FINISHED_DIR, "vistos_global.json")
# Nuevo: archivo meta para guardar fechas y flags por link (no rompe cola/vistos)
GLOBAL_QUEUE_META_FILE = os.path.join(FINISHED_DIR, "cola_links_global_meta.json")

def resolve_messages_file():
    for candidate in BINARY_MESSAGES_CANDIDATES:
        if candidate and os.path.exists(candidate):
            return candidate
    return BINARY_MESSAGES_CANDIDATES[0]

def kill_chrome_processes(log_fn=None, user_label="USUARIO"):
    try:
        if log_fn:
            log_fn(user_label, "Limpiando procesos Chrome...")
        cmd = "taskkill /f /im chrome.exe /t" if IS_WINDOWS else "pkill -f chrome"
        subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as e:
        if log_fn:
            log_fn(user_label, f"Kill chrome error: {e}")

sys.path.append(FINISHED_DIR)
try:
    import smart_utils
except ImportError:
    print("\n[USUARIO] ERROR CRITICO: Ejecuta el script generador del modulo 'smart_utils' primero.\n", flush=True)
    sys.exit(1)



class BesCpuLimiter:
    def __init__(self, root_pid, limit_percent=10, cycle_ms=100):
        self.root_pid = int(root_pid)
        self.limit_percent = max(1, min(100, int(limit_percent)))
        self.cycle_ms = max(30, int(cycle_ms))
        self._stop = threading.Event()
        self._thread = None
        self._procs = {}

    def _discover(self):
        if not psutil:
            return
        found = {}
        try:
            root = psutil.Process(self.root_pid)
            candidates = [root] + root.children(recursive=True)
        except Exception:
            candidates = []
        for p in candidates:
            try:
                name = (p.name() or '').lower()
                if 'chrome' in name or 'chromium' in name:
                    found[p.pid] = p
            except Exception:
                continue
        self._procs = found

    def _suspend(self):
        for pid, p in list(self._procs.items()):
            try:
                if p.is_running():
                    p.suspend()
            except Exception:
                self._procs.pop(pid, None)

    def _resume(self):
        for pid, p in list(self._procs.items()):
            try:
                if p.is_running():
                    p.resume()
            except Exception:
                self._procs.pop(pid, None)

    def _run(self):
        on_ms = max(5, int(self.cycle_ms * (self.limit_percent / 100.0)))
        off_ms = max(0, self.cycle_ms - on_ms)
        ticks = 0
        while not self._stop.is_set():
            ticks += 1
            if ticks % 20 == 1:
                self._discover()
            if off_ms > 0:
                self._suspend()
                self._stop.wait(off_ms / 1000.0)
                self._resume()
            self._stop.wait(on_ms / 1000.0)
        self._resume()

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, daemon=True, name='bes-cpu-limiter')
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._resume()

# ==========================
# CLASES DE UTILIDAD
# ==========================
class Utils:
    _print_lock = threading.Lock()
    SYSTEM_LABEL = "SISTEMA"

    @staticmethod
    def _emit(line):
        with Utils._print_lock:
            print(line, flush=True)

    @staticmethod
    def log(user, msg, instancia_id=None):
        inst = f" [INST-{instancia_id}]" if instancia_id is not None else ""
        Utils._emit(f"[{datetime.now().strftime('%H:%M:%S')}] [{user.upper()}]{inst} {msg}")

    @staticmethod
    def default_user():
        return (USERS[0] if USERS else "usuario").upper()

    @staticmethod
    def detect_browser_path():
        """Intento robusto de detectar ruta del navegador en Windows / Termux / Linux."""
        # Windows: rutas comunes
        try:
            if IS_WINDOWS:
                candidates = [
                    os.path.join(CHROME_DIR, "chrome.exe"),
                    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
                    os.path.join(os.environ.get("LOCALAPPDATA", ""), "Google", "Chrome", "Application", "chrome.exe")
                ]
                for path in candidates:
                    try:
                        if path and os.path.exists(path):
                            return path
                    except:
                        continue
            else:
                # No-Windows: buscar binarios en termux o en PATH
                termux_candidates = [
                    "/data/data/com.termux/files/usr/bin/chromium-browser",
                    "/data/data/com.termux/files/usr/bin/chromium",
                    "/data/data/com.termux/files/usr/bin/google-chrome",
                ]
                for path in termux_candidates:
                    try:
                        if path and os.path.exists(path):
                            return path
                    except:
                        continue

                # Buscar en PATH nombres comunes
                for cmd in ["google-chrome", "chromium", "chromium-browser", "chrome"]:
                    try:
                        found = shutil.which(cmd)
                        if found:
                            return found
                    except:
                        continue
        except:
            pass

        # Si no se detecta, devolver mensaje indicativo (no rompe llamadas)
        return "No detectado (usa ruta por defecto de Selenium)"

    @staticmethod
    def log_global(msg, instancia_id=None):
        Utils.log(Utils.SYSTEM_LABEL, msg, instancia_id)

    @staticmethod
    def print_header():
        Utils.log_global( "========================= CONFIGURACION =========================")
        Utils.log_global( f"OS={platform.system()} | PYTHON={PYTHON_ROOT}")
        Utils.log_global( f"PERFILES={PROFILE_BASE} | COOKIES={FINISHED_DIR}")
        # --- AQUI SE IMPRIMEN LAS RUTAS JSON ---
        Utils.log_global( f"📁 JSON COLA (Pendientes): {GLOBAL_QUEUE_FILE}")
        Utils.log_global( f"📁 JSON VISTOS (Historial): {GLOBAL_USED_FILE}")
        Utils.log_global( f"📁 JSON META (Detalles): {GLOBAL_QUEUE_META_FILE}")
        Utils.log_global( f"CHROME={CHROME_DRIVER} | BROWSER={Utils.detect_browser_path()}")
        Utils.log_global( f"TARGET={TARGET}")
        Utils.log_global( "=================================================================")

    @staticmethod
    def print_runtime_mode(num_instancias):
        modo_instancias = "SIMULTANEA" if MODO_INSTANCIAS_SIMULTANEO else "SECUENCIAL"
        Utils.log_global( "========================= MODO DE EJECUCION ========================")
        Utils.log_global( f"METODO=ACCION:{MODO_ACCION} | TIPO_CITA:{TIPO_CITA}")
        Utils.log_global( f"MODO=INSTANCIAS:{modo_instancias} | TOTAL:{num_instancias}")
        Utils.log_global( f"LOGS DETALLADOS={PRINT_DETAILED_LOGS} | PRINT_TEXT_WITH_LINK={PRINT_TEXT_WITH_LINK} | SEARCH_FILTER_QUERY={SEARCH_FILTER_QUERY!r}")
        Utils.log_global( f"CPU_THROTTLE_MODE={CPU_THROTTLE_MODE} | CPU_THROTTLE_RATE={CPU_THROTTLE_RATE}")
        Utils.log_global( "=================================================================")

    @staticmethod
    async def countdown_async(seconds, info="", user="USUARIO", instancia_id=None):
        """Conteo regresivo limpio que sobrescribe la linea."""
        total = max(0, int(seconds))
        if total <= 0: return
        
        inst = f" [INST-{instancia_id}]" if instancia_id is not None else ""
        prefix = f"[{datetime.now().strftime('%H:%M:%S')}] [{user.upper()}]{inst} ⏳ {info}: "
        
        with Utils._print_lock:
             sys.stdout.write(f"{prefix}{total}s")
             sys.stdout.flush()

        for i in range(total - 1, -1, -1):
            await asyncio.sleep(1)
            with Utils._print_lock:
                sys.stdout.write(f"\r{' ' * 80}\r{prefix}{i}s")
                sys.stdout.flush()
        
        sys.stdout.write("\n")
        sys.stdout.flush()

    @staticmethod
    def load_json(path):
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f: return json.load(f)
            except: pass
        return []

    @staticmethod
    def save_json(path, data):
        with open(path, "w", encoding="utf-8") as f: json.dump(data, f, indent=2)

    @staticmethod
    def detect_display_size():
        try:
            import tkinter as tk
            root = tk.Tk()
            root.withdraw()
            w = int(root.winfo_screenwidth())
            h = int(root.winfo_screenheight())
            root.destroy()
            if w > 0 and h > 0: return w, h
        except: pass

        if not IS_WINDOWS:
            try:
                p = subprocess.run("xdpyinfo | rg dimensions", shell=True, capture_output=True, text=True)
                m = re.search(r"(\d+)x(\d+)", (p.stdout or "") + " " + (p.stderr or ""))
                if m: return int(m.group(1)), int(m.group(2))
            except: pass
        else:
            try:
                ps = r"powershell -NoProfile -Command \"Add-Type -AssemblyName System.Windows.Forms; $s=[System.Windows.Forms.Screen]::PrimaryScreen.Bounds; Write-Output ($s.Width.ToString() + 'x' + $s.Height.ToString())\""
                p = subprocess.run(ps, shell=True, capture_output=True, text=True)
                m = re.search(r"(\d+)x(\d+)", p.stdout or "")
                if m: return int(m.group(1)), int(m.group(2))
            except: pass
        return (1280, 720) if IS_WINDOWS else (1024, 768)

_LINK_STORE_LOCK = threading.Lock()

class MessageBag:
    def __init__(self, path):
        self.path = path
        self.mensajes = []
        self.load()

    def load(self):
        if not os.path.exists(self.path):
            Utils.log_global(f"⚠ ARCHIVO MENSAJES NO ENCONTRADO: {self.path}")
            return
        
        Utils.log_global(f"📂 Cargando mensajes desde: {self.path}")

        raw_content = ""
        encoding_used = "utf-8"
        
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                raw_content = f.read()
        except UnicodeDecodeError:
            Utils.log_global("⚠ Error UTF-8 (posible ñ/tilde). Reintentando con encoding 'latin-1'...")
            try:
                with open(self.path, "r", encoding="latin-1") as f:
                    raw_content = f.read()
                    encoding_used = "latin-1"
            except Exception as e:
                Utils.log_global(f"❌ Error fatal leyendo archivo: {e}")
                return

        try:
            if self.path.endswith(".txt"):
                self.mensajes = [line.strip() for line in raw_content.splitlines() if line.strip()]
                Utils.log_global(f"✅ Mensajes TXT cargados: {len(self.mensajes)} (Encoding: {encoding_used})")
            else:
                data = json.loads(raw_content)
                if isinstance(data, list):
                    self.mensajes = data
                    Utils.log_global(f"✅ Mensajes JSON (Lista) cargados: {len(self.mensajes)} (Encoding: {encoding_used})")
                elif isinstance(data, dict):
                    self.mensajes = data.get("mensajes", [])
                    Utils.log_global(f"✅ Mensajes JSON (Dict) cargados: {len(self.mensajes)} (Encoding: {encoding_used})")
                else:
                    Utils.log_global(f"⚠ Formato JSON desconocido en {self.path}")

        except Exception as e:
            Utils.log_global(f"⚠ Error parseando JSON/TXT: {e}")
            self.mensajes = []
    
    def to_binary(self, text):
        return ' '.join(format(ord(x), '08b') for x in text)

    def get_content(self, link):
        base = random.choice(self.mensajes) if self.mensajes else TEXTO_FALLBACK
        if CONVERTIR_BINARIO:
            base = self.to_binary(base)
        return f"{base}\n\n{link}"

# ==========================
# GESTOR DE COOKIES
# ==========================
def check_cookies_exist(user):
    c_path = os.path.join(FINISHED_DIR, f"cookies_{user}.json")
    try:
        if os.path.exists(c_path):
            with open(c_path, 'r') as f:
                data = json.load(f)
                if isinstance(data, dict) and len(data) > 0:
                    return True
    except:
        pass
    return False

def _build_export_driver(user, instancia_id_fake=1):
    profile = os.path.join(PROFILE_BASE, f'{user}_chrome')
    opts = Options()
    opts.add_argument(f"--user-data-dir={profile}")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    if HEADLESS: opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--hide-crash-restore-bubble")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    screen_w, screen_h = Utils.detect_display_size()
    width = max(600, screen_w // 2)
    opts.add_argument(f"--window-size={width},{screen_h}")

    browser_binary = Utils.detect_browser_path()
    if browser_binary and os.path.exists(browser_binary):
        opts.binary_location = browser_binary

    srv = Service(CHROME_DRIVER) if os.path.exists(CHROME_DRIVER) else Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=srv, options=opts)

async def export_cookies_task(user, instancia_id):
    c_path = os.path.join(FINISHED_DIR, f"cookies_{user}.json")
    Utils.log(user, f"Iniciando exportación de cookies...", instancia_id)
    driver = None
    try:
        driver = _build_export_driver(user, instancia_id)
        driver.get("https://x.com")
        await asyncio.sleep(SLEEP_COOKIES)
        cookies = driver.get_cookies()
        if cookies:
            with open(c_path, "w") as f:
                json.dump({c['name']: c['value'] for c in cookies}, f)
            Utils.log(user, "Cookies exportadas exitosamente.", instancia_id)
        else:
            Utils.log(user, "ADVERTENCIA: No se detectaron cookies (¿login pendiente?).", instancia_id)
    except Exception as e:
        Utils.log(user, f"Error exportando cookies: {e}", instancia_id)
    finally:
        if driver:
            try: driver.quit()
            except: pass

# ==========================
# BOT PRINCIPAL
# ==========================
class Bot:
    _startup_log_lock = threading.Lock()
    _startup_info_printed = False

    def __init__(self, instancia_id, users_assigned, all_users_ref, tabs_por_instancia):
        self.instancia_id = instancia_id
        self.users = users_assigned 
        if not self.users:
            raise ValueError(f"Instancia {instancia_id} sin usuarios asignados")
        self.all_users = all_users_ref 
        self.selenium_idx = 0
        self.citation_user = self.users[self.selenium_idx]
        self.tabs_por_instancia = max(1, int(tabs_por_instancia))
        self.tab_idx = -1
        self.driver = None
        self.start_t = time.time()
        self.inst_dir = os.path.join(FINISHED_DIR, f"instancia_hibrida_{self.instancia_id}")
        for d in [FINISHED_DIR, PROFILE_BASE, self.inst_dir]:
            os.makedirs(d, exist_ok=True)
        self.f_cola = GLOBAL_QUEUE_FILE
        self.f_vistos = GLOBAL_USED_FILE
        self.f_meta = GLOBAL_QUEUE_META_FILE
        self.f_msgs = resolve_messages_file()
        self.f_start = os.path.join(self.inst_dir, "last_start.json")
        self.msg_bag = MessageBag(self.f_msgs)
        self.target_smart = None
        self.target_status_id = None
        self.normalized_target_link = None
        self.search_targets = []
        self.assigned_targets = None 
        self.search_targets_normalized = False
        self.is_link_mode = False
        self.last_fetch_ts = 0.0
        self._reply_send_click_count = 0
        self._cpu_throttle_applied = False
        self._bes_cpu_limiter = None
        self._print_json_paths()
        self._load_state()
        self._analyze_target_global()

    def _log(self, user, msg):
        Utils.log(user, msg, self.instancia_id)

    def _inst_user_log(self, msg):
        self._log(self._get_current_selenium_user(), msg)

    def _print_json_paths(self):
        with Bot._startup_log_lock:
            if Bot._startup_info_printed: return
            Bot._startup_info_printed = True
        Utils.log_global(f"Todos los Users (Twikit): {self.all_users}")
        Utils.log_global(f"User Citado Local (Selenium): {self.citation_user}")

    def _restart_script(self, reason):
        self._inst_user_log(f"Reiniciando script: {reason}")
        self._save_state()
        try:
            if self.driver: self.driver.quit()
        except: pass
        self.driver = None
        self.kill_chrome()
        os.execv(sys.executable, [sys.executable] + sys.argv)

    def _driver_closed(self):
        if not self.driver: return False
        try:
            if not self.driver.window_handles: return True
            _ = self.driver.current_url
            return False
        except: return True

    def _load_state(self):
        try:
            with open(self.f_start, "r") as f:
                data = json.load(f)
                self.selenium_idx = data.get("selenium_idx", 0) % len(self.users)
                self.last_fetch_ts = float(data.get("last_fetch_ts", 0.0) or 0.0)
        except:
            self.selenium_idx = 0
            self.last_fetch_ts = 0.0
        self.citation_user = self.users[self.selenium_idx]

    def _save_state(self):
        data = {
            "selenium_idx": self.selenium_idx,
            "last_fetch_ts": self.last_fetch_ts,
        }
        with open(self.f_start, "w") as f: json.dump(data, f)

    def _analyze_target_global(self):
        t = TARGET.strip()
        parsed_targets = [u.strip().replace("@", "") for u in SEARCH_TARGETS if u and u.strip()]
        if "/status/" in t:
            self.is_link_mode = True
            self.target_smart = t
            self._inst_user_log(f"Modo LINK detectado: {self.target_smart}")
        elif parsed_targets:
            self.is_link_mode = False
            self.search_targets = parsed_targets
            self._inst_user_log(f"Modo BUSQUEDA MULTIPLE: {', '.join(self.search_targets)}")
        else:
            self.is_link_mode = False
            clean = t.replace("https://x.com/", "").replace("/", "").replace("@", "")
            self.target_smart = clean
            self._inst_user_log(f"Modo USUARIO detectado: @{clean}")

    def kill_chrome(self):
        self._inst_user_log("Limpiando procesos del navegador...")
        kill_chrome_processes(self._log, self._get_current_selenium_user())

    def _compute_window_rect(self):
        screen_w, screen_h = Utils.detect_display_size()
        
        if NUM_INSTANCIAS == 1:
            if IS_WINDOWS:
                width = max(600, screen_w // 2)
                return 0, 0, width, screen_h, screen_w, screen_h
            else:
                return 0, 0, screen_w, screen_h, screen_w, screen_h

        width = max(600, screen_w // max(1, NUM_INSTANCIAS))
        height = max(500, screen_h)
        x = max(0, (self.instancia_id - 1) * width)
        y = 0
        if self.instancia_id == NUM_INSTANCIAS:
            width = max(600, screen_w - x)
        return x, y, width, height, screen_w, screen_h

    def _apply_window_placement(self):
        if not self.driver: return
        try:
            x, y, width, height, sw, sh = self._compute_window_rect()
            self.driver.set_window_rect(x=x, y=y, width=width, height=height)
            self._inst_user_log(f"Ventana acomodada auto: x={x} y={y} w={width} h={height} (pantalla={sw}x{sh})")
        except Exception as e:
            self._inst_user_log(f"No se pudo acomodar ventana: {e}")

    def _build_driver(self, user):
        profile = os.path.join(PROFILE_BASE, f'{user}_chrome')
        self._log(user, f"Lanzando perfil: {user}")
        opts = Options()
        opts.add_argument(f"--user-data-dir={profile}")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument("--force-dark-mode")
        opts.add_argument("--enable-features=WebUIDarkMode")
        opts.add_argument("--hide-crash-restore-bubble")
        opts.add_argument("--disable-session-crashed-bubble")
        if HEADLESS: opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("prefs", {
            "profile.default_content_setting_values.notifications": 2,
            "profile.exit_type": "Normal",
            "profile.exited_cleanly": True,
        })
        try:
            x, y, width, height, _, _ = self._compute_window_rect()
            opts.add_argument(f"--window-size={width},{height}")
            opts.add_argument(f"--window-position={x},{y}")
        except: pass

        browser_binary = Utils.detect_browser_path()
        if browser_binary and os.path.exists(browser_binary):
            opts.binary_location = browser_binary

        srv = Service(CHROME_DRIVER) if os.path.exists(CHROME_DRIVER) else Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=srv, options=opts)

    def start_driver(self, user):
        self.driver = self._build_driver(user)
        self._apply_window_placement()
        self._prepare_tabs()
        self._apply_initial_cpu_throttle_if_needed(user)
        return self.driver

    def _normalized_cpu_throttle_mode(self):
        mode = (CPU_THROTTLE_MODE or "").strip().upper()
        if mode in {"MODO1", "1", "REPLY", "REPLY_FIRST_SEND"}:
            return "MODO1"
        if mode in {"MODO2", "2", "GENERAL", "GLOBAL"}:
            return "MODO2"
        return "MODO3"

    def _cpu_throttle_rate(self):
        try:
            return max(1, int(CPU_THROTTLE_RATE))
        except Exception:
            return 1

    def _set_cpu_throttle(self, user, rate, reason=""):
        if not self.driver:
            return False
        target = max(1, min(100, int(rate)))
        msg_reason = f" | motivo={reason}" if reason else ""
        # 1) Intento principal tipo BES (suspend/resume por proceso, afecta subprocesos de Chrome).
        if psutil:
            try:
                pid = None
                try:
                    pid = self.driver.service.process.pid if self.driver.service and self.driver.service.process else None
                except Exception:
                    pid = None
                if pid:
                    if self._bes_cpu_limiter:
                        self._bes_cpu_limiter.stop()
                    self._bes_cpu_limiter = BesCpuLimiter(pid, limit_percent=target, cycle_ms=100)
                    self._bes_cpu_limiter.start()
                    self._cpu_throttle_applied = True
                    self._log(user, f"CPU limiter BES aplicado: max~{target}%{msg_reason}")
                    return True
            except Exception as e:
                self._log(user, f"Fallo CPU limiter BES (max~{target}%): {e}")

        # 2) Fallback CDP (menos estricto en subprocesos).
        try:
            cdp_rate = max(1, int(100 / max(1, target)))
            self.driver.execute_cdp_cmd("Emulation.setCPUThrottlingRate", {"rate": cdp_rate})
            self._cpu_throttle_applied = cdp_rate > 1
            self._log(user, f"CPU throttle CDP fallback aplicado: rate={cdp_rate} (~{target}%){msg_reason}")
            return True
        except Exception as e:
            self._log(user, f"No se pudo aplicar CPU throttle (target={target}%): {e}")
            return False

    def _apply_initial_cpu_throttle_if_needed(self, user):
        mode = self._normalized_cpu_throttle_mode()
        if mode != "MODO2":
            return
        self._set_cpu_throttle(user, self._cpu_throttle_rate(), "modo2-general")

    def _maybe_apply_reply_send_throttle(self, user, is_reply_mode):
        mode = self._normalized_cpu_throttle_mode()
        if mode == "MODO3":
            return
        if mode == "MODO2":
            if not self._cpu_throttle_applied:
                self._set_cpu_throttle(user, self._cpu_throttle_rate(), "modo2-general")
            return
        if mode == "MODO1" and is_reply_mode and self._reply_send_click_count == 0:
            if self._set_cpu_throttle(user, self._cpu_throttle_rate(), "modo1-primer-click-reply"):
                self._reply_send_click_count += 1

    def _prepare_tabs(self):
        if not self.driver: return
        handles = self.driver.window_handles
        while len(handles) < self.tabs_por_instancia:
            self.driver.switch_to.new_window('tab')
            self.driver.get(URL_INICIAL)
            time.sleep(SLEEP_CARGA)
            handles = self.driver.window_handles

    def _switch_to_next_tab(self):
        if not self.driver: return
        handles = self.driver.window_handles
        if not handles: return
        self.tab_idx = (self.tab_idx + 1) % len(handles)
        self.driver.switch_to.window(handles[self.tab_idx])

    def _ensure_driver_ready(self, user):
        if self.driver and self._driver_closed():
            self._restart_script("Chrome cerrado/crasheado al preparar driver")
        if not self.driver:
            self.start_driver(user)
            try:
                self.driver.get(URL_INICIAL)
                self._dismiss_restore_prompt_if_present(user)
                self._log(user, f"Driver listo en {URL_INICIAL}, esperando {SLEEP_CARGA}s...")
                time.sleep(SLEEP_CARGA)
            except Exception as e:
                self._log(user, f"No se pudo preparar URL inicial: {e}")
        try:
            smart_utils.perform_smart_close(self.driver)
        except: pass

    def _dismiss_restore_prompt_if_present(self, user):
        if not self.driver: return
        try:
            body_text = (self.driver.find_element(By.TAG_NAME, "body").text or "").lower()
            if "restore" not in body_text and "restaur" not in body_text: return
            selectors = ["button[aria-label*='Close']", "button[aria-label*='Cerrar']", "button[jsname]"]
            clicked = False
            for sel in selectors:
                try:
                    buttons = self.driver.find_elements(By.CSS_SELECTOR, sel)
                    for btn in buttons:
                        txt = (btn.text or "").strip().lower()
                        aria = (btn.get_attribute("aria-label") or "").strip().lower()
                        if any(k in txt or k in aria for k in ["close", "cerr", "x", "dismiss", "restore"]):
                            btn.click()
                            clicked = True
                            break
                    if clicked: break
                except: continue
            if clicked:
                self._log(user, "Se cerró aviso de restore page.")
                time.sleep(0.8)
        except: return

    def shutdown_driver(self):
        if self._bes_cpu_limiter:
            try:
                self._bes_cpu_limiter.stop()
            except: pass
            self._bes_cpu_limiter = None
        if not self.driver: return
        try:
            self._inst_user_log("Cierre limpio de instancia...")
            smart_utils.perform_smart_close(self.driver)
        except: pass
        try:
            self.driver.quit()
        except: pass
        self.driver = None

    def _get_current_selenium_user(self):
        return self.citation_user

    def _rotate_selenium_user(self):
        if self.users:
            self.selenium_idx = (self.selenium_idx + 1) % len(self.users)
            self.citation_user = self.users[self.selenium_idx]

    def _load_queue_and_seen(self):
        with _LINK_STORE_LOCK:
            cola = Utils.load_json(self.f_cola)
            vistos = set(Utils.load_json(self.f_vistos))
            meta = {}
            try:
                meta = {} if not os.path.exists(self.f_meta) else (json.load(open(self.f_meta, "r", encoding="utf-8")) or {})
            except:
                meta = {}
        return cola, vistos, meta

    def _save_queue_and_seen(self, cola, vistos, meta=None):
        with _LINK_STORE_LOCK:
            Utils.save_json(self.f_cola, cola)
            Utils.save_json(self.f_vistos, list(vistos))
            if meta is not None:
                try:
                    Utils.save_json(self.f_meta, meta)
                except Exception as e:
                    self._log(self._get_current_selenium_user(), f"ERROR guardando meta file: {e}")

    def _pop_link(self):
        with _LINK_STORE_LOCK:
            cola = Utils.load_json(self.f_cola)
            if not cola: return None
            link = cola.pop(0)
            Utils.save_json(self.f_cola, cola)
        return link

    def _extract_status_id(self, url):
        if not url: return None
        m = re.search(r"/status/(\d+)", url)
        return m.group(1) if m else None

    def _normalize_link_with_replies(self, link, selenium_user):
        if self.normalized_target_link and self.target_status_id:
            return self.normalized_target_link
        try:
            self._ensure_driver_ready(selenium_user)
            self.driver.get(link)
            WebDriverWait(self.driver, 12).until(lambda d: "/status/" in d.current_url)
            loaded_url = self.driver.current_url.split("?")[0].rstrip("/")
            status_id = self._extract_status_id(loaded_url)
            if not status_id: raise ValueError(f"No se pudo obtener status_id de: {loaded_url}")
            normalized = re.sub(r"/with_replies$", "", loaded_url)
            normalized = f"{normalized}/with_replies"
            self.normalized_target_link = normalized
            self.target_status_id = status_id
            self._log(selenium_user, f"FETCH | Link normalizado: {normalized}")
            return normalized
        except Exception as e:
            self._log(selenium_user, f"No se pudo normalizar link con Selenium: {e}")
            return None

    def _build_excluded_users(self):
        return {u.lower() for u in self.all_users + EXCLUDE_USERS if u}

    def _normalize_search_targets_with_selenium(self, selenium_user):
        if self.search_targets_normalized or self.is_link_mode: return
        raw_targets = [(raw_target or "").strip() for raw_target in self.search_targets if (raw_target or "").strip()]
        
        id_targets_count = sum(1 for t in raw_targets if re.match(r"^https?://x\.com/i/user/\d+/?$", t, re.IGNORECASE))
        if id_targets_count == 0:
            self.search_targets_normalized = True
            return

        normalized_targets = []
        for raw_target in raw_targets:
            target = raw_target
            if not re.match(r"^https?://x\.com/i/user/\d+/?$", target, re.IGNORECASE):
                normalized_targets.append(target)
                continue
            try:
                self._ensure_driver_ready(selenium_user)
                self.driver.get(target)
                WebDriverWait(self.driver, 12).until(lambda d: re.search(r"https?://x\.com/[^/]+/?", d.current_url) and "/i/user/" not in d.current_url)
                loaded_url = self.driver.current_url.split("?")[0].rstrip("/")
                user_match = re.search(r"x\.com/([^/]+)$", loaded_url, re.IGNORECASE)
                if user_match:
                    profile_with_replies = f"{loaded_url}/with_replies"
                    normalized_targets.append(profile_with_replies)
                    self._log(selenium_user, f"Target ID normalizado: {profile_with_replies}")
                else:
                    normalized_targets.append(target)
            except Exception as e:
                normalized_targets.append(target)
                self._log(selenium_user, f"Fallo al normalizar '{target}': {e}")
            finally:
                try:
                    self.driver.get(URL_INICIAL)
                    time.sleep(SLEEP_CARGA)
                except: pass
        self.search_targets = normalized_targets
        self.search_targets_normalized = True

    def _build_search_url(self, search_term, excluded_users):
        parts = [search_term]
        filter_part = (SEARCH_FILTER_QUERY or "").strip()
        if filter_part:
            parts.append(filter_part)

        if excluded_users: parts.extend([f"-{u}" for u in sorted(excluded_users)])
        query_text = " ".join([p for p in parts if p]).strip()
        
        # NOTA: src=typed_query&f=live FUERZA EL MODO LIVE (LATEST)
        return f"https://x.com/search?q={quote(query_text)}&src=typed_query&f=live", query_text

    def _extract_tweet_metadata(self, t, current_target_term):
        text = (getattr(t, "text", "") or "").strip()
        screen_name = (getattr(t, "user", None) and getattr(t.user, "screen_name", "") or "").strip().lower()
        link = f"https://x.com/{getattr(t.user, 'screen_name')}/status/{getattr(t, 'id')}"
        is_reply = bool(getattr(t, "in_reply_to_status_id", None) or getattr(t, "in_reply_to_user_id", None))
        is_quote = bool(getattr(t, "is_quote_status", False) or getattr(t, "is_quote", False))
        in_reply_to_screen_name = (
            getattr(t, "in_reply_to_screen_name", None)
            or getattr(t, "in_reply_to_username", None)
            or ""
        ).strip().lower().replace("@", "")

        mentions = []
        try:
            ent = getattr(t, "entities", None)
            if ent and isinstance(ent, dict):
                ums = ent.get("user_mentions") or ent.get("mentions") or []
                for m in ums:
                    name = (m.get("screen_name") or m.get("username") or "").lower()
                    if name and name not in mentions:
                        mentions.append(name)
        except:
            pass
        try:
            found = re.findall(r"@([A-Za-z0-9_]{1,30})", text)
            for f in found:
                f = f.lower()
                if f not in mentions:
                    mentions.append(f)
        except:
            pass

        exact_text_found = False
        try:
            for m in (self.msg_bag.mensajes or []):
                if m and m.strip() and m.strip() in text:
                    exact_text_found = True
                    break
        except:
            exact_text_found = False

        target_term = (current_target_term or "").strip().lower().replace("@", "")
        target_terms = []
        for piece in re.split(r"(?i)\s+or\s+", target_term):
            term = (piece or "").strip().replace("@", "")
            if term and term not in target_terms:
                target_terms.append(term)
        if not target_terms and target_term:
            target_terms = [target_term]

        matched_terms_link = [
            term for term in target_terms
            if re.search(rf"x\.com/{re.escape(term)}/status/", link, re.IGNORECASE)
        ]
        matched_terms_mention = [term for term in target_terms if term in mentions]
        matched_terms_exact_text = [
            term for term in target_terms
            if re.search(rf"(?<![A-Za-z0-9_]){re.escape(term)}(?![A-Za-z0-9_])", text, re.IGNORECASE)
        ]
        direct_reply_to_target = bool(in_reply_to_screen_name and in_reply_to_screen_name in target_terms)

        link_contains_target = bool(matched_terms_link)
        mentioned_target = bool(matched_terms_mention)
        exact_target_text_found = bool(matched_terms_exact_text)

        # "Ajeno" => solo mención sin respuesta directa ni quote.
        # Para guardar exigimos señales fuertes por target: autor target,
        # respuesta directa al target, quote/cita, texto exacto del target,
        # o coincidencia exacta de mensajes configurados.
        should_enqueue = bool(
            link_contains_target
            or direct_reply_to_target
            or is_quote
            or exact_target_text_found
            or exact_text_found
        )

        return {
            "text": text,
            "screen_name": screen_name,
            "link": link,
            "is_reply": bool(is_reply),
            "is_quote": bool(is_quote),
            "mentions": mentions,
            "exact_text_found": bool(exact_text_found),
            "target_term": target_term,
            "target_terms": target_terms,
            "in_reply_to_screen_name": in_reply_to_screen_name,
            "direct_reply_to_target": direct_reply_to_target,
            "mentioned_target": mentioned_target,
            "link_contains_target": link_contains_target,
            "exact_target_text_found": exact_target_text_found,
            "matched_terms_link": matched_terms_link,
            "matched_terms_mention": matched_terms_mention,
            "matched_terms_exact_text": matched_terms_exact_text,
            "should_enqueue": should_enqueue,
        }

    def _log_extracted_tweet(self, twikit_user, meta_entry):
        base_msg = f"🔹 EXTRAIDO: {meta_entry['link']}"
        if PRINT_TEXT_WITH_LINK:
            preview = (meta_entry.get("text_preview") or "")[:100]
            base_msg += f" | TEXTO: {preview}..."
        self._log(twikit_user, base_msg)
        self._log(twikit_user, f"      -> created_at={meta_entry['tweet_created_at']} saved_at={meta_entry['saved_at']}")
        self._log(twikit_user, f"      -> target={meta_entry['target_term']} terms={meta_entry.get('target_terms', [])} enqueue={meta_entry['should_enqueue']} | link_has_target={meta_entry['link_contains_target']} mention_target={meta_entry['mentioned_target']} exact_target={meta_entry['exact_target_text_found']} direct_reply_target={meta_entry.get('direct_reply_to_target', False)} | reply={meta_entry['is_reply']} quote={meta_entry['is_quote']} mentions={meta_entry['mentions']} reply_to={meta_entry.get('in_reply_to_screen_name', '')} exact_text_found={meta_entry['exact_text_found']}")

    def _extract_query_term_from_target(self, raw_target):
        target = (raw_target or "").strip()
        if not target: return ""
        profile_match = re.match(r"^https?://x\.com/([^/?#]+)(?:/with_replies)?/?$", target, re.IGNORECASE)
        if profile_match: return profile_match.group(1)
        search_match = re.match(r"^https?://x\.com/search\?q=([^&]+)", target, re.IGNORECASE)
        if search_match: return unquote(search_match.group(1)).strip()
        return target.replace("@", "").strip()

    def _extract_profile_user_from_target(self, raw_target):
        target = (raw_target or "").strip()
        if not target: return None
        profile_match = re.match(r"^https?://x\.com/([^/?#]+)(?:/with_replies)?/?$", target, re.IGNORECASE)
        if not profile_match: return None
        user = (profile_match.group(1) or "").strip()
        if not user or user.lower() in {"search", "home", "notifications", "explore", "i"}: return None
        return user

    def _wait_clickable_with_retries(self, user, selector, label, retries=3, timeout=3):
        for intent_idx in range(1, retries + 1):
            if self._driver_closed():
                return None
            try:
                wait = WebDriverWait(self.driver, timeout, poll_frequency=0.1)
                elem = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
                return elem
            except Exception as e:
                self._log(user, f"{label} no disponible (espera {intent_idx}/{retries}): {e}")
                if intent_idx < retries:
                    time.sleep(SLEEP_BASE)
        return None

    async def _attempt_send_with_retry(self, user, is_reply_mode=False):
        selector_reply = "[data-testid='tweetButton']"
        for attempt in range(1, REINTENTOS_ENVIO + 1):
            if self._driver_closed():
                return False
            if not smart_utils.is_composer_active(self.driver):
                self._log(user, f"Envío confirmado (composer cerrado) antes del intento {attempt}.")
                return True
            try:
                reply = self._wait_clickable_with_retries(
                    user,
                    selector_reply,
                    "Botón Reply/Tweet",
                    retries=3,
                    timeout=3,
                )
                if not reply:
                    self._log(user, f"No se encontró botón Reply/Tweet en intento {attempt}/{REINTENTOS_ENVIO}.")
                else:
                    self._maybe_apply_reply_send_throttle(user, is_reply_mode=is_reply_mode)
                    self._log(user, f"Haciendo click en Reply (Intento {attempt})...")
                    smart_utils.safe_click(self.driver, reply, f"Botón Reply intento {attempt}")

                WebDriverWait(self.driver, 3, poll_frequency=0.1).until(
                    lambda d: not smart_utils.is_composer_active(d)
                )
                self._log(user, f"Envío confirmado en intento {attempt}/{REINTENTOS_ENVIO}.")
                return True
            except Exception as e:
                self._log(user, f"Fallo intento {attempt}: {e}")
            await asyncio.sleep(SLEEP_BASE)
        return False

    def _intent_composer_still_open(self):
        """Detección robusta del composer en la pestaña intent/reply."""
        try:
            current_url = (self.driver.current_url or "").lower()
        except Exception:
            current_url = ""

        in_intent = "intent/tweet" in current_url
        in_compose = "compose/post" in current_url or "compose/tweet" in current_url

        if in_intent or in_compose:
            return True

        composer_selectors = [
            "[data-testid='tweetTextarea_0']",
            "div[role='textbox'][data-testid='tweetTextarea_0']",
            "div[role='dialog'] [data-testid='tweetTextarea_0']",
            "div[aria-label='Post text']",
            "div[aria-label='Texto de la publicación']",
        ]

        for selector in composer_selectors:
            try:
                elems = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if any(elem.is_displayed() for elem in elems):
                    return True
            except WebDriverException:
                continue
            except Exception:
                continue

        fallback_active = smart_utils.is_composer_active(self.driver)

        # Si ya salimos de intent/compose y no hay textbox visible,
        # priorizamos el estado de URL para evitar falsos positivos.
        if not in_intent and not in_compose:
            return False
        return fallback_active

    def _intent_is_home_ready(self):
        """Devuelve True cuando la tab de intent/reply ya regresó a Home."""
        try:
            current_url = (self.driver.current_url or "").lower()
        except Exception:
            return False

        home_markers = [
            "x.com/home",
            "twitter.com/home",
            "x.com/i/",
            "twitter.com/i/",
        ]
        return any(marker in current_url for marker in home_markers)

    def _has_send_success_toast(self):
        """Detecta toast de éxito al enviar en ES/EN y variantes."""
        success_patterns = [
            "your post was sent",
            "your reply was sent",
            "post sent",
            "post enviado",
            "se envió",
            "se envio",
            "respuesta enviada",
        ]

        selectors = [
            "div[data-testid='toast']",
            "div[role='status']",
            "div[aria-live='polite']",
            "div[aria-live='assertive']",
        ]

        try:
            candidates = []
            for selector in selectors:
                candidates.extend(self.driver.find_elements(By.CSS_SELECTOR, selector))

            for elem in candidates:
                try:
                    if not elem.is_displayed():
                        continue
                    text = (elem.text or "").strip().lower()
                    if text and any(pattern in text for pattern in success_patterns):
                        return True
                except WebDriverException:
                    continue
                except Exception:
                    continue
        except Exception:
            return False

        return False

    async def _wait_intent_send_confirmation(self, user, timeout=8):
        """Confirma envío en intent/reply esperando toast + cierre real del composer."""
        end_t = time.time() + timeout
        toast_seen = False
        last_state_print = 0.0

        while time.time() < end_t:
            if self._driver_closed():
                return False

            now = time.time()

            if self._has_send_success_toast():
                toast_seen = True

            composer_open = self._intent_composer_still_open()
            is_home_ready = self._intent_is_home_ready()
            if toast_seen and (not composer_open or is_home_ready):
                self._log(
                    user,
                    f"Reply OK: toast detectado y tab lista (home={is_home_ready}, composer_activo={composer_open}).",
                )
                return True

            if now - last_state_print >= 1.0:
                try:
                    curr_url = self.driver.current_url
                except Exception:
                    curr_url = "(sin URL)"
                remaining = max(0.0, end_t - now)
                self._log(
                    user,
                    f"Confirmando envío reply... toast={toast_seen} home={is_home_ready} composer_activo={composer_open} restante={remaining:.1f}s url={curr_url}",
                )
                last_state_print = now

            await asyncio.sleep(0.25)

        if toast_seen:
            self._log(user, "Toast detectado, pero composer/URL de intent sigue activo.")
        else:
            self._log(user, "No se detectó toast de envío dentro del tiempo esperado.")
        return False

    def _tweet_created_ts(self, t):
        # Intentar extraer timestamp de varios atributos comunes.
        # Devuelve float(timestamp) en segundos UTC.
        cand = None
        for attr in ("created_at", "created_at_iso", "created", "created_at_datetime", "timestamp"):
            cand = getattr(t, attr, None)
            if cand:
                break
        if cand:
            try:
                if isinstance(cand, (int, float)):
                    return float(cand)
                if isinstance(cand, str):
                    # ISO formato?
                    try:
                        return datetime.fromisoformat(cand.replace("Z", "+00:00")).timestamp()
                    except Exception:
                        pass
                    # formato estilo "Mon Feb 12 15:04:05 +0000 2024" (Twitter legacy)
                    try:
                        return time.mktime(time.strptime(cand, "%a %b %d %H:%M:%S %z %Y"))
                    except Exception:
                        pass
                # si es datetime
                if hasattr(cand, "timestamp"):
                    return float(cand.timestamp())
            except Exception:
                pass
        # Fallback: si twikit expone t.created_at as objeto .datetime:
        try:
            v = getattr(t, "created_at_datetime", None)
            if v and hasattr(v, "timestamp"):
                return float(v.timestamp())
        except: pass
        # Last resort:
        return time.time()

    def _format_iso(self, ts):
        try:
            return datetime.fromtimestamp(float(ts), timezone.utc).isoformat().replace("+00:00", "Z")
        except:
            return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def _parse_iso_to_ts(self, value):
        if value is None:
            return None
        try:
            if isinstance(value, (int, float)):
                return float(value)
            if isinstance(value, str):
                return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except Exception:
            return None
        return None

    def _is_tweet_fresh(self, tweet_created_ts, max_age_hours=MAX_TWEET_AGE_HOURS):
        try:
            age_seconds = time.time() - float(tweet_created_ts)
        except Exception:
            return False
        return age_seconds <= float(max_age_hours) * 3600.0

    def _purge_stale_queue_entries(self, cola, meta):
        if not cola:
            return cola, meta, 0
        fresh_cola = []
        removed = 0
        for link in cola:
            entry = meta.get(link, {}) if isinstance(meta, dict) else {}
            created_ts = self._parse_iso_to_ts(entry.get("tweet_created_at"))
            if created_ts is not None and not self._is_tweet_fresh(created_ts):
                removed += 1
                if isinstance(meta, dict):
                    meta.pop(link, None)
                continue
            fresh_cola.append(link)
        return fresh_cola, meta, removed


    async def fetch(self, global_cycle_idx):
        twikit_user = self.all_users[global_cycle_idx % len(self.all_users)]
        selenium_user = self._get_current_selenium_user() 

        c_path = os.path.join(FINISHED_DIR, f"cookies_{twikit_user}.json")
        if not check_cookies_exist(twikit_user):
            self._log(twikit_user, "ADVERTENCIA: Cookies no encontradas en fetch. Saltando.")
            return

        client = Client('en-US')
        try:
            with open(c_path, "r") as f: client.set_cookies(json.load(f))
            excluded_users = self._build_excluded_users()
            excl = " ".join([f"-{u}" for u in sorted(excluded_users)])
            cola, vistos, meta = self._load_queue_and_seen()
            cola, meta, stale_removed = self._purge_stale_queue_entries(cola, meta)
            if stale_removed:
                self._log(twikit_user, f"🧹 Purga de cola por antigüedad>{MAX_TWEET_AGE_HOURS}h: -{stale_removed}")
            cola_before = len(cola)
            
            # --- LIMITE DE EXTRACCION (EN LOGICA) ---
            count_extracted = 0
            limit_reached = False
            
            # Filtro único configurable (ej: "lang:es", "-filter:replies")
            filter_part = (SEARCH_FILTER_QUERY or "").strip()

            if self.is_link_mode:
                normalized_link = self._normalize_link_with_replies(self.target_smart or TARGET, selenium_user)
                target_term = self._extract_query_term_from_target(self.target_smart or TARGET)
                if normalized_link and self.target_status_id: 
                    # Usa el filtro replies si esta activo
                    query = f"conversation_id:{self.target_status_id} {filter_part} filter:live {excl}".strip()
                    self._log(twikit_user, f"FETCH GLOBAL | Query: '{query}' | Url: {normalized_link}")
                    
                    # product='Latest' FUERZA MODO LIVE
                    raw_tweets = await client.search_tweet(query, product='Latest')
                    # Asegurarnos de ordenar por fecha - recolectar y ordenar
                    tweets_with_ts = []
                    for t in raw_tweets:
                        ts = self._tweet_created_ts(t)
                        tweets_with_ts.append((ts, t))
                    tweets_with_ts.sort(key=lambda x: x[0], reverse=True)
                    selected = tweets_with_ts[:MAX_TWEETS_FETCH]

                    for ts, t in selected:
                        if count_extracted >= MAX_TWEETS_FETCH:
                            limit_reached = True
                            break
                        tweet_info = self._extract_tweet_metadata(t, target_term)
                        if tweet_info["screen_name"] in excluded_users:
                            continue
                        link = tweet_info["link"]

                        if link not in vistos and link not in cola:
                            if not tweet_info["should_enqueue"]:
                                if PRINT_DETAILED_LOGS:
                                    self._log(twikit_user, f"⏭ DESCARTADO (sin señales): {link} | target={tweet_info['target_term']}")
                                continue

                            saved_at = time.time()
                            tweet_created_ts = float(ts or self._tweet_created_ts(t))
                            if not self._is_tweet_fresh(tweet_created_ts):
                                if PRINT_DETAILED_LOGS:
                                    self._log(twikit_user, f"⏭ DESCARTADO (antigüedad>{MAX_TWEET_AGE_HOURS}h): {link}")
                                continue
                            meta_entry = {
                                "link": link,
                                "saved_at": self._format_iso(saved_at),
                                "tweet_created_at": self._format_iso(tweet_created_ts),
                                "is_reply": tweet_info["is_reply"],
                                "is_quote": tweet_info["is_quote"],
                                "mentions": tweet_info["mentions"],
                                "exact_text_found": tweet_info["exact_text_found"],
                                "target_term": tweet_info["target_term"],
                                "target_terms": tweet_info.get("target_terms", []),
                                "in_reply_to_screen_name": tweet_info.get("in_reply_to_screen_name", ""),
                                "direct_reply_to_target": tweet_info.get("direct_reply_to_target", False),
                                "mentioned_target": tweet_info["mentioned_target"],
                                "link_contains_target": tweet_info["link_contains_target"],
                                "exact_target_text_found": tweet_info["exact_target_text_found"],
                                "matched_terms_link": tweet_info.get("matched_terms_link", []),
                                "matched_terms_mention": tweet_info.get("matched_terms_mention", []),
                                "matched_terms_exact_text": tweet_info.get("matched_terms_exact_text", []),
                                "should_enqueue": tweet_info["should_enqueue"],
                                "text_preview": tweet_info["text"],
                                "extracted_by_user": twikit_user,
                            }

                            if PRINT_DETAILED_LOGS:
                                self._log_extracted_tweet(twikit_user, meta_entry)

                            cola.append(link); vistos.add(link)
                            meta[link] = meta_entry
                            count_extracted += 1
            else:
                self._normalize_search_targets_with_selenium(selenium_user)
                if self.search_targets:
                    target_idx = global_cycle_idx % len(self.search_targets)
                    target = self.search_targets[target_idx]
                    profile_user = self._extract_profile_user_from_target(target)
                    
                    if profile_user:
                        target_term = profile_user
                        # Forzamos "from:profile_user" cuando target sea perfil
                        final_query = f"from:{profile_user} {filter_part} {excl}".strip()
                        self._log(twikit_user, f"FETCH GLOBAL | User {twikit_user} -> Target {profile_user} ({target_idx+1}/{len(self.search_targets)})")
                        self._log(twikit_user, f"🔎 QUERY TWIKIT: {final_query}")
                    else:
                        query_term = self._extract_query_term_from_target(target)
                        target_term = query_term
                        # Build query and display URL for debugging (uses _build_search_url)
                        debug_url, final_query = self._build_search_url(query_term, excluded_users)
                        
                        self._log(twikit_user, f"FETCH GLOBAL | User {twikit_user} -> Busqueda '{query_term}' ({target_idx+1}/{len(self.search_targets)})")
                        self._log(twikit_user, f"🔗 SEARCH URL (LIVE): {debug_url}")
                        self._log(twikit_user, f"🔎 QUERY TWIKIT: {final_query}")
                    
                    # product='Latest' FUERZA MODO LIVE, EVITA TOP
                    raw_tweets = await client.search_tweet(final_query, product='Latest')
                    tweets_with_ts = []
                    for t in raw_tweets:
                        ts = self._tweet_created_ts(t)
                        tweets_with_ts.append((ts, t))
                    tweets_with_ts.sort(key=lambda x: x[0], reverse=True)
                    selected = tweets_with_ts[:MAX_TWEETS_FETCH]

                    for ts, t in selected:
                        if count_extracted >= MAX_TWEETS_FETCH:
                            limit_reached = True
                            break
                        tweet_info = self._extract_tweet_metadata(t, target_term)
                        if tweet_info["screen_name"] in excluded_users:
                            continue
                        link = tweet_info["link"]
                        if link not in vistos and link not in cola:
                            if not tweet_info["should_enqueue"]:
                                if PRINT_DETAILED_LOGS:
                                    self._log(twikit_user, f"⏭ DESCARTADO (sin señales): {link} | target={tweet_info['target_term']}")
                                continue

                            saved_at = time.time()
                            tweet_created_ts = float(ts or self._tweet_created_ts(t))
                            if not self._is_tweet_fresh(tweet_created_ts):
                                if PRINT_DETAILED_LOGS:
                                    self._log(twikit_user, f"⏭ DESCARTADO (antigüedad>{MAX_TWEET_AGE_HOURS}h): {link}")
                                continue
                            meta_entry = {
                                "link": link,
                                "saved_at": self._format_iso(saved_at),
                                "tweet_created_at": self._format_iso(tweet_created_ts),
                                "is_reply": tweet_info["is_reply"],
                                "is_quote": tweet_info["is_quote"],
                                "mentions": tweet_info["mentions"],
                                "exact_text_found": tweet_info["exact_text_found"],
                                "target_term": tweet_info["target_term"],
                                "target_terms": tweet_info.get("target_terms", []),
                                "in_reply_to_screen_name": tweet_info.get("in_reply_to_screen_name", ""),
                                "direct_reply_to_target": tweet_info.get("direct_reply_to_target", False),
                                "mentioned_target": tweet_info["mentioned_target"],
                                "link_contains_target": tweet_info["link_contains_target"],
                                "exact_target_text_found": tweet_info["exact_target_text_found"],
                                "matched_terms_link": tweet_info.get("matched_terms_link", []),
                                "matched_terms_mention": tweet_info.get("matched_terms_mention", []),
                                "matched_terms_exact_text": tweet_info.get("matched_terms_exact_text", []),
                                "should_enqueue": tweet_info["should_enqueue"],
                                "text_preview": tweet_info["text"],
                                "extracted_by_user": twikit_user,
                            }

                            if PRINT_DETAILED_LOGS:
                                self._log_extracted_tweet(twikit_user, meta_entry)

                            cola.append(link); vistos.add(link)
                            meta[link] = meta_entry
                            count_extracted += 1
                else:
                    self._log(twikit_user, "FETCH | Sin targets válidos.")

            # Guardar cola/vistos/meta
            self._save_queue_and_seen(cola, vistos, meta)
            delta = len(cola) - cola_before
            msg_limit = f" [LIMITE {MAX_TWEETS_FETCH} ALCANZADO]" if limit_reached else ""
            if delta > 0: self._log(twikit_user, f"FETCH | Nuevos links: +{delta} (cola={len(cola)}){msg_limit}")
            else: self._log(twikit_user, f"FETCH | Sin nuevos links (cola={len(cola)})")

        except Exception as e:
            self._log(twikit_user, f"ERROR | FETCH: {e}")

    async def execute(self, user, link):
        content = self.msg_bag.get_content(link)
        if self.driver and self._driver_closed():
            self._restart_script("Chrome cerrado/crasheado antes de ejecutar acciones")
        if not self.driver: self.start_driver(user)
        wait = WebDriverWait(self.driver, 8)
        
        self._log(user, "Verificando si hay composer abierto (Smart Close)...")
        is_clean = smart_utils.perform_smart_close(self.driver)
        if is_clean:
            self._log(user, "Composer limpio (o se cerró correctamente).")
        else:
            self._log(user, "No se pudo limpiar composer. Intentando recarga forzada...")
            try:
                self.driver.get(URL_INICIAL)
                time.sleep(SLEEP_CARGA)
            except: pass

        start_t = time.time()
        try:
            if MODO_ACCION == "CITA":
                tipo_cita_normalizado = (TIPO_CITA or "").strip().upper()
                if tipo_cita_normalizado == "BOTON":
                    curr_url = self.driver.current_url
                    if "compose" in curr_url or "intent" in curr_url or (URL_INICIAL not in curr_url and "home" not in curr_url):
                        self._log(user, "URL incorrecta, navegando a inicial...")
                        self.driver.get(URL_INICIAL)
                        await asyncio.sleep(SLEEP_CARGA)
                    
                    self._log(user, "Abriendo panel de escritura ('n')...")
                    self.driver.find_element(By.TAG_NAME, "body").send_keys("n")
                    box = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "[data-testid='tweetTextarea_0']")))

                    self._log(user, "Escribiendo contenido...")
                    box.click()
                    self.driver.execute_script("arguments[0].value = '';", box)
                    box.send_keys(content)
                    await asyncio.sleep(SLEEP_ESCRITURA)
                    
                    self._log(user, "Enviando tweet...")
                    if not await self._attempt_send_with_retry(user, is_reply_mode=False): return False, 0
                elif tipo_cita_normalizado in REPLY_MODE_ALIASES:
                    status_id = self._extract_status_id(link)
                    if not status_id:
                        self._log(user, f"No se pudo extraer status_id desde link: {link}")
                        return False, 0

                    base_handle = self.driver.current_window_handle
                    reply_sent = False
                    temp_handle = None
                    self._log(user, f"Modo Reply detectado ({TIPO_CITA}). Abriendo intent/reply en tab temporal...")
                    self.driver.switch_to.new_window('tab')
                    temp_handle = self.driver.current_window_handle
                    try:
                        intent_reply_url = f"https://x.com/intent/tweet?in_reply_to={status_id}&text={quote(content)}"
                        self._log(user, f"Reply intent URL: {intent_reply_url}")
                        self.driver.get(intent_reply_url)
                        await asyncio.sleep(SLEEP_CARGA)
                        await asyncio.sleep(REPLY_INTENT_INITIAL_SLEEP)

                        self._log(user, "Enviando reply (intent)...")
                        if not await self._attempt_send_with_retry(user, is_reply_mode=True):
                            if self._intent_composer_still_open():
                                self._log(user, "Composer detectado tras carga lenta. Reintentando envío una vez más...")
                                await asyncio.sleep(SLEEP_BASE)
                                if await self._attempt_send_with_retry(user, is_reply_mode=True):
                                    confirmed = await self._wait_intent_send_confirmation(user, timeout=8)
                                    reply_sent = bool(confirmed)
                                else:
                                    reply_sent = False
                            else:
                                reply_sent = False
                        else:
                            confirmed = await self._wait_intent_send_confirmation(user, timeout=8)
                            if not confirmed and self._intent_composer_still_open():
                                self._log(user, "Composer sigue activo en tab intent/reply tras envío. Reintento único con verificación de toast...")
                                await asyncio.sleep(SLEEP_BASE)
                                if await self._attempt_send_with_retry(user, is_reply_mode=True):
                                    reply_sent = await self._wait_intent_send_confirmation(user, timeout=8)
                                else:
                                    reply_sent = False
                            else:
                                reply_sent = True
                    finally:
                        try:
                            if self.driver:
                                handles = self.driver.window_handles
                                if temp_handle and temp_handle in handles:
                                    self.driver.switch_to.window(temp_handle)
                                    self.driver.close()
                                handles_after = self.driver.window_handles
                                if base_handle in handles_after:
                                    self.driver.switch_to.window(base_handle)
                                elif handles_after:
                                    self.driver.switch_to.window(handles_after[0])

                                current_after = (self.driver.current_url or "") if self.driver else ""
                                self._log(user, f"Tab temporal cerrada. URL activa actual: {current_after}")
                        except Exception as close_err:
                            self._log(user, f"No se pudo cerrar/salir de tab temporal de reply: {close_err}")

                    try:
                        active_url = (self.driver.current_url or "").lower()
                    except Exception:
                        active_url = ""
                    if "intent/tweet" in active_url:
                        self._log(user, "URL final aún contiene intent/tweet. Marcando como fallo para reintento.")
                        reply_sent = False

                    if reply_sent:
                        dur = time.time() - start_t
                        self._log(user, f"Reply finalizado correctamente en {dur:.1f}s.")
                        return True, dur

                    self._log(user, "Reply no confirmado. Activando fallback tipo BOTON en notifications con link...")
                    try:
                        curr_url = self.driver.current_url
                    except Exception:
                        curr_url = ""
                    if "compose" in curr_url or "intent" in curr_url or (URL_INICIAL not in curr_url and "home" not in curr_url):
                        self._log(user, "Fallback: URL incorrecta, navegando a notifications...")
                        self.driver.get(URL_INICIAL)
                        await asyncio.sleep(SLEEP_CARGA)

                    fallback_content = f"{content}\n\n{link}"
                    self._log(user, "Fallback: abriendo composer con tecla 'n'...")
                    self.driver.find_element(By.TAG_NAME, "body").send_keys("n")
                    box = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "[data-testid='tweetTextarea_0']")))
                    box.click()
                    self.driver.execute_script("arguments[0].value = '';", box)
                    box.send_keys(fallback_content)
                    await asyncio.sleep(SLEEP_ESCRITURA)

                    if await self._attempt_send_with_retry(user, is_reply_mode=False):
                        await asyncio.sleep(SLEEP_POST_CLICK)
                        if not smart_utils.is_composer_active(self.driver):
                            dur = time.time() - start_t
                            self._log(user, f"Fallback BOTON OK en {dur:.1f}s.")
                            return True, dur

                    self._log(user, "Fallback BOTON también falló. Se devolverá a cola para reintento.")
                    return False, 0
                else:
                    self._log(user, f"Modo '{TIPO_CITA}' no reconocido. Usando intent/tweet como fallback.")
                    self.driver.get(f"https://x.com/intent/tweet?text={quote(content)}")
                    if not await self._attempt_send_with_retry(user, is_reply_mode=False): return False, 0

            await asyncio.sleep(SLEEP_POST_CLICK)
            if not smart_utils.is_composer_active(self.driver):
                dur = time.time() - start_t
                return True, dur
            else:
                self._log(user, "El composer sigue activo tras intento. Ejecutando cierre forzoso...")
                smart_utils.perform_smart_close(self.driver)
                await asyncio.sleep(SLEEP_RECOVERY)
                return False, 0

        except Exception as e:
            self._log(user, f"Excepción en ejecución: {e}")
            return False, 0

    async def process_action_only(self):
        """Intenta procesar UN link. Si no hay, devuelve False."""
        elapsed_min = (time.time() - self.start_t) / 60
        if AUTO_RESTART and elapsed_min > TIEMPO_RESTART_HIBRIDO:
            self._restart_script(f"Tiempo límite alcanzado ({elapsed_min:.1f}m)")

        curr_selenium_user = self._get_current_selenium_user()
        if self.driver and self._driver_closed():
            self._restart_script("Chrome cerrado/crasheado detectado en loop principal")

        self._ensure_driver_ready(curr_selenium_user)

        link = self._pop_link()
        if not link:
            return {"processed": 0, "last_send_duration": 0}

        cola_meta = {}
        try:
            cola_meta = Utils.load_json(self.f_meta) if os.path.exists(self.f_meta) else {}
        except Exception:
            cola_meta = {}
        link_meta = cola_meta.get(link, {}) if isinstance(cola_meta, dict) else {}
        link_created_ts = self._parse_iso_to_ts(link_meta.get("tweet_created_at"))
        if link_created_ts is not None and not self._is_tweet_fresh(link_created_ts):
            self._log(curr_selenium_user, f"⏭ Link expirado (> {MAX_TWEET_AGE_HOURS}h), no se cita ni reencola: {link}")
            with _LINK_STORE_LOCK:
                cola = Utils.load_json(self.f_cola)
                meta = Utils.load_json(self.f_meta) if os.path.exists(self.f_meta) else {}
                if isinstance(meta, dict):
                    meta.pop(link, None)
                Utils.save_json(self.f_cola, cola)
                Utils.save_json(self.f_meta, meta)
            return {"processed": 0, "last_send_duration": 0}

        with _LINK_STORE_LOCK:
            cola = Utils.load_json(self.f_cola)
            remaining_count = len(cola)
        
        self._log(curr_selenium_user, f"Procesando: {link} (En cola: {remaining_count})")
        self._switch_to_next_tab()
        success, dur = await self.execute(curr_selenium_user, link)
        
        if not success:
            with _LINK_STORE_LOCK:
                cola = Utils.load_json(self.f_cola)
                cola.insert(0, link)
                Utils.save_json(self.f_cola, cola)
            self._inst_user_log("Error al citar; devuelto a cola.")
            return {"processed": 0, "last_send_duration": 0}

        self._save_state()
        return {"processed": 1, "last_send_duration": dur}

def _split_evenly(items, buckets):
    buckets = max(1, int(buckets))
    if not items: return [[] for _ in range(buckets)]
    n = len(items)
    base, extra = divmod(n, buckets)
    result = []
    start = 0
    for i in range(buckets):
        size = base + (1 if i < extra else 0)
        chunk = items[start:start + size]
        if not chunk: chunk = [items[i % n]]
        result.append(chunk)
        start += size
    return result

def _build_instance_assignments(num_instancias):
    if not USERS: raise ValueError("USERS no puede estar vacío")
    groups = _split_evenly(USERS, num_instancias)
    assignments = []
    for i in range(num_instancias):
        assignments.append({
            "instancia_id": i + 1,
            "users": groups[i],
        })
    return assignments

_ACTIVE_BOTS = []

def _shutdown_all_bots():
    for bot in list(_ACTIVE_BOTS):
        try: bot.shutdown_driver()
        except: pass

atexit.register(_shutdown_all_bots)

if __name__ == "__main__":
    async def main():
        Utils.print_header()
        
        # --- NUEVO: IMPRIMIR PENDIENTES AL INICIO SI LA VARIABLE ES TRUE ---
        cola_inicio = Utils.load_json(GLOBAL_QUEUE_FILE)
        if PRINT_DETAILED_LOGS:
            Utils.log_global(f"📋 LISTA PENDIENTE ACTUAL ({len(cola_inicio)}):")
            for l in cola_inicio:
                Utils.log_global(f"   -> {l}")
        else:
             Utils.log_global(f"📋 Pendientes en cola: {len(cola_inicio)} (Detalles ocultos)")
        # -------------------------------------------------------------------

        kill_chrome_processes(Utils.log, Utils.SYSTEM_LABEL)
        num_instancias = max(1, NUM_INSTANCIAS)

        Utils.log_global("--- FASE 1: Verificación de cookies ---")
        users_needing_cookies = [u for u in USERS if not check_cookies_exist(u)]
        
        if users_needing_cookies:
            Utils.log_global(f"Usuarios sin cookies: {users_needing_cookies}. Iniciando exportación por lotes.")
            for i in range(0, len(users_needing_cookies), num_instancias):
                batch = users_needing_cookies[i:i + num_instancias]
                Utils.log_global(f"Procesando lote cookies: {batch}")
                tasks = []
                for idx, user in enumerate(batch):
                    tasks.append(export_cookies_task(user, idx + 1))
                await asyncio.gather(*tasks)
                kill_chrome_processes(None, None) 
        else:
            Utils.log_global("Todos los usuarios tienen cookies. Saltando exportación.")

        Utils.log_global("--- FASE 2: Iniciando instancias de cita ---")
        assignments = _build_instance_assignments(num_instancias)
        bots = [Bot(a["instancia_id"], a["users"], USERS, TABS_POR_INSTANCIA) for a in assignments]
        _ACTIVE_BOTS.clear(); _ACTIVE_BOTS.extend(bots)

        Utils.print_runtime_mode(num_instancias)
        for a in assignments:
            Utils.log(a['users'][0], f"Instancia {a['instancia_id']} | User Selenium: {a['users'][0]}", a['instancia_id'])

        for idx, bot in enumerate(bots):
            seu = bot._get_current_selenium_user()
            bot._ensure_driver_ready(seu)

        global_cycle_count = 0 
        Utils.log_global("Realizando Fetch inicial...")
        if bots:
            await bots[0].fetch(global_cycle_count)
            global_cycle_count += 1

        last_success_started_at = {bot.instancia_id: None for bot in bots}
        
        while True:
            # 1. CICLO DE CITAS
            hubo_citas = False
            total_turnos = max(1, int(TWEETS_POR_CICLO))
            
            for turno in range(1, total_turnos + 1):
                bot = bots[(turno - 1) % len(bots)]
                bot_idx = bot.instancia_id
                
                # Control Periodo
                last_start = last_success_started_at.get(bot_idx)
                remaining_exact = max(0.0, (last_start + PERIOD) - time.time()) if last_start else 0
                if remaining_exact > 0:
                    Utils.log(
                        bot._get_current_selenium_user(),
                        f"Respetando PERIOD={PERIOD}s | espera restante={remaining_exact:.1f}s",
                        bot_idx,
                    )
                    await Utils.countdown_async(remaining_exact, "Esperando period", user=bot._get_current_selenium_user(), instancia_id=bot_idx)

                action_start_t = time.time()
                
                # Ejecutar Acción
                result = await bot.process_action_only()
                processed = result.get("processed", 0)

                if processed > 0:
                    hubo_citas = True
                    last_success_started_at[bot_idx] = action_start_t
                    dur = result.get("last_send_duration", 0)
                    
                    remaining_period_calc = max(0, PERIOD - dur)
                    
                    Utils.log(
                        bot._get_current_selenium_user(), 
                        f"✅ Cita {turno}/{total_turnos} OK | tiempo envío={dur:.1f}s | restante para próximo PERIOD={remaining_period_calc:.1f}s", 
                        bot_idx
                    )
                    
                    if turno < total_turnos: 
                        await asyncio.sleep(3) 
                else:
                    Utils.log(bot._get_current_selenium_user(), "Cola vacía durante ciclo de citas. Pasando a extracción.")
                    break

            # 2. FASE DE EXTRACCION
            fetch_bot = bots[0]
            await fetch_bot.fetch(global_cycle_count)
            global_cycle_count += 1

            with _LINK_STORE_LOCK:
                cola_len = len(Utils.load_json(GLOBAL_QUEUE_FILE))
            
            if not hubo_citas and cola_len == 0:
                Utils.log_global(f"Sin links tras fetch; poll {POLL_INTERVAL}s")
                await asyncio.sleep(POLL_INTERVAL)

    try: asyncio.run(main())
    except KeyboardInterrupt: Utils.log_global("Detenido por usuario.")
    finally: _shutdown_all_bots()
