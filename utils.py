"""
utils.py - Utilidades compartilhadas do projeto
================================================
Funcoes utilitarias usadas por multiplos modulos:
hash de arquivos, sanitizacao, formatacao, config, historico, etc.
"""

import hashlib
import json
import logging
import os
import time
from datetime import datetime
from threading import Lock


# ============================================================
# CONSTANTES
# ============================================================

AUDIO_EXTENSIONS = {".mp3", ".flac", ".ogg", ".opus", ".m4a", ".wav", ".aac", ".wma"}

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(SCRIPT_DIR, "config.json")
HISTORICO_FILE = os.path.join(SCRIPT_DIR, "historico.json")
LOG_FILE = os.path.join(SCRIPT_DIR, "musicas.log")

DEFAULT_CONFIG = {
    "output_dir": "./musicas",
    "formato": "mp3",
    "default_export_path": "",
    "default_scan_path": "./final",
    "rate_limit_delay": 0.3,
    "qualidade_minima_kbps": 192,
}


# ============================================================
# LOGGING
# ============================================================

def setup_logging():
    """Configura logging para arquivo."""
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("musicas")


logger = setup_logging()


# ============================================================
# FORMATACAO
# ============================================================

def format_size(size_bytes):
    """Formata bytes para string legivel (KB, MB, GB)."""
    if size_bytes >= 1024 ** 3:
        return f"{size_bytes / 1024**3:.2f} GB"
    elif size_bytes >= 1024 ** 2:
        return f"{size_bytes / 1024**2:.1f} MB"
    elif size_bytes >= 1024:
        return f"{size_bytes / 1024:.1f} KB"
    return f"{size_bytes} B"


def format_duration(seconds):
    """Formata duracao em segundos para mm:ss ou hh:mm:ss."""
    if not seconds:
        return "0:00"
    seconds = float(seconds)
    if seconds >= 3600:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        s = int(seconds % 60)
        return f"{h}:{m:02d}:{s:02d}"
    m = int(seconds // 60)
    s = int(seconds % 60)
    return f"{m}:{s:02d}"


# ============================================================
# ARQUIVOS
# ============================================================

def file_hash(filepath, chunk_size=8192):
    """Calcula SHA-256 de um arquivo."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def sanitize_filename(name):
    """Remove caracteres invalidos para nome de pasta/arquivo."""
    invalid = '<>:"/\\|?*'
    for c in invalid:
        name = name.replace(c, "")
    return name.strip()


def cleanup_empty_dirs(path):
    """Remove pastas vazias recursivamente. Retorna quantidade removida."""
    removed = 0
    for root, dirs, files in os.walk(path, topdown=False):
        for d in dirs:
            dirpath = os.path.join(root, d)
            try:
                if not os.listdir(dirpath):
                    os.rmdir(dirpath)
                    removed += 1
            except OSError:
                pass
    return removed


def is_audio_file(filename):
    """Verifica se o arquivo e de audio pela extensao."""
    _, ext = os.path.splitext(filename)
    return ext.lower() in AUDIO_EXTENSIONS


# ============================================================
# RATE LIMITER
# ============================================================

class RateLimiter:
    """Rate limiter simples para chamadas de API."""

    def __init__(self, min_interval=0.3):
        self.min_interval = min_interval
        self._last_call = 0.0
        self._lock = Lock()

    def wait(self):
        """Espera o tempo minimo entre chamadas."""
        with self._lock:
            now = time.time()
            elapsed = now - self._last_call
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self._last_call = time.time()


# ============================================================
# CONFIGURACAO
# ============================================================

def load_config():
    """Carrega config do config.json. Cria com defaults se nao existir."""
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                user_config = json.load(f)
            # Merge com defaults (defaults preenchem campos faltantes)
            config = {**DEFAULT_CONFIG, **user_config}
            return config
        except Exception:
            pass
    # Criar config padrao
    save_config(DEFAULT_CONFIG)
    return DEFAULT_CONFIG.copy()


def save_config(config):
    """Salva config no config.json."""
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)


# ============================================================
# HISTORICO
# ============================================================

def load_historico():
    """Carrega historico de downloads."""
    if os.path.exists(HISTORICO_FILE):
        try:
            with open(HISTORICO_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"downloads": [], "stats": {"total_downloads": 0, "total_bytes": 0}}


def save_historico(historico):
    """Salva historico de downloads."""
    with open(HISTORICO_FILE, "w", encoding="utf-8") as f:
        json.dump(historico, f, ensure_ascii=False, indent=2)


def log_download(artist, title, album="", size=0, status="ok", source="deezer"):
    """Registra um download no historico."""
    historico = load_historico()
    historico["downloads"].append({
        "artist": artist,
        "title": title,
        "album": album,
        "size": size,
        "status": status,
        "source": source,
        "timestamp": datetime.now().isoformat(),
    })
    if status == "ok":
        historico["stats"]["total_downloads"] += 1
        historico["stats"]["total_bytes"] += size
    save_historico(historico)
    logger.info(f"Download: {artist} - {title} ({status})")
