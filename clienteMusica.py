"""
clienteMusica.py - Cliente unificado de musicas Top Brasil
==========================================================
Menu interativo que centraliza scraping do Top 50 Brasil
e download de musicas/albuns via yt-dlp.

Uso:
    python clienteMusica.py
"""

import argparse
import hashlib
import json
import os
import random
import re
import shutil
import subprocess
import sys
import time
from collections import Counter

import requests

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.progress import (
    Progress,
    SpinnerColumn,
    BarColumn,
    TextColumn,
    TimeElapsedColumn,
    MofNCompleteColumn,
)
from rich import box

from scrapper import (
    DeezerClient,
    Artist,
    Album,
    HEADERS,
)
from down_albuns import (
    download_track,
    get_deezer_tracklist,
    sanitize_filename,
    check_ytdlp,
    check_ffmpeg,
    YTDLP_PATH,
)
from organizar_musicas import organizar, scan_audio_files
from utils import (
    format_size,
    format_duration,
    file_hash,
    cleanup_empty_dirs,
    RateLimiter,
    load_config,
    save_config,
    load_historico,
    save_historico,
    log_download,
    AUDIO_EXTENSIONS,
    logger,
)

# ── Mutagen (opcional - para editor de tags) ──
try:
    import mutagen
    from mutagen.mp3 import MP3
    from mutagen.id3 import ID3, TIT2, TPE1, TALB, TCON, APIC, ID3NoHeaderError
    from mutagen.flac import FLAC
    HAS_MUTAGEN = True
except ImportError:
    HAS_MUTAGEN = False


# ============================================================
# CONFIG
# ============================================================

DEEZER_API = "https://api.deezer.com"
SPOTIFY_PLAYLIST_URL = "https://open.spotify.com/playlist/37i9dQZEVXbMXbN3EUUhlg"
SPOTIFY_EMBED_URL = "https://open.spotify.com/embed/playlist/37i9dQZEVXbMXbN3EUUhlg"
YOUTUBE_PLAYLIST_URL = "https://www.youtube.com/playlist?list=PLgzTt0k8mXzGdzIHo-6T1jEUOyYgqdCsJ"
OUTPUT_DIR = "./musicas"
FORMATO = "mp3"

console = Console()


# ============================================================
# CLIENTE
# ============================================================

class ClienteMusica:
    """Cliente unificado - gerencia estado e operacoes."""

    def __init__(self):
        self.deezer = DeezerClient()
        self.top50_artists = None  # cache: list[(nome, contagem)]
        self.config = load_config()
        self.output_dir = self.config.get("output_dir", OUTPUT_DIR)
        self.formato = self.config.get("formato", FORMATO)
        self.rate_limiter = RateLimiter(self.config.get("rate_limit_delay", 0.3))
        self._artist_cache = {}  # cache de busca de artistas Deezer

    # --------------------------------------------------------
    # DEEZER HELPERS
    # --------------------------------------------------------

    def buscar_artista(self, nome):
        """Busca artista no Deezer. Retorna dict com id, name, nb_fan ou None.
        Resultado e cacheado para evitar requests repetidos."""
        cache_key = nome.lower().strip()
        if cache_key in self._artist_cache:
            return self._artist_cache[cache_key]
        try:
            self.rate_limiter.wait()
            resp = requests.get(
                f"{DEEZER_API}/search/artist",
                params={"q": nome, "limit": 1},
                timeout=10,
            )
            items = resp.json().get("data", [])
            if not items:
                self._artist_cache[cache_key] = None
                return None
            a = items[0]
            result = {
                "id": a["id"],
                "name": a["name"],
                "nb_fan": a.get("nb_fan", 0),
            }
            self._artist_cache[cache_key] = result
            return result
        except Exception as e:
            logger.error(f"Erro buscando artista '{nome}': {e}")
            return None

    def get_top_tracks(self, artist_id, limit=10):
        """Top tracks de um artista via Deezer API."""
        try:
            resp = requests.get(
                f"{DEEZER_API}/artist/{artist_id}/top",
                params={"limit": limit},
                timeout=10,
            )
            tracks = []
            for t in resp.json().get("data", []):
                tracks.append({
                    "title": t.get("title", "?"),
                    "rank": t.get("rank", 0),
                    "duration": t.get("duration", 0),
                    "artist": t.get("artist", {}).get("name", ""),
                })
            return tracks
        except Exception:
            return []

    def get_albums_with_metrics(self, artist_id, limit=10):
        """Albums de um artista com metrica de fans."""
        try:
            resp = requests.get(
                f"{DEEZER_API}/artist/{artist_id}/albums",
                params={"limit": limit, "order": "RANKING"},
                timeout=10,
            )
            albums = []
            seen = set()
            for a in resp.json().get("data", []):
                # Pular duplicatas
                clean = re.sub(r"\s*[\(\[].*?[\)\]]\s*", "", a.get("title", "")).strip().lower()
                if clean in seen:
                    continue
                seen.add(clean)

                # Buscar detalhe do album para nb_tracks
                detail = {}
                try:
                    d = requests.get(f"{DEEZER_API}/album/{a['id']}", timeout=10)
                    detail = d.json()
                except Exception:
                    pass

                albums.append({
                    "id": a["id"],
                    "title": a.get("title", "?"),
                    "fans": a.get("fans", 0),
                    "nb_tracks": detail.get("nb_tracks", 0),
                    "release_date": a.get("release_date", "N/A"),
                    "deezer_url": a.get("link", ""),
                })

            albums.sort(key=lambda x: x["fans"], reverse=True)
            return albums
        except Exception:
            return []

    def fetch_top50_spotify(self):
        """Busca artistas do Top 50 Brasil via Spotify embed API."""
        console.print("  Acessando Spotify Top 50 Brasil...", style="dim")
        try:
            resp = requests.get(
                SPOTIFY_EMBED_URL,
                headers={"User-Agent": HEADERS["User-Agent"]},
                timeout=15,
            )
            resp.raise_for_status()

            match = re.search(
                r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.+?)</script>',
                resp.text,
            )
            if not match:
                console.print("  [red]Formato da pagina mudou.[/]")
                return False

            data = json.loads(match.group(1))
            track_list = (
                data["props"]["pageProps"]["state"]["data"]["entity"]["trackList"]
            )

            counter = Counter()
            for t in track_list:
                subtitle = t.get("subtitle", "")
                for name in subtitle.replace("\xa0", " ").split(","):
                    name = name.strip()
                    if name:
                        counter[name] += 1

            if counter:
                self.top50_artists = counter.most_common()
                self._top50_fonte = "Spotify"
                return True
        except Exception as e:
            console.print(f"  [red]Erro: {e}[/]")
        return False

    def fetch_top50_deezer(self):
        """Busca artistas do Top Brasil via Deezer API (playlists)."""
        console.print("  Buscando playlists Top Brasil no Deezer...", style="dim")
        counter = Counter()
        try:
            search = requests.get(
                f"{DEEZER_API}/search/playlist",
                params={"q": "top 50 brasil", "limit": 5},
                timeout=10,
            )
            playlists = search.json().get("data", [])

            for pl in playlists[:3]:
                pl_id = pl.get("id")
                if not pl_id:
                    continue
                tracks = requests.get(
                    f"{DEEZER_API}/playlist/{pl_id}/tracks",
                    params={"limit": 100},
                    timeout=10,
                )
                if tracks.status_code == 200:
                    for t in tracks.json().get("data", []):
                        name = t.get("artist", {}).get("name", "")
                        if name:
                            counter[name] += 1
                time.sleep(0.3)

            if counter:
                self.top50_artists = counter.most_common()
                self._top50_fonte = "Deezer"
                return True
        except Exception as e:
            console.print(f"  [red]Erro: {e}[/]")
        return False

    def fetch_top50_youtube(self):
        """Busca artistas do Top 50 Brasil via YouTube Music playlist."""
        console.print("  Buscando playlist YouTube Music...", style="dim")
        try:
            proc = subprocess.run(
                [
                    YTDLP_PATH, "--flat-playlist", "--dump-json",
                    "--no-warnings", YOUTUBE_PLAYLIST_URL,
                ],
                capture_output=True, text=True, timeout=60,
            )
            if proc.returncode != 0:
                console.print("  [red]Erro ao acessar playlist.[/]")
                return False

            counter = Counter()
            for line in proc.stdout.strip().split("\n"):
                if not line:
                    continue
                d = json.loads(line)
                channel = d.get("channel") or d.get("uploader") or ""
                if channel:
                    clean = re.sub(
                        r"\s*(Oficial|Official|VEVO|Topic|Music)$",
                        "", channel, flags=re.IGNORECASE,
                    ).strip()
                    if clean:
                        counter[clean] += 1

            if counter:
                self.top50_artists = counter.most_common()
                self._top50_fonte = "YouTube Music"
                return True
        except subprocess.TimeoutExpired:
            console.print("  [red]Timeout ao acessar YouTube.[/]")
        except Exception as e:
            console.print(f"  [red]Erro: {e}[/]")
        return False

    def ensure_top50(self):
        """Garante que top50_artists esta carregado. Retorna True se ok."""
        if self.top50_artists:
            return True
        console.print("\n[yellow]Dados do Top 50 ainda nao carregados.[/]")
        console.print("  [cyan][1][/] Spotify")
        console.print("  [cyan][2][/] Deezer")
        console.print("  [cyan][3][/] YouTube Music")
        choice = input("\n  Fonte: ").strip()
        if choice == "1":
            return self.fetch_top50_spotify()
        elif choice == "3":
            return self.fetch_top50_youtube()
        return self.fetch_top50_deezer()

    # --------------------------------------------------------
    # TOP MUSICAS DO MOMENTO (songs, nao artistas)
    # --------------------------------------------------------

    def fetch_top_songs_spotify(self):
        """Top musicas do Brasil via Spotify embed."""
        console.print("  Acessando Spotify Top 50 Brasil...", style="dim")
        try:
            resp = requests.get(
                SPOTIFY_EMBED_URL,
                headers={"User-Agent": HEADERS["User-Agent"]},
                timeout=15,
            )
            resp.raise_for_status()
            match = re.search(
                r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.+?)</script>',
                resp.text,
            )
            if not match:
                return []
            data = json.loads(match.group(1))
            track_list = (
                data["props"]["pageProps"]["state"]["data"]["entity"]["trackList"]
            )
            songs = []
            for i, t in enumerate(track_list, 1):
                artist = t.get("subtitle", "").replace("\xa0", " ").strip()
                songs.append({
                    "pos": i,
                    "title": t.get("title", "?"),
                    "artist": artist,
                })
            return songs
        except Exception as e:
            console.print(f"  [red]Erro: {e}[/]")
            return []

    def fetch_top_songs_deezer(self):
        """Top musicas do Brasil via Deezer chart API."""
        console.print("  Acessando Deezer Charts...", style="dim")
        try:
            resp = requests.get(
                f"{DEEZER_API}/chart/0/tracks",
                params={"limit": 50},
                timeout=10,
            )
            songs = []
            for i, t in enumerate(resp.json().get("data", []), 1):
                dur = t.get("duration", 0)
                songs.append({
                    "pos": t.get("position", i),
                    "title": t.get("title", "?"),
                    "artist": t.get("artist", {}).get("name", "?"),
                    "rank": t.get("rank", 0),
                    "duration": dur,
                })
            return songs
        except Exception as e:
            console.print(f"  [red]Erro: {e}[/]")
            return []

    def fetch_top_songs_youtube(self):
        """Top musicas do Brasil via YouTube Music playlist."""
        console.print("  Buscando playlist YouTube Music...", style="dim")
        try:
            proc = subprocess.run(
                [
                    YTDLP_PATH, "--flat-playlist", "--dump-json",
                    "--no-warnings", YOUTUBE_PLAYLIST_URL,
                ],
                capture_output=True, text=True, timeout=60,
            )
            if proc.returncode != 0:
                return []
            songs = []
            for i, line in enumerate(proc.stdout.strip().split("\n"), 1):
                if not line:
                    continue
                d = json.loads(line)
                channel = d.get("channel") or d.get("uploader") or ""
                clean = re.sub(
                    r"\s*(Oficial|Official|VEVO|Topic|Music)$",
                    "", channel, flags=re.IGNORECASE,
                ).strip() if channel else ""
                # Limpar titulo: extrair nome da musica
                title = d.get("title", "?")
                # Formato comum: "Artista - Musica (Video Oficial)"
                if " - " in title:
                    parts = title.split(" - ", 1)
                    title = parts[1]
                # Remover tags de video
                title = re.sub(
                    r"\s*[\(\[](V[ií]deo\s*Oficial|Official\s*(Video|Music\s*Video)|Clipe\s*Oficial|Lyric\s*Video|[AÁ]udio\s*Oficial|Visualizer)[\)\]]",
                    "", title, flags=re.IGNORECASE,
                ).strip()
                # Remover sufixos como "| Ecoando Amazon Music Brasil"
                title = re.sub(r"\s*\|.*$", "", title).strip()
                songs.append({
                    "pos": i,
                    "title": title or d.get("title", "?"),
                    "artist": clean or "Desconhecido",
                    "duration": d.get("duration") or 0,
                })
            return songs
        except subprocess.TimeoutExpired:
            console.print("  [red]Timeout ao acessar YouTube.[/]")
        except Exception as e:
            console.print(f"  [red]Erro: {e}[/]")
        return []

    # --------------------------------------------------------
    # SELECAO DE ITENS
    # --------------------------------------------------------

    def selecionar_items(self, total):
        """
        Pede ao usuario para selecionar itens.
        Aceita: 1,3,5 | 1-5 | todas
        Retorna lista de indices (1-based).
        """
        console.print(
            "\n  Selecione: [cyan]numeros[/] (1,3,5), "
            "[cyan]range[/] (1-5), ou [cyan]todas[/]"
        )
        choice = input("  > ").strip().lower()

        if choice in ("todas", "todos", "all", "*"):
            return list(range(1, total + 1))

        selected = []
        for part in choice.split(","):
            part = part.strip()
            if "-" in part:
                try:
                    a, b = part.split("-")
                    for i in range(int(a), int(b) + 1):
                        if 1 <= i <= total:
                            selected.append(i)
                except ValueError:
                    pass
            else:
                try:
                    i = int(part)
                    if 1 <= i <= total:
                        selected.append(i)
                except ValueError:
                    pass
        return selected

    # --------------------------------------------------------
    # DOWNLOAD COM PROGRESSO
    # --------------------------------------------------------

    def download_tracks_with_progress(self, tracks, artist_name):
        """
        Baixa uma lista de tracks com barras de progresso Rich.
        tracks: list[dict] com title (e opcionalmente number).
        Retorna (ok, fail).
        """
        ok_count = 0
        fail_count = 0

        with Progress(
            SpinnerColumn("dots"),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=30),
            MofNCompleteColumn(),
            TextColumn("[dim]|[/]"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(
                f"[cyan]{artist_name}[/]", total=len(tracks),
            )
            dl_task = progress.add_task(
                "[dim]...[/]", total=100, visible=False,
            )

            for i, track in enumerate(tracks, 1):
                title = track["title"]
                num = track.get("number", i)
                album = track.get("album", "Singles")

                progress.update(
                    task,
                    description=f"[cyan]{num:02d}[/] {title}",
                )
                progress.update(
                    dl_task, total=100, completed=0, visible=True,
                    description="[dim]...[/]",
                )

                def on_progress(pct, size_str, speed_str):
                    desc = f"[dim]{size_str}[/]"
                    if speed_str:
                        desc += f" [dim]@ {speed_str}[/]"
                    progress.update(dl_task, completed=pct, description=desc)

                success, msg = download_track(
                    artist_name, title, album, num,
                    self.output_dir, self.formato,
                    progress_callback=on_progress,
                )

                if success:
                    ok_count += 1
                    progress.update(dl_task, completed=100)
                    if msg.startswith("Ja existe"):
                        progress.update(
                            dl_task, description=f"[yellow]Pulado[/] [dim]{msg}[/]",
                        )
                else:
                    fail_count += 1
                    console.print(f"    [red]x[/] {title}: [dim]{msg}[/]")

                progress.advance(task)

            progress.update(dl_task, visible=False)

        return ok_count, fail_count

    def download_album_by_deezer_url(self, deezer_url, artist_name, album_name):
        """Baixa album completo: busca tracklist no Deezer e baixa cada faixa."""
        tracks = get_deezer_tracklist(deezer_url)
        if not tracks:
            console.print("[red]Nao foi possivel obter tracklist do Deezer.[/]")
            return 0, 0

        console.print(
            f"\n  [green]{len(tracks)}[/] faixas encontradas em "
            f"[cyan]{album_name}[/]"
        )

        dl_tracks = []
        for t in tracks:
            dl_tracks.append({
                "title": t["title"],
                "number": t["number"],
                "album": album_name,
            })

        return self.download_tracks_with_progress(dl_tracks, artist_name)

    # --------------------------------------------------------
    # OPCAO 1: VER TOP 50
    # --------------------------------------------------------

    def opcao_ver_top50(self):
        console.print(Panel(
            "[bold]Ver artistas do Top 50 Brasil[/]",
            border_style="cyan",
        ))

        console.print("  Escolha a fonte:")
        console.print("  [cyan][1][/] Spotify (embed da playlist)")
        console.print("  [cyan][2][/] Deezer (API - mais confiavel)")
        console.print("  [cyan][3][/] YouTube Music (playlist Top 50)")

        choice = input("\n  Fonte: ").strip()
        if choice == "1":
            ok = self.fetch_top50_spotify()
        elif choice == "3":
            ok = self.fetch_top50_youtube()
        else:
            ok = self.fetch_top50_deezer()

        if not ok or not self.top50_artists:
            console.print("[red]Nao foi possivel obter dados.[/]")
            return

        table = Table(
            title="Artistas no Top 50 Brasil",
            box=box.ROUNDED,
            title_style="bold magenta",
        )
        table.add_column("#", style="dim", width=4, justify="right")
        table.add_column("Artista", style="cyan bold")
        table.add_column("Aparicoes", justify="center", style="green")

        for i, (name, count) in enumerate(self.top50_artists, 1):
            table.add_row(str(i), name, str(count))

        console.print()
        console.print(table)
        console.print(
            f"\n  [dim]Total: {len(self.top50_artists)} artistas unicos[/]"
        )

        # Selecionar artistas para acao
        console.print(
            "\n  Selecionar artistas? "
            "([cyan]s[/]/n)"
        )
        resp = input("  > ").strip().lower()
        if resp not in ("s", "sim", "y", "yes"):
            return

        selected = self.selecionar_items(len(self.top50_artists))
        if not selected:
            console.print("[yellow]Nenhum selecionado.[/]")
            return

        nomes = [self.top50_artists[i - 1][0] for i in selected]
        console.print(f"\n  [green]{len(nomes)}[/] artista(s) selecionado(s)")
        console.print("  O que fazer?")
        console.print("  [cyan][1][/] Ver top musicas")
        console.print("  [cyan][2][/] Ver albuns (com metricas)")
        console.print("  [cyan][3][/] Baixar top musicas")
        acao = input("\n  Acao: ").strip()

        for nome in nomes:
            console.print(Panel(
                f"[bold cyan]{nome}[/]", border_style="dim",
            ))
            info = self.buscar_artista(nome)
            if not info:
                console.print(f"  [red]'{nome}' nao encontrado no Deezer.[/]")
                continue

            if acao == "2":
                albums = self.get_albums_with_metrics(info["id"], limit=10)
                if not albums:
                    console.print("  [red]Nenhum album encontrado.[/]")
                    continue
                t = Table(box=box.ROUNDED, title_style="bold")
                t.add_column("#", style="dim", width=4, justify="right")
                t.add_column("Album", style="green")
                t.add_column("Fans", justify="right", style="yellow")
                t.add_column("Faixas", justify="center")
                t.add_column("Data", style="dim", width=12)
                for i, a in enumerate(albums, 1):
                    fans_str = f"{a['fans']:,}" if a["fans"] else "-"
                    tracks_str = str(a["nb_tracks"]) if a["nb_tracks"] else "-"
                    t.add_row(str(i), a["title"], fans_str, tracks_str, a["release_date"])
                console.print(t)
                console.print("\n  Baixar algum album? ([cyan]s[/]/n)")
                if input("  > ").strip().lower() in ("s", "sim", "y", "yes"):
                    sel = self.selecionar_items(len(albums))
                    for idx in sel:
                        a = albums[idx - 1]
                        url = a.get("deezer_url") or f"https://www.deezer.com/album/{a['id']}"
                        self.download_album_by_deezer_url(url, info["name"], a["title"])
            else:
                # acao 1 ou 3: top musicas
                tracks = self.get_top_tracks(info["id"], limit=10)
                if not tracks:
                    console.print("  [red]Nenhuma track encontrada.[/]")
                    continue
                t = Table(box=box.ROUNDED, title_style="bold")
                t.add_column("#", style="dim", width=4, justify="right")
                t.add_column("Musica", style="green")
                t.add_column("Popularidade", justify="right", style="yellow")
                t.add_column("Duracao", justify="center", style="dim")
                for i, tr in enumerate(tracks, 1):
                    mins = tr["duration"] // 60
                    secs = tr["duration"] % 60
                    rank_str = f"{tr['rank']:,}" if tr["rank"] else "-"
                    t.add_row(str(i), tr["title"], rank_str, f"{mins}:{secs:02d}")
                console.print(t)

                if acao == "3":
                    # Baixar todas automaticamente
                    dl = [{"title": tr["title"], "number": i, "album": "Top Tracks"}
                          for i, tr in enumerate(tracks, 1)]
                    self.download_tracks_with_progress(dl, info["name"])
                else:
                    console.print("\n  Baixar alguma? ([cyan]s[/]/n)")
                    if input("  > ").strip().lower() in ("s", "sim", "y", "yes"):
                        sel = self.selecionar_items(len(tracks))
                        dl = [{"title": tracks[i-1]["title"], "number": i, "album": "Top Tracks"}
                              for i in sel]
                        self.download_tracks_with_progress(dl, info["name"])

    # --------------------------------------------------------
    # OPCAO 2: MAIS FREQUENTES
    # --------------------------------------------------------

    def opcao_mais_frequentes(self):
        console.print(Panel(
            "[bold]Artistas mais frequentes no Top 50[/]",
            border_style="cyan",
        ))

        if not self.ensure_top50():
            return

        top10 = self.top50_artists[:10]

        table = Table(
            title="Top 10 - Mais Frequentes",
            box=box.ROUNDED,
            title_style="bold magenta",
        )
        table.add_column("#", style="dim", width=4, justify="right")
        table.add_column("Artista", style="cyan bold")
        table.add_column("Aparicoes", justify="center", style="green")
        table.add_column("Fans (Deezer)", justify="right", style="yellow")

        console.print("  [dim]Buscando dados no Deezer...[/]")

        for i, (name, count) in enumerate(top10, 1):
            info = self.buscar_artista(name)
            fans = ""
            if info:
                nb = info["nb_fan"]
                if nb >= 1_000_000:
                    fans = f"{nb / 1_000_000:.1f}M"
                elif nb >= 1_000:
                    fans = f"{nb / 1_000:.0f}K"
                else:
                    fans = str(nb)
            table.add_row(str(i), name, str(count), fans)
            time.sleep(0.2)

        console.print()
        console.print(table)

    # --------------------------------------------------------
    # OPCAO 3: BAIXAR DOS MAIS FREQUENTES
    # --------------------------------------------------------

    def opcao_baixar_frequentes(self):
        console.print(Panel(
            "[bold]Baixar albuns dos mais frequentes[/]",
            border_style="cyan",
        ))

        if not self.ensure_top50():
            return

        top10 = self.top50_artists[:10]

        console.print("  [dim]Buscando top 2 albuns de cada artista...[/]\n")

        all_albums = []
        for name, count in top10:
            artist = Artist(name=name, track_count=count)
            albums = self.deezer.get_top_albums(artist, limit=2)
            for album in albums:
                all_albums.append({
                    "artist": name,
                    "album_name": album.name,
                    "deezer_url": album.deezer_url,
                    "release_date": album.release_date,
                })
            time.sleep(0.2)

        if not all_albums:
            console.print("[red]Nenhum album encontrado.[/]")
            return

        # Exibir tabela
        table = Table(
            title="Albuns para Download",
            box=box.ROUNDED,
            title_style="bold",
        )
        table.add_column("#", style="dim", width=4, justify="right")
        table.add_column("Artista", style="cyan")
        table.add_column("Album", style="green")
        table.add_column("Data", style="yellow", width=12)

        for i, a in enumerate(all_albums, 1):
            table.add_row(
                str(i), a["artist"], a["album_name"], a["release_date"],
            )

        console.print(table)

        # Selecionar
        selected = self.selecionar_items(len(all_albums))
        if not selected:
            console.print("[yellow]Nenhum selecionado.[/]")
            return

        console.print(
            f"\n  Baixando [green]{len(selected)}[/] album(ns)...\n"
        )

        grand_ok, grand_fail = 0, 0
        for idx in selected:
            a = all_albums[idx - 1]
            console.print(Panel(
                f"[cyan]{a['artist']}[/] - [green]{a['album_name']}[/]",
                border_style="dim",
            ))
            ok, fail = self.download_album_by_deezer_url(
                a["deezer_url"], a["artist"], a["album_name"],
            )
            grand_ok += ok
            grand_fail += fail

        self._show_download_result(grand_ok, grand_fail)

    # --------------------------------------------------------
    # OPCAO 4: TOP MUSICAS DE UM ARTISTA
    # --------------------------------------------------------

    def opcao_top_musicas(self):
        console.print(Panel(
            "[bold]Baixar top musicas de um artista[/]",
            border_style="cyan",
        ))

        nome = input("  Nome do artista: ").strip()
        if not nome:
            return

        console.print(f"  [dim]Buscando '{nome}' no Deezer...[/]")
        info = self.buscar_artista(nome)
        if not info:
            console.print(f"[red]Artista '{nome}' nao encontrado.[/]")
            return

        console.print(
            f"  [green]Encontrado:[/] {info['name']} "
            f"([dim]{info['nb_fan']:,} fans[/])\n"
        )

        tracks = self.get_top_tracks(info["id"], limit=10)
        if not tracks:
            console.print("[red]Nenhuma track encontrada.[/]")
            return

        table = Table(
            title=f"Top Musicas - {info['name']}",
            box=box.ROUNDED,
            title_style="bold",
        )
        table.add_column("#", style="dim", width=4, justify="right")
        table.add_column("Musica", style="green")
        table.add_column("Popularidade", justify="right", style="yellow")
        table.add_column("Duracao", justify="center", style="dim")

        for i, t in enumerate(tracks, 1):
            mins = t["duration"] // 60
            secs = t["duration"] % 60
            rank_str = f"{t['rank']:,}" if t["rank"] else "-"
            table.add_row(
                str(i), t["title"], rank_str, f"{mins}:{secs:02d}",
            )

        console.print(table)

        # Selecionar
        selected = self.selecionar_items(len(tracks))
        if not selected:
            console.print("[yellow]Nenhuma selecionada.[/]")
            return

        dl_tracks = []
        for idx in selected:
            t = tracks[idx - 1]
            dl_tracks.append({
                "title": t["title"],
                "number": idx,
                "album": "Top Tracks",
            })

        ok, fail = self.download_tracks_with_progress(dl_tracks, info["name"])
        self._show_download_result(ok, fail)

    # --------------------------------------------------------
    # OPCAO 5: BUSCAR ALBUNS COM METRICAS
    # --------------------------------------------------------

    def opcao_buscar_albuns(self):
        console.print(Panel(
            "[bold]Buscar albuns de um artista (com metricas)[/]",
            border_style="cyan",
        ))

        nome = input("  Nome do artista: ").strip()
        if not nome:
            return

        console.print(f"  [dim]Buscando '{nome}' no Deezer...[/]")
        info = self.buscar_artista(nome)
        if not info:
            console.print(f"[red]Artista '{nome}' nao encontrado.[/]")
            return

        console.print(
            f"  [green]Encontrado:[/] {info['name']} "
            f"([dim]{info['nb_fan']:,} fans[/])\n"
        )

        console.print("  [dim]Buscando albuns...[/]")
        albums = self.get_albums_with_metrics(info["id"], limit=15)
        if not albums:
            console.print("[red]Nenhum album encontrado.[/]")
            return

        table = Table(
            title=f"Albuns - {info['name']}",
            box=box.ROUNDED,
            title_style="bold",
        )
        table.add_column("#", style="dim", width=4, justify="right")
        table.add_column("Album", style="green")
        table.add_column("Fans", justify="right", style="yellow")
        table.add_column("Faixas", justify="center")
        table.add_column("Data", style="dim", width=12)

        for i, a in enumerate(albums, 1):
            fans_str = f"{a['fans']:,}" if a["fans"] else "-"
            tracks_str = str(a["nb_tracks"]) if a["nb_tracks"] else "-"
            table.add_row(
                str(i), a["title"], fans_str, tracks_str, a["release_date"],
            )

        console.print(table)
        console.print(
            "\n  [dim]Ordenado por fans (mais popular primeiro)[/]"
        )

        # Selecionar
        selected = self.selecionar_items(len(albums))
        if not selected:
            console.print("[yellow]Nenhum selecionado.[/]")
            return

        grand_ok, grand_fail = 0, 0
        for idx in selected:
            a = albums[idx - 1]
            deezer_url = a.get("deezer_url") or f"https://www.deezer.com/album/{a['id']}"
            console.print(Panel(
                f"[cyan]{info['name']}[/] - [green]{a['title']}[/]",
                border_style="dim",
            ))
            ok, fail = self.download_album_by_deezer_url(
                deezer_url, info["name"], a["title"],
            )
            grand_ok += ok
            grand_fail += fail

        self._show_download_result(grand_ok, grand_fail)

    # --------------------------------------------------------
    # OPCAO 6: TOP MUSICAS DO MOMENTO
    # --------------------------------------------------------

    def opcao_top_musicas_momento(self):
        console.print(Panel(
            "[bold]Top musicas do momento - Brasil[/]",
            border_style="cyan",
        ))

        console.print("  Escolha a fonte:")
        console.print("  [cyan][1][/] Spotify (embed da playlist)")
        console.print("  [cyan][2][/] Deezer (charts)")
        console.print("  [cyan][3][/] YouTube Music (playlist Top 50)")

        choice = input("\n  Fonte: ").strip()
        if choice == "1":
            songs = self.fetch_top_songs_spotify()
            fonte = "Spotify"
        elif choice == "3":
            songs = self.fetch_top_songs_youtube()
            fonte = "YouTube Music"
        else:
            songs = self.fetch_top_songs_deezer()
            fonte = "Deezer"

        if not songs:
            console.print("[red]Nao foi possivel obter dados.[/]")
            return

        table = Table(
            title=f"Top Musicas do Momento - {fonte}",
            box=box.ROUNDED,
            title_style="bold magenta",
        )
        table.add_column("#", style="dim", width=4, justify="right")
        table.add_column("Musica", style="green")
        table.add_column("Artista", style="cyan")
        if any(s.get("duration") for s in songs):
            table.add_column("Duracao", justify="center", style="dim")
            has_dur = True
        else:
            has_dur = False

        for i, s in enumerate(songs, 1):
            row = [str(i), s["title"], s["artist"]]
            if has_dur:
                dur = s.get("duration", 0)
                if dur:
                    row.append(f"{dur // 60}:{dur % 60:02d}")
                else:
                    row.append("-")
            table.add_row(*row)

        console.print()
        console.print(table)
        console.print(f"\n  [dim]Total: {len(songs)} musicas | Fonte: {fonte}[/]")

        # Oferecer download
        console.print(
            "\n  Deseja baixar alguma? "
            "([cyan]s[/]/n)"
        )
        dl = input("  > ").strip().lower()
        if dl not in ("s", "sim", "y", "yes"):
            return

        selected = self.selecionar_items(len(songs))
        if not selected:
            console.print("[yellow]Nenhuma selecionada.[/]")
            return

        dl_tracks = []
        for idx in selected:
            s = songs[idx - 1]
            main_artist = s["artist"].split(",")[0].strip()
            dl_tracks.append({
                "title": s["title"],
                "number": idx,
                "_artist": main_artist,
            })

        # Pasta dedicada: musicas/Top Momento/
        pasta = os.path.join(self.output_dir, f"Top Momento")
        os.makedirs(pasta, exist_ok=True)
        console.print(
            f"\n  Salvando em [cyan]{pasta}/[/]\n"
        )

        ok_count, fail_count = 0, 0
        with Progress(
            SpinnerColumn("dots"),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=30),
            MofNCompleteColumn(),
            TextColumn("[dim]|[/]"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(
                "[cyan]Download[/]", total=len(dl_tracks),
            )
            dl_task = progress.add_task(
                "[dim]...[/]", total=100, visible=False,
            )

            for track in dl_tracks:
                artist = track["_artist"]
                title = track["title"]
                num = track["number"]
                progress.update(
                    task,
                    description=f"[cyan]{artist}[/] - {title}",
                )
                progress.update(
                    dl_task, total=100, completed=0, visible=True,
                    description="[dim]...[/]",
                )

                def on_progress(pct, size_str, speed_str):
                    desc = f"[dim]{size_str}[/]"
                    if speed_str:
                        desc += f" [dim]@ {speed_str}[/]"
                    progress.update(dl_task, completed=pct, description=desc)

                # Baixar direto na pasta Top Momento (sem subpastas)
                success, msg = self._download_to_folder(
                    artist, title, num, pasta, on_progress,
                )
                if success:
                    ok_count += 1
                    progress.update(dl_task, completed=100)
                    if msg.startswith("Ja existe"):
                        progress.update(
                            dl_task, description=f"[yellow]Pulado[/] [dim]{msg}[/]",
                        )
                else:
                    fail_count += 1
                    console.print(f"    [red]x[/] {title}: [dim]{msg}[/]")

                progress.advance(task)

            progress.update(dl_task, visible=False)

        self._show_download_result(ok_count, fail_count)

    # --------------------------------------------------------
    # OPCAO 7: EXPORTAR MUSICAS
    # --------------------------------------------------------

    def opcao_exportar(self):
        console.print(Panel(
            "[bold]Exportar musicas para outra pasta[/]",
            border_style="cyan",
        ))

        # Verificar se tem musicas
        files = scan_audio_files(self.output_dir)
        if not files:
            console.print("[red]Nenhuma musica encontrada para exportar.[/]")
            console.print(f"  [dim]Pasta: {self.output_dir}[/]")
            return

        # Agrupar por artista
        artists = {}
        for f in files:
            key = f["artist"]
            if key not in artists:
                artists[key] = {"albums": {}, "total_files": 0, "total_size": 0}
            album = f["album"]
            if album not in artists[key]["albums"]:
                artists[key]["albums"][album] = []
            artists[key]["albums"][album].append(f)
            artists[key]["total_files"] += 1
            artists[key]["total_size"] += f["size"]

        # Mostrar tabela
        table = Table(
            title="Musicas Disponiveis para Exportar",
            box=box.ROUNDED,
            title_style="bold magenta",
        )
        table.add_column("#", style="dim", width=4, justify="right")
        table.add_column("Artista", style="cyan bold")
        table.add_column("Albuns", justify="center", style="green")
        table.add_column("Faixas", justify="center")
        table.add_column("Tamanho", justify="right", style="yellow")

        artist_list = sorted(artists.items(), key=lambda x: x[1]["total_files"], reverse=True)
        for i, (name, info) in enumerate(artist_list, 1):
            size_mb = info["total_size"] / (1024 * 1024)
            table.add_row(
                str(i), name,
                str(len(info["albums"])),
                str(info["total_files"]),
                f"{size_mb:.1f} MB",
            )

        console.print()
        console.print(table)

        total_size = sum(a["total_size"] for a in artists.values())
        console.print(
            f"\n  [bold]Total:[/] {len(files)} arquivos, "
            f"{total_size / (1024 * 1024):.1f} MB"
        )

        # Selecionar artistas ou todos
        console.print("\n  Exportar quais artistas?")
        selected = self.selecionar_items(len(artist_list))
        if not selected:
            console.print("[yellow]Nenhum selecionado.[/]")
            return

        selected_artists = [artist_list[i - 1] for i in selected]
        selected_files = []
        for name, info in selected_artists:
            for album, tracks in info["albums"].items():
                selected_files.extend(tracks)

        # Pedir destino
        console.print(
            f"\n  [green]{len(selected_files)}[/] arquivos selecionados"
        )
        console.print("  Digite o caminho de destino (ex: /Volumes/USB/Musicas):")
        destino = input("  > ").strip()
        if not destino:
            console.print("[yellow]Cancelado.[/]")
            return

        destino = os.path.expanduser(destino)

        if not os.path.isdir(destino):
            console.print(f"  Criar pasta [cyan]{destino}[/]? ([cyan]s[/]/n)")
            if input("  > ").strip().lower() in ("s", "sim", "y", "yes"):
                try:
                    os.makedirs(destino, exist_ok=True)
                except OSError as e:
                    console.print(f"[red]Erro ao criar pasta: {e}[/]")
                    return
            else:
                console.print("[yellow]Cancelado.[/]")
                return

        # Mover arquivos (nao copiar, para economizar espaco)
        console.print(
            f"\n  [bold yellow]ATENCAO:[/] Os arquivos serao MOVIDOS "
            f"(nao copiados) para economizar espaco."
        )
        console.print(
            f"  Mover [green]{len(selected_files)}[/] arquivos para "
            f"[cyan]{destino}[/]? ([cyan]s[/]/n)"
        )
        if input("  > ").strip().lower() not in ("s", "sim", "y", "yes"):
            console.print("[yellow]Cancelado.[/]")
            return

        movidos = 0
        erros = 0
        hashes_seen = {}

        with Progress(
            SpinnerColumn("dots"),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=30),
            MofNCompleteColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(
                "[cyan]Exportando[/]", total=len(selected_files),
            )

            for f in selected_files:
                artist_dir = sanitize_filename(f["artist"])
                album_dir = sanitize_filename(f["album"])
                dest_dir = os.path.join(destino, artist_dir, album_dir)
                dest_path = os.path.join(dest_dir, f["filename"])

                progress.update(
                    task,
                    description=f"[cyan]{artist_dir}[/]/[green]{f['filename']}[/]",
                )

                # Verificar duplicata por hash
                try:
                    h = hashlib.sha256()
                    with open(f["path"], "rb") as fh:
                        while True:
                            chunk = fh.read(8192)
                            if not chunk:
                                break
                            h.update(chunk)
                    file_hash = h.hexdigest()
                except Exception:
                    file_hash = None

                if file_hash and file_hash in hashes_seen:
                    # Duplicata - remover da origem sem exportar
                    try:
                        os.remove(f["path"])
                    except OSError:
                        pass
                    progress.advance(task)
                    continue

                if file_hash:
                    hashes_seen[file_hash] = f["path"]

                # Se ja existe no destino com mesmo conteudo, so remover origem
                if os.path.exists(dest_path) and file_hash:
                    try:
                        eh = hashlib.sha256()
                        with open(dest_path, "rb") as fh:
                            while True:
                                chunk = fh.read(8192)
                                if not chunk:
                                    break
                                eh.update(chunk)
                        if eh.hexdigest() == file_hash:
                            os.remove(f["path"])
                            movidos += 1
                            progress.advance(task)
                            continue
                    except Exception:
                        pass

                try:
                    os.makedirs(dest_dir, exist_ok=True)
                    shutil.move(f["path"], dest_path)
                    movidos += 1
                except Exception as e:
                    erros += 1
                    console.print(f"    [red]x[/] {f['filename']}: [dim]{e}[/]")

                progress.advance(task)

        # Limpar pastas vazias na origem
        for root, dirs, fnames in os.walk(self.output_dir, topdown=False):
            for d in dirs:
                dirpath = os.path.join(root, d)
                try:
                    if not os.listdir(dirpath):
                        os.rmdir(dirpath)
                except OSError:
                    pass

        console.print()
        if erros == 0 and movidos > 0:
            console.print(Panel(
                f"[bold green]Exportacao completa![/]\n"
                f"{movidos} arquivos movidos para:\n"
                f"[dim]{os.path.abspath(destino)}[/]",
                border_style="green", padding=(0, 2),
            ))
        elif movidos > 0:
            console.print(Panel(
                f"[bold yellow]Exportacao parcial[/]\n"
                f"[green]{movidos}[/] movidos | [red]{erros}[/] erros\n"
                f"[dim]{os.path.abspath(destino)}[/]",
                border_style="yellow", padding=(0, 2),
            ))
        else:
            console.print(Panel(
                f"[bold red]Nenhum arquivo exportado[/] ({erros} erros)",
                border_style="red", padding=(0, 2),
            ))

    # --------------------------------------------------------
    # OPCAO 8: ORGANIZAR PASTA DE MUSICAS
    # --------------------------------------------------------

    def opcao_organizar(self):
        console.print(Panel(
            "[bold]Organizar pasta de musicas[/]\n"
            "[dim]Estrutura: Artista / Album / musicas[/]",
            border_style="cyan",
        ))

        console.print("  Origem (Enter = ./musicas):")
        origem = input("  > ").strip() or self.output_dir
        console.print("  Destino (Enter = ./final):")
        destino = input("  > ").strip() or "./final"

        organizar(origem=origem, destino=destino)

    # --------------------------------------------------------
    # OPCAO 9: RESUMO DA PASTA
    # --------------------------------------------------------

    def opcao_resumo(self):
        console.print(Panel(
            "[bold]Resumo da pasta de musicas[/]",
            border_style="cyan",
        ))

        console.print(f"  Pasta (Enter = {self.output_dir}):")
        pasta = input("  > ").strip() or self.output_dir
        pasta = os.path.expanduser(pasta)

        if not os.path.isdir(pasta):
            console.print(f"[red]Pasta '{pasta}' nao encontrada.[/]")
            return

        files = scan_audio_files(pasta)
        if not files:
            console.print(f"[yellow]Nenhum arquivo de audio em '{pasta}'[/]")
            return

        # Agrupar por artista
        artists = {}
        for f in files:
            key = f["artist"]
            if key not in artists:
                artists[key] = {"albums": set(), "tracks": 0, "size": 0}
            artists[key]["albums"].add(f["album"])
            artists[key]["tracks"] += 1
            artists[key]["size"] += f["size"]

        # Tabela principal
        table = Table(
            title=f"Resumo: {os.path.abspath(pasta)}",
            box=box.ROUNDED,
            title_style="bold magenta",
        )
        table.add_column("#", style="dim", width=4, justify="right")
        table.add_column("Artista", style="cyan bold")
        table.add_column("Albuns", justify="center", style="green")
        table.add_column("Musicas", justify="center")
        table.add_column("Tamanho", justify="right", style="yellow")

        sorted_artists = sorted(
            artists.items(), key=lambda x: x[1]["tracks"], reverse=True,
        )
        total_tracks = 0
        total_size = 0

        for i, (name, info) in enumerate(sorted_artists, 1):
            size_mb = info["size"] / (1024 * 1024)
            total_tracks += info["tracks"]
            total_size += info["size"]
            table.add_row(
                str(i), name,
                str(len(info["albums"])),
                str(info["tracks"]),
                f"{size_mb:.1f} MB",
            )

        console.print()
        console.print(table)

        # Totais
        if total_size >= 1024 * 1024 * 1024:
            size_str = f"{total_size / (1024**3):.2f} GB"
        else:
            size_str = f"{total_size / (1024**2):.1f} MB"

        console.print(Panel(
            f"[bold]{len(sorted_artists)}[/] artistas  |  "
            f"[bold]{total_tracks}[/] musicas  |  "
            f"[bold yellow]{size_str}[/]",
            border_style="blue", padding=(0, 2),
        ))
        console.print()

    # --------------------------------------------------------
    # OPCAO 10: CLASSIFICAR POR GENERO
    # --------------------------------------------------------

    def get_genre_map(self):
        """Carrega mapa de generos do Deezer (id -> nome). Cache no objeto."""
        if hasattr(self, "_genre_map") and self._genre_map:
            return self._genre_map
        try:
            resp = requests.get(f"{DEEZER_API}/genre", timeout=10)
            data = resp.json().get("data", [])
            self._genre_map = {g["id"]: g["name"] for g in data if g["id"] != 0}
            return self._genre_map
        except Exception:
            return {}

    def get_artist_genre(self, artist_name):
        """
        Busca o genero principal de um artista via Deezer.
        Pega os albuns do artista e retorna o genero mais frequente.
        Retorna (genre_name, deezer_artist_name) ou (None, None).
        """
        genre_map = self.get_genre_map()
        if not genre_map:
            return None, None

        # Buscar artista no Deezer
        info = self.buscar_artista(artist_name)
        if not info:
            return None, None

        # Buscar albuns (com genre_id)
        try:
            resp = requests.get(
                f"{DEEZER_API}/artist/{info['id']}/albums",
                params={"limit": 20},
                timeout=10,
            )
            albums = resp.json().get("data", [])
        except Exception:
            return None, info["name"]

        if not albums:
            return None, info["name"]

        # Contar generos pelos albuns
        genre_counter = Counter()
        for a in albums:
            gid = a.get("genre_id", 0)
            if gid and gid in genre_map:
                genre_counter[gid] += 1

        if not genre_counter:
            return None, info["name"]

        # Genero mais frequente
        top_gid = genre_counter.most_common(1)[0][0]
        return genre_map[top_gid], info["name"]

    def opcao_genero(self):
        console.print(Panel(
            "[bold]Classificar musicas por genero[/]\n"
            "[dim]Consulta Deezer para descobrir o genero de cada artista[/]",
            border_style="cyan",
        ))

        console.print(f"  Pasta para analisar (Enter = ./final):")
        pasta = input("  > ").strip() or "./final"
        pasta = os.path.expanduser(pasta)

        if not os.path.isdir(pasta):
            console.print(f"[red]Pasta '{pasta}' nao encontrada.[/]")
            return

        files = scan_audio_files(pasta)
        if not files:
            console.print(f"[yellow]Nenhum arquivo de audio em '{pasta}'[/]")
            return

        # Pegar artistas unicos
        artists_data = {}
        for f in files:
            key = f["artist"]
            if key not in artists_data:
                artists_data[key] = {"tracks": 0, "size": 0, "albums": set()}
            artists_data[key]["tracks"] += 1
            artists_data[key]["size"] += f["size"]
            artists_data[key]["albums"].add(f["album"])

        console.print(
            f"\n  Encontrados [green]{len(artists_data)}[/] artistas. "
            f"Consultando generos no Deezer...\n"
        )

        # Consultar genero de cada artista
        genre_results = {}  # artist_name -> genre
        with Progress(
            SpinnerColumn("dots"),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=30),
            MofNCompleteColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(
                "[cyan]Buscando generos[/]", total=len(artists_data),
            )

            for artist_name in artists_data:
                progress.update(
                    task, description=f"[cyan]{artist_name}[/]",
                )
                genre, deezer_name = self.get_artist_genre(artist_name)
                genre_results[artist_name] = genre or "Desconhecido"
                time.sleep(0.3)  # rate limit
                progress.advance(task)

        # Montar tabela
        table = Table(
            title=f"Classificacao por Genero",
            box=box.ROUNDED,
            title_style="bold magenta",
        )
        table.add_column("#", style="dim", width=4, justify="right")
        table.add_column("Artista", style="cyan bold")
        table.add_column("Genero", style="green bold")
        table.add_column("Albuns", justify="center")
        table.add_column("Musicas", justify="center")
        table.add_column("Tamanho", justify="right", style="yellow")

        sorted_artists = sorted(
            artists_data.items(), key=lambda x: x[1]["tracks"], reverse=True,
        )

        for i, (name, info) in enumerate(sorted_artists, 1):
            genre = genre_results.get(name, "Desconhecido")
            size_mb = info["size"] / (1024 * 1024)
            table.add_row(
                str(i), name, genre,
                str(len(info["albums"])),
                str(info["tracks"]),
                f"{size_mb:.1f} MB",
            )

        console.print()
        console.print(table)

        # Resumo por genero
        genre_summary = {}
        for name, info in artists_data.items():
            g = genre_results.get(name, "Desconhecido")
            if g not in genre_summary:
                genre_summary[g] = {"artists": 0, "tracks": 0, "size": 0}
            genre_summary[g]["artists"] += 1
            genre_summary[g]["tracks"] += info["tracks"]
            genre_summary[g]["size"] += info["size"]

        table2 = Table(
            title="Resumo por Genero",
            box=box.ROUNDED,
            title_style="bold",
        )
        table2.add_column("Genero", style="green bold")
        table2.add_column("Artistas", justify="center", style="cyan")
        table2.add_column("Musicas", justify="center")
        table2.add_column("Tamanho", justify="right", style="yellow")

        for g, info in sorted(genre_summary.items(), key=lambda x: x[1]["tracks"], reverse=True):
            size_mb = info["size"] / (1024 * 1024)
            table2.add_row(
                g, str(info["artists"]),
                str(info["tracks"]), f"{size_mb:.1f} MB",
            )

        console.print()
        console.print(table2)

        # Perguntar se quer reorganizar
        console.print(
            "\n  Deseja reorganizar em pastas por genero? "
            "([cyan]s[/]/n)"
        )
        console.print(
            "  [dim]Estrutura: genero/artista/album/musicas[/]"
        )
        resp = input("  > ").strip().lower()
        if resp not in ("s", "sim", "y", "yes"):
            console.print("[dim]OK, apenas exibindo.[/]")
            return

        # Pedir destino
        console.print(f"\n  Pasta destino (Enter = {pasta}):")
        destino = input("  > ").strip() or pasta
        destino = os.path.expanduser(destino)

        # Reorganizar
        movidos = 0
        erros = 0

        with Progress(
            SpinnerColumn("dots"),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=30),
            MofNCompleteColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(
                "[cyan]Reorganizando[/]", total=len(files),
            )

            for f in files:
                genre = genre_results.get(f["artist"], "Desconhecido")
                genre_dir = sanitize_filename(genre)
                artist_dir = sanitize_filename(f["artist"])
                album_dir = sanitize_filename(f["album"])
                dest_dir = os.path.join(destino, genre_dir, artist_dir, album_dir)
                dest_path = os.path.join(dest_dir, f["filename"])

                progress.update(
                    task,
                    description=f"[green]{genre_dir}[/]/[cyan]{artist_dir}[/]",
                )

                # Se ja esta no lugar certo, pular
                if os.path.abspath(f["path"]) == os.path.abspath(dest_path):
                    movidos += 1
                    progress.advance(task)
                    continue

                try:
                    os.makedirs(dest_dir, exist_ok=True)
                    shutil.move(f["path"], dest_path)
                    movidos += 1
                except Exception as e:
                    erros += 1
                    console.print(f"    [red]x[/] {f['filename']}: [dim]{e}[/]")

                progress.advance(task)

        # Limpar pastas vazias na origem
        if os.path.abspath(pasta) != os.path.abspath(destino):
            for root, dirs, fnames in os.walk(pasta, topdown=False):
                for d in dirs:
                    dirpath = os.path.join(root, d)
                    try:
                        if not os.listdir(dirpath):
                            os.rmdir(dirpath)
                    except OSError:
                        pass
        else:
            # Mesmo diretorio - limpar pastas vazias antigas
            for root, dirs, fnames in os.walk(destino, topdown=False):
                for d in dirs:
                    dirpath = os.path.join(root, d)
                    try:
                        if not os.listdir(dirpath):
                            os.rmdir(dirpath)
                    except OSError:
                        pass

        console.print()
        if erros == 0:
            console.print(Panel(
                f"[bold green]Reorganizacao completa![/]\n"
                f"{movidos} arquivos organizados por genero\n"
                f"[dim]{os.path.abspath(destino)}[/]",
                border_style="green", padding=(0, 2),
            ))
        else:
            console.print(Panel(
                f"[bold yellow]Reorganizacao parcial[/]\n"
                f"[green]{movidos}[/] movidos | [red]{erros}[/] erros\n"
                f"[dim]{os.path.abspath(destino)}[/]",
                border_style="yellow", padding=(0, 2),
            ))
        console.print()

    # --------------------------------------------------------
    # OPCAO 11: EXPORTAR ALEATORIO (SHUFFLE P/ CAIXA DE SOM)
    # --------------------------------------------------------

    def opcao_shuffle_export(self):
        console.print(Panel(
            "[bold]Shuffle - Exportar ou reorganizar em ordem aleatoria[/]\n"
            "[dim]Ideal para caixas de som sem funcao shuffle[/]",
            border_style="cyan",
        ))

        console.print("  Modo:")
        console.print("  [cyan][1][/] Exportar de uma pasta para outra (copiar/mover)")
        console.print(
            "  [cyan][2][/] Shuffle no local (renomear arquivos na propria pasta)"
        )
        console.print(
            "        [dim]Ideal para pendrive que ja tem as musicas[/]"
        )

        modo_op = input("\n  > ").strip()

        if modo_op == "2":
            self._shuffle_in_place()
        else:
            self._shuffle_export()

    def _shuffle_in_place(self, pasta_preenchida=None):
        """Renomeia arquivos na propria pasta com numeracao aleatoria (instantaneo)."""
        console.print(Panel(
            "[bold]Shuffle no local[/]\n"
            "[dim]Renomeia os arquivos com numero aleatorio (sem copiar dados)[/]",
            border_style="green",
        ))

        if pasta_preenchida:
            pasta = pasta_preenchida
        else:
            console.print("  Pasta com as musicas (ex: /Volumes/USB/Musicas):")
            pasta = input("  > ").strip()
            if not pasta:
                console.print("[yellow]Cancelado.[/]")
                return
            pasta = os.path.expanduser(pasta)

        if not os.path.isdir(pasta):
            console.print(f"[red]Pasta '{pasta}' nao encontrada.[/]")
            return

        # Buscar arquivos de audio na pasta (somente 1 nivel - pasta flat)
        AUDIO_EXT = {".mp3", ".flac", ".ogg", ".opus", ".m4a", ".wav", ".aac", ".wma"}
        files = []
        for fname in os.listdir(pasta):
            fpath = os.path.join(pasta, fname)
            if os.path.isfile(fpath):
                _, ext = os.path.splitext(fname)
                if ext.lower() in AUDIO_EXT:
                    files.append({"path": fpath, "filename": fname})

        if not files:
            # Tentar recursivo se nao achou nada flat
            files = scan_audio_files(pasta)
            if files:
                console.print(
                    f"  [yellow]Arquivos encontrados em subpastas.[/]"
                )
                console.print(
                    "  O shuffle no local funciona melhor em pasta flat "
                    "(sem subpastas)."
                )
                console.print(
                    "  Use a opcao 1 (exportar) para juntar tudo em uma pasta."
                )
                return
            console.print(f"[yellow]Nenhum arquivo de audio em '{pasta}'[/]")
            return

        total_size = sum(os.path.getsize(f["path"]) for f in files)
        if total_size >= 1024 * 1024 * 1024:
            size_str = f"{total_size / (1024**3):.2f} GB"
        else:
            size_str = f"{total_size / (1024**2):.1f} MB"

        console.print(
            f"\n  Encontradas [green]{len(files)}[/] musicas ({size_str})"
        )

        # Mostrar preview dos primeiros arquivos atuais
        console.print("\n  [dim]Ordem atual (primeiros 5):[/]")
        sorted_current = sorted(files, key=lambda x: x["filename"])
        for i, f in enumerate(sorted_current[:5], 1):
            console.print(f"    [dim]{i}.[/] {f['filename']}")
        if len(files) > 5:
            console.print(f"    [dim]... +{len(files) - 5} musicas[/]")

        # Confirmar
        console.print(
            f"\n  Embaralhar [green]{len(files)}[/] musicas em "
            f"[cyan]{pasta}[/]?"
        )
        console.print(
            "  [dim]Os arquivos serao renomeados com numeros aleatorios "
            "(instantaneo, sem copiar dados)[/]"
        )
        console.print("  ([cyan]s[/]/n)")
        if input("  > ").strip().lower() not in ("s", "sim", "y", "yes"):
            console.print("[yellow]Cancelado.[/]")
            return

        # Embaralhar
        random.shuffle(files)
        digitos = len(str(len(files)))

        # Fase 1: renomear para nomes temporarios (evitar conflito)
        temp_names = []
        for i, f in enumerate(files):
            temp_name = f"__shuffle_temp_{i}__" + os.path.splitext(f["filename"])[1]
            temp_path = os.path.join(pasta, temp_name)
            temp_names.append({"temp_path": temp_path, "original": f})

        ok_count = 0
        erros = 0

        with Progress(
            SpinnerColumn("dots"),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=30),
            MofNCompleteColumn(),
            console=console,
        ) as progress:
            # Fase 1: temp rename
            task1 = progress.add_task(
                "[cyan]Preparando[/]", total=len(files),
            )
            for t in temp_names:
                try:
                    os.rename(t["original"]["path"], t["temp_path"])
                except Exception as e:
                    erros += 1
                    console.print(
                        f"    [red]x[/] {t['original']['filename']}: [dim]{e}[/]"
                    )
                progress.advance(task1)

            # Fase 2: rename final com numero
            task2 = progress.add_task(
                "[cyan]Renomeando[/]", total=len(temp_names),
            )
            for i, t in enumerate(temp_names, 1):
                num_prefix = str(i).zfill(digitos)
                old_name = t["original"]["filename"]
                # Remover prefixo numerico existente
                clean_name = re.sub(r"^\d+[\.\-\s]+\s*", "", old_name)
                _, ext = os.path.splitext(clean_name)
                name_no_ext = os.path.splitext(clean_name)[0]

                new_name = f"{num_prefix}. {name_no_ext}{ext}"
                new_name = sanitize_filename(new_name)
                if not new_name.endswith(ext):
                    new_name = new_name + ext

                new_path = os.path.join(pasta, new_name)

                progress.update(
                    task2,
                    description=f"[cyan]{num_prefix}[/] {name_no_ext}",
                )

                try:
                    os.rename(t["temp_path"], new_path)
                    ok_count += 1
                except Exception as e:
                    erros += 1
                    # Tentar restaurar nome original
                    try:
                        os.rename(t["temp_path"], t["original"]["path"])
                    except Exception:
                        pass
                    console.print(
                        f"    [red]x[/] {old_name}: [dim]{e}[/]"
                    )

                progress.advance(task2)

        console.print()
        if erros == 0 and ok_count > 0:
            console.print(Panel(
                f"[bold green]Shuffle completo![/]\n"
                f"{ok_count} musicas renomeadas em ordem aleatoria\n"
                f"[dim]{os.path.abspath(pasta)}[/]",
                border_style="green", padding=(0, 2),
            ))
        elif ok_count > 0:
            console.print(Panel(
                f"[bold yellow]Shuffle parcial[/]\n"
                f"[green]{ok_count}[/] OK | [red]{erros}[/] erros\n"
                f"[dim]{os.path.abspath(pasta)}[/]",
                border_style="yellow", padding=(0, 2),
            ))
        else:
            console.print(Panel(
                f"[bold red]Nenhum arquivo renomeado[/] ({erros} erros)",
                border_style="red", padding=(0, 2),
            ))

        # Preview
        if ok_count > 0:
            console.print("  [dim]Nova ordem (primeiros 10):[/]")
            for i, t in enumerate(temp_names[:10], 1):
                num_prefix = str(i).zfill(digitos)
                old_name = t["original"]["filename"]
                clean = re.sub(r"^\d+[\.\-\s]+\s*", "", old_name)
                name_no_ext = os.path.splitext(clean)[0]
                console.print(
                    f"    [dim]{num_prefix}.[/] {name_no_ext}"
                )
            if len(files) > 10:
                console.print(f"    [dim]... +{len(files) - 10} musicas[/]")
            console.print()

        # Re-shuffle?
        if ok_count > 0:
            console.print("  Shufflar novamente? ([cyan]s[/]/n)")
            if input("  > ").strip().lower() in ("s", "sim", "y", "yes"):
                self._shuffle_in_place(pasta_preenchida=pasta)

    def _shuffle_export(self):
        """Exporta musicas embaralhadas de uma pasta para outra."""
        console.print(f"  Pasta origem (Enter = ./final):")
        origem = input("  > ").strip() or "./final"
        origem = os.path.expanduser(origem)

        if not os.path.isdir(origem):
            console.print(f"[red]Pasta '{origem}' nao encontrada.[/]")
            return

        files = scan_audio_files(origem)
        if not files:
            console.print(f"[yellow]Nenhum arquivo de audio em '{origem}'[/]")
            return

        total_size = sum(f["size"] for f in files)
        if total_size >= 1024 * 1024 * 1024:
            size_str = f"{total_size / (1024**3):.2f} GB"
        else:
            size_str = f"{total_size / (1024**2):.1f} MB"

        console.print(
            f"\n  Encontradas [green]{len(files)}[/] musicas ({size_str})"
        )

        # Escolher modo: copiar ou mover
        console.print("\n  Modo de exportacao:")
        console.print("  [cyan][1][/] Copiar (mantem os originais)")
        console.print("  [cyan][2][/] Mover (economiza espaco)")
        modo = input("  > ").strip()
        mover = modo == "2"

        if mover:
            console.print(
                "  [bold yellow]ATENCAO:[/] Os arquivos serao MOVIDOS da origem."
            )

        # Selecionar artistas ou todas
        artists_data = {}
        for f in files:
            key = f["artist"]
            if key not in artists_data:
                artists_data[key] = {"tracks": 0, "size": 0}
            artists_data[key]["tracks"] += 1
            artists_data[key]["size"] += f["size"]

        if len(artists_data) > 1:
            console.print(
                f"\n  [dim]{len(artists_data)} artistas encontrados.[/]"
            )
            console.print(
                "  Exportar todos ou selecionar? "
                "([cyan]todos[/] / [cyan]selecionar[/])"
            )
            sel_choice = input("  > ").strip().lower()

            if sel_choice in ("selecionar", "s", "sel"):
                table = Table(box=box.ROUNDED, title_style="bold")
                table.add_column("#", style="dim", width=4, justify="right")
                table.add_column("Artista", style="cyan bold")
                table.add_column("Musicas", justify="center")
                table.add_column("Tamanho", justify="right", style="yellow")

                sorted_a = sorted(
                    artists_data.items(),
                    key=lambda x: x[1]["tracks"], reverse=True,
                )
                for i, (name, info) in enumerate(sorted_a, 1):
                    mb = info["size"] / (1024 * 1024)
                    table.add_row(
                        str(i), name, str(info["tracks"]), f"{mb:.1f} MB",
                    )
                console.print()
                console.print(table)

                selected = self.selecionar_items(len(sorted_a))
                if not selected:
                    console.print("[yellow]Nenhum selecionado.[/]")
                    return

                selected_names = {sorted_a[i - 1][0] for i in selected}
                files = [f for f in files if f["artist"] in selected_names]

                total_size = sum(f["size"] for f in files)
                if total_size >= 1024 * 1024 * 1024:
                    size_str = f"{total_size / (1024**3):.2f} GB"
                else:
                    size_str = f"{total_size / (1024**2):.1f} MB"

                console.print(
                    f"\n  Selecionadas [green]{len(files)}[/] musicas ({size_str})"
                )

        # Destino
        console.print("\n  Pasta destino (ex: /Volumes/USB/Musicas):")
        destino = input("  > ").strip()
        if not destino:
            console.print("[yellow]Cancelado.[/]")
            return
        destino = os.path.expanduser(destino)

        if not os.path.isdir(destino):
            console.print(f"  Criar pasta [cyan]{destino}[/]? ([cyan]s[/]/n)")
            if input("  > ").strip().lower() in ("s", "sim", "y", "yes"):
                try:
                    os.makedirs(destino, exist_ok=True)
                except OSError as e:
                    console.print(f"[red]Erro ao criar pasta: {e}[/]")
                    return
            else:
                console.print("[yellow]Cancelado.[/]")
                return

        # Embaralhar
        random.shuffle(files)

        # Calcular digitos para o prefixo (001 vs 0001 etc)
        digitos = len(str(len(files)))

        console.print(
            f"\n  [green]{len(files)}[/] musicas embaralhadas"
        )
        console.print(
            f"  Formato: [cyan]{str(1).zfill(digitos)}. Artista - Musica.ext[/]"
        )
        acao = "Mover" if mover else "Copiar"
        console.print(
            f"\n  {acao} para [cyan]{destino}[/]? ([cyan]s[/]/n)"
        )
        if input("  > ").strip().lower() not in ("s", "sim", "y", "yes"):
            console.print("[yellow]Cancelado.[/]")
            return

        # Exportar
        ok_count = 0
        erros = 0

        with Progress(
            SpinnerColumn("dots"),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=30),
            MofNCompleteColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(
                f"[cyan]{acao}[/]", total=len(files),
            )

            for i, f in enumerate(files, 1):
                # Nome: 001. Artista - Musica.ext
                num_prefix = str(i).zfill(digitos)
                # Extrair nome limpo da musica (sem numero original)
                song_name = f["filename"]
                # Remover prefixo numerico existente (ex: "01. ", "1 - ")
                song_name = re.sub(r"^\d+[\.\-\s]+\s*", "", song_name)
                # Tirar extensao para reconstruir
                name_no_ext, ext = os.path.splitext(song_name)
                # Nome final: 001. Artista - Musica.mp3
                new_name = f"{num_prefix}. {f['artist']} - {name_no_ext}{ext}"
                # Sanitizar caracteres invalidos
                new_name = sanitize_filename(new_name)
                # Manter extensao original caso sanitize tenha removido o ponto
                if not new_name.endswith(ext):
                    new_name = new_name + ext

                dest_path = os.path.join(destino, new_name)

                progress.update(
                    task,
                    description=f"[cyan]{num_prefix}[/] {f['artist']} - {name_no_ext}",
                )

                try:
                    if mover:
                        shutil.move(f["path"], dest_path)
                    else:
                        shutil.copy2(f["path"], dest_path)
                    ok_count += 1
                except Exception as e:
                    erros += 1
                    console.print(
                        f"    [red]x[/] {f['filename']}: [dim]{e}[/]"
                    )

                progress.advance(task)

        # Limpar pastas vazias se moveu
        if mover:
            for root, dirs, fnames in os.walk(origem, topdown=False):
                for d in dirs:
                    dirpath = os.path.join(root, d)
                    try:
                        if not os.listdir(dirpath):
                            os.rmdir(dirpath)
                    except OSError:
                        pass

        console.print()
        if erros == 0 and ok_count > 0:
            console.print(Panel(
                f"[bold green]Exportacao shuffle completa![/]\n"
                f"{ok_count} musicas em ordem aleatoria\n"
                f"[dim]{os.path.abspath(destino)}[/]",
                border_style="green", padding=(0, 2),
            ))
        elif ok_count > 0:
            console.print(Panel(
                f"[bold yellow]Exportacao parcial[/]\n"
                f"[green]{ok_count}[/] OK | [red]{erros}[/] erros\n"
                f"[dim]{os.path.abspath(destino)}[/]",
                border_style="yellow", padding=(0, 2),
            ))
        else:
            console.print(Panel(
                f"[bold red]Nenhum arquivo exportado[/] ({erros} erros)",
                border_style="red", padding=(0, 2),
            ))

        # Mostrar preview das primeiras 10
        if ok_count > 0:
            console.print("  [dim]Primeiras 10 faixas na ordem:[/]")
            for i, f in enumerate(files[:10], 1):
                num_prefix = str(i).zfill(digitos)
                song = re.sub(r"^\d+[\.\-\s]+\s*", "", f["filename"])
                name_no_ext = os.path.splitext(song)[0]
                console.print(
                    f"    [dim]{num_prefix}.[/] [cyan]{f['artist']}[/] - {name_no_ext}"
                )
            if len(files) > 10:
                console.print(f"    [dim]... +{len(files) - 10} musicas[/]")
            console.print()

    # --------------------------------------------------------
    # OPCAO 12: BUSCA NA BIBLIOTECA LOCAL
    # --------------------------------------------------------

    def opcao_buscar_local(self):
        console.print(Panel(
            "[bold]Buscar na biblioteca local[/]\n"
            "[dim]Pesquisa por artista, musica ou album[/]",
            border_style="cyan",
        ))

        default_path = self.config.get("default_scan_path", "./final")
        console.print(f"  Pasta (Enter = {default_path}):")
        pasta = input("  > ").strip() or default_path
        pasta = os.path.expanduser(pasta)

        if not os.path.isdir(pasta):
            console.print(f"[red]Pasta '{pasta}' nao encontrada.[/]")
            return

        termo = input("  Buscar: ").strip()
        if not termo:
            console.print("[yellow]Cancelado.[/]")
            return

        files = scan_audio_files(pasta)
        if not files:
            console.print(f"[yellow]Nenhum arquivo de audio em '{pasta}'[/]")
            return

        # Buscar por termo (case insensitive)
        termo_lower = termo.lower()
        resultados = []
        for f in files:
            if (
                termo_lower in f["filename"].lower()
                or termo_lower in f["artist"].lower()
                or termo_lower in f["album"].lower()
            ):
                resultados.append(f)

        if not resultados:
            console.print(f"  [yellow]Nenhum resultado para '{termo}'[/]")
            return

        table = Table(
            title=f"Resultados para '{termo}'",
            box=box.ROUNDED,
            title_style="bold magenta",
        )
        table.add_column("#", style="dim", width=4, justify="right")
        table.add_column("Artista", style="cyan bold")
        table.add_column("Album", style="green")
        table.add_column("Musica", style="white")
        table.add_column("Tamanho", justify="right", style="yellow")

        for i, f in enumerate(resultados, 1):
            table.add_row(
                str(i), f["artist"], f["album"],
                f["filename"], format_size(f["size"]),
            )

        console.print()
        console.print(table)
        console.print(
            f"\n  [green]{len(resultados)}[/] resultado(s) encontrado(s)"
        )

    # --------------------------------------------------------
    # OPCAO 13: EDITOR DE TAGS / METADADOS
    # --------------------------------------------------------

    def opcao_editar_tags(self):
        console.print(Panel(
            "[bold]Editor de tags / metadados[/]\n"
            "[dim]Edita tags ID3 dos arquivos MP3 (artista, album, genero, capa)[/]",
            border_style="cyan",
        ))

        if not HAS_MUTAGEN:
            console.print("[red]Biblioteca 'mutagen' nao instalada.[/]")
            console.print("  Instale com: [cyan]pip install mutagen[/]")
            return

        default_path = self.config.get("default_scan_path", "./final")
        console.print(f"  Pasta (Enter = {default_path}):")
        pasta = input("  > ").strip() or default_path
        pasta = os.path.expanduser(pasta)

        if not os.path.isdir(pasta):
            console.print(f"[red]Pasta '{pasta}' nao encontrada.[/]")
            return

        files = scan_audio_files(pasta)
        if not files:
            console.print(f"[yellow]Nenhum arquivo de audio em '{pasta}'[/]")
            return

        # Filtrar MP3/FLAC (suportados por mutagen)
        supported = [
            f for f in files
            if f["filename"].lower().endswith((".mp3", ".flac"))
        ]
        if not supported:
            console.print("[yellow]Nenhum arquivo MP3/FLAC encontrado.[/]")
            return

        # Ler tags atuais
        table = Table(
            title="Tags Atuais",
            box=box.ROUNDED,
            title_style="bold magenta",
        )
        table.add_column("#", style="dim", width=4, justify="right")
        table.add_column("Arquivo", style="white", max_width=30)
        table.add_column("Artista (tag)", style="cyan")
        table.add_column("Album (tag)", style="green")
        table.add_column("Titulo (tag)", style="yellow")
        table.add_column("Genero", style="magenta")

        file_tags = []
        for i, f in enumerate(supported[:50], 1):
            tags = self._read_tags(f["path"])
            file_tags.append({"file": f, "tags": tags})
            table.add_row(
                str(i),
                f["filename"][:30],
                tags.get("artist", "") or "[dim]-[/]",
                tags.get("album", "") or "[dim]-[/]",
                tags.get("title", "") or "[dim]-[/]",
                tags.get("genre", "") or "[dim]-[/]",
            )

        console.print()
        console.print(table)
        if len(supported) > 50:
            console.print(
                f"  [dim]... mostrando 50 de {len(supported)} arquivos[/]"
            )

        console.print("\n  Opcoes:")
        console.print("  [cyan][1][/] Auto-preencher tags com dados do Deezer")
        console.print("  [cyan][2][/] Editar tags manualmente (arquivo individual)")
        console.print("  [cyan][3][/] Baixar e embutir capas de album")
        console.print("  [cyan][0][/] Voltar")

        choice = input("\n  > ").strip()

        if choice == "1":
            self._auto_fill_tags(supported)
        elif choice == "2":
            self._manual_edit_tags(file_tags)
        elif choice == "3":
            self._download_covers(supported)

    def _read_tags(self, filepath):
        """Le tags de um arquivo de audio via mutagen."""
        tags = {}
        try:
            if filepath.lower().endswith(".mp3"):
                audio = MP3(filepath)
                id3 = audio.tags
                if id3:
                    tags["artist"] = str(id3.get("TPE1", ""))
                    tags["album"] = str(id3.get("TALB", ""))
                    tags["title"] = str(id3.get("TIT2", ""))
                    tags["genre"] = str(id3.get("TCON", ""))
                    tags["year"] = str(id3.get("TDRC", ""))
                tags["bitrate"] = audio.info.bitrate // 1000 if audio.info.bitrate else 0
                tags["duration"] = audio.info.length
            elif filepath.lower().endswith(".flac"):
                audio = FLAC(filepath)
                if audio.tags:
                    tags["artist"] = audio.tags.get("artist", [""])[0]
                    tags["album"] = audio.tags.get("album", [""])[0]
                    tags["title"] = audio.tags.get("title", [""])[0]
                    tags["genre"] = audio.tags.get("genre", [""])[0]
                tags["duration"] = audio.info.length
        except Exception:
            pass
        return tags

    def _auto_fill_tags(self, files):
        """Preenche tags automaticamente com dados da estrutura de pastas + Deezer."""
        console.print("\n  [dim]Preenchendo tags com dados do Deezer...[/]\n")

        updated = 0
        genre_cache = {}  # artista -> genero

        with Progress(
            SpinnerColumn("dots"),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=30),
            MofNCompleteColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(
                "[cyan]Atualizando tags[/]", total=len(files),
            )

            for f in files:
                progress.update(
                    task,
                    description=f"[cyan]{f['filename'][:40]}[/]",
                )

                try:
                    if f["filename"].lower().endswith(".mp3"):
                        try:
                            id3 = ID3(f["path"])
                        except ID3NoHeaderError:
                            id3 = ID3()

                        artist = f.get("artist", "")
                        album = f.get("album", "")
                        title = os.path.splitext(f["filename"])[0]
                        title = re.sub(r"^\d+[\.\-\s]+\s*", "", title)

                        if artist and artist != "Desconhecido":
                            id3["TPE1"] = TPE1(encoding=3, text=[artist])
                        if album and album != "Singles":
                            id3["TALB"] = TALB(encoding=3, text=[album])
                        if title:
                            id3["TIT2"] = TIT2(encoding=3, text=[title])

                        # Genero via Deezer (com cache por artista)
                        if artist and artist != "Desconhecido":
                            if artist not in genre_cache:
                                genre, _ = self.get_artist_genre(artist)
                                genre_cache[artist] = genre
                            genre = genre_cache[artist]
                            if genre:
                                id3["TCON"] = TCON(encoding=3, text=[genre])

                        id3.save(f["path"])
                        updated += 1

                except Exception as e:
                    console.print(
                        f"    [red]x[/] {f['filename']}: [dim]{e}[/]"
                    )

                progress.advance(task)

        console.print(Panel(
            f"[bold green]{updated}[/] arquivos atualizados",
            border_style="green", padding=(0, 2),
        ))

    def _manual_edit_tags(self, file_tags):
        """Edita tags de um arquivo individual."""
        if not file_tags:
            return

        console.print("\n  Numero do arquivo para editar:")
        try:
            idx = int(input("  > ").strip()) - 1
            if idx < 0 or idx >= len(file_tags):
                console.print("[red]Indice invalido.[/]")
                return
        except ValueError:
            console.print("[red]Numero invalido.[/]")
            return

        ft = file_tags[idx]
        f = ft["file"]
        tags = ft["tags"]

        console.print(f"\n  Editando: [cyan]{f['filename']}[/]")
        console.print(f"  Artista atual: [dim]{tags.get('artist', '-')}[/]")
        console.print(f"  Album atual:   [dim]{tags.get('album', '-')}[/]")
        console.print(f"  Titulo atual:  [dim]{tags.get('title', '-')}[/]")
        console.print(f"  Genero atual:  [dim]{tags.get('genre', '-')}[/]")

        console.print("\n  [dim]Enter para manter o valor atual[/]")
        new_artist = input("  Artista: ").strip() or tags.get("artist", "")
        new_album = input("  Album: ").strip() or tags.get("album", "")
        new_title = input("  Titulo: ").strip() or tags.get("title", "")
        new_genre = input("  Genero: ").strip() or tags.get("genre", "")

        try:
            if f["path"].lower().endswith(".mp3"):
                try:
                    id3 = ID3(f["path"])
                except ID3NoHeaderError:
                    id3 = ID3()

                id3["TPE1"] = TPE1(encoding=3, text=[new_artist])
                id3["TALB"] = TALB(encoding=3, text=[new_album])
                id3["TIT2"] = TIT2(encoding=3, text=[new_title])
                id3["TCON"] = TCON(encoding=3, text=[new_genre])
                id3.save(f["path"])
                console.print("  [green]Tags atualizadas![/]")
            else:
                console.print(
                    "  [yellow]Edicao manual so suporta MP3 por enquanto.[/]"
                )
        except Exception as e:
            console.print(f"  [red]Erro: {e}[/]")

    def _download_covers(self, files):
        """Baixa e embute capas de album nos arquivos MP3."""
        console.print("\n  [dim]Buscando capas de album no Deezer...[/]\n")

        # Agrupar por artista/album
        albums_map = {}
        for f in files:
            key = (f["artist"], f["album"])
            if key not in albums_map:
                albums_map[key] = []
            albums_map[key].append(f)

        updated = 0

        with Progress(
            SpinnerColumn("dots"),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=30),
            MofNCompleteColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(
                "[cyan]Capas[/]", total=len(albums_map),
            )

            for (artist, album), album_files in albums_map.items():
                progress.update(
                    task,
                    description=f"[cyan]{artist}[/] - [green]{album}[/]",
                )

                cover_data = self._fetch_cover(artist, album)
                if cover_data:
                    for af in album_files:
                        try:
                            if af["path"].lower().endswith(".mp3"):
                                try:
                                    id3 = ID3(af["path"])
                                except ID3NoHeaderError:
                                    id3 = ID3()

                                id3.delall("APIC")
                                id3["APIC"] = APIC(
                                    encoding=3,
                                    mime="image/jpeg",
                                    type=3,
                                    desc="Cover",
                                    data=cover_data,
                                )
                                id3.save(af["path"])
                                updated += 1
                        except Exception:
                            pass

                    # Salvar cover.jpg na pasta do album
                    if album_files:
                        cover_dir = os.path.dirname(album_files[0]["path"])
                        cover_path = os.path.join(cover_dir, "cover.jpg")
                        if not os.path.exists(cover_path):
                            try:
                                with open(cover_path, "wb") as cf:
                                    cf.write(cover_data)
                            except Exception:
                                pass

                progress.advance(task)
                self.rate_limiter.wait()

        console.print(Panel(
            f"[bold green]{updated}[/] arquivos com capa atualizada",
            border_style="green", padding=(0, 2),
        ))

    def _fetch_cover(self, artist_name, album_name):
        """Busca capa do album no Deezer. Retorna bytes da imagem ou None."""
        try:
            resp = requests.get(
                f"{DEEZER_API}/search/album",
                params={"q": f"{artist_name} {album_name}", "limit": 1},
                timeout=10,
            )
            albums = resp.json().get("data", [])
            if not albums:
                return None

            cover_url = (
                albums[0].get("cover_big")
                or albums[0].get("cover_medium")
            )
            if not cover_url:
                return None

            img_resp = requests.get(cover_url, timeout=10)
            if img_resp.status_code == 200:
                return img_resp.content
        except Exception:
            pass
        return None

    # --------------------------------------------------------
    # OPCAO 14: GERADOR DE PLAYLISTS M3U
    # --------------------------------------------------------

    def opcao_gerar_playlist(self):
        console.print(Panel(
            "[bold]Gerador de playlists M3U[/]\n"
            "[dim]Cria arquivos .m3u para players de musica[/]",
            border_style="cyan",
        ))

        default_path = self.config.get("default_scan_path", "./final")
        console.print(f"  Pasta com musicas (Enter = {default_path}):")
        pasta = input("  > ").strip() or default_path
        pasta = os.path.expanduser(pasta)

        if not os.path.isdir(pasta):
            console.print(f"[red]Pasta '{pasta}' nao encontrada.[/]")
            return

        files = scan_audio_files(pasta)
        if not files:
            console.print(f"[yellow]Nenhum arquivo de audio em '{pasta}'[/]")
            return

        console.print(f"\n  [green]{len(files)}[/] musicas encontradas")

        console.print("\n  Tipo de playlist:")
        console.print("  [cyan][1][/] Todas as musicas")
        console.print("  [cyan][2][/] Por artista (uma playlist por artista)")
        console.print("  [cyan][3][/] Por album")
        console.print("  [cyan][4][/] Shuffle (ordem aleatoria)")

        choice = input("\n  > ").strip()

        if choice == "1":
            sorted_files = sorted(
                files,
                key=lambda f: (f["artist"], f["album"], f["filename"]),
            )
            name = input("  Nome (Enter = todas): ").strip() or "todas"
            self._write_m3u(sorted_files, pasta, f"{name}.m3u")
            console.print(f"  [green]Playlist '{name}.m3u' criada![/]")

        elif choice == "2":
            artists = {}
            for f in files:
                key = f["artist"]
                if key not in artists:
                    artists[key] = []
                artists[key].append(f)

            for artist_name, artist_files in sorted(artists.items()):
                sorted_a = sorted(
                    artist_files,
                    key=lambda f: (f["album"], f["filename"]),
                )
                safe_name = sanitize_filename(artist_name)
                self._write_m3u(sorted_a, pasta, f"{safe_name}.m3u")

            console.print(
                f"  [green]{len(artists)} playlists criadas "
                f"(uma por artista)[/]"
            )

        elif choice == "3":
            albums = {}
            for f in files:
                key = (f["artist"], f["album"])
                if key not in albums:
                    albums[key] = []
                albums[key].append(f)

            for (artist_name, album_name), album_files in sorted(
                albums.items()
            ):
                sorted_a = sorted(
                    album_files, key=lambda f: f["filename"],
                )
                safe_name = sanitize_filename(
                    f"{artist_name} - {album_name}"
                )
                self._write_m3u(sorted_a, pasta, f"{safe_name}.m3u")

            console.print(
                f"  [green]{len(albums)} playlists criadas "
                f"(uma por album)[/]"
            )

        elif choice == "4":
            random.shuffle(files)
            name = input("  Nome (Enter = shuffle): ").strip() or "shuffle"
            self._write_m3u(files, pasta, f"{name}.m3u")
            console.print(
                f"  [green]Playlist shuffle '{name}.m3u' criada![/]"
            )

    def _write_m3u(self, files, base_dir, filename):
        """Escreve arquivo M3U."""
        m3u_path = os.path.join(base_dir, filename)
        with open(m3u_path, "w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")
            for af in files:
                rel_path = os.path.relpath(af["path"], base_dir)
                duration = -1
                if HAS_MUTAGEN:
                    try:
                        tags = self._read_tags(af["path"])
                        if tags.get("duration"):
                            duration = int(tags["duration"])
                    except Exception:
                        pass
                artist = af.get("artist", "")
                title = os.path.splitext(af["filename"])[0]
                title = re.sub(r"^\d+[\.\-\s]+\s*", "", title)
                f.write(f"#EXTINF:{duration},{artist} - {title}\n")
                f.write(f"{rel_path}\n")

    # --------------------------------------------------------
    # OPCAO 15: VERIFICADOR DE QUALIDADE
    # --------------------------------------------------------

    def opcao_verificar_qualidade(self):
        console.print(Panel(
            "[bold]Verificador de qualidade de audio[/]\n"
            "[dim]Analisa bitrate, duracao e integridade[/]",
            border_style="cyan",
        ))

        if not HAS_MUTAGEN:
            console.print("[red]Biblioteca 'mutagen' nao instalada.[/]")
            console.print("  Instale com: [cyan]pip install mutagen[/]")
            return

        default_path = self.config.get("default_scan_path", "./final")
        console.print(f"  Pasta (Enter = {default_path}):")
        pasta = input("  > ").strip() or default_path
        pasta = os.path.expanduser(pasta)

        if not os.path.isdir(pasta):
            console.print(f"[red]Pasta '{pasta}' nao encontrada.[/]")
            return

        files = scan_audio_files(pasta)
        if not files:
            console.print(f"[yellow]Nenhum arquivo de audio em '{pasta}'[/]")
            return

        console.print(f"\n  Analisando [green]{len(files)}[/] arquivos...\n")

        results = []
        problemas = 0
        min_kbps = self.config.get("qualidade_minima_kbps", 192)

        with Progress(
            SpinnerColumn("dots"),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=30),
            MofNCompleteColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(
                "[cyan]Analisando[/]", total=len(files),
            )

            for f in files:
                progress.update(
                    task,
                    description=f"[cyan]{f['filename'][:40]}[/]",
                )

                info = {
                    "file": f,
                    "bitrate": 0,
                    "duration": 0,
                    "valid": True,
                    "issue": "",
                }

                try:
                    tags = self._read_tags(f["path"])
                    info["bitrate"] = tags.get("bitrate", 0)
                    info["duration"] = tags.get("duration", 0)

                    if info["bitrate"] and info["bitrate"] < min_kbps:
                        info["issue"] = (
                            f"Baixa qualidade ({info['bitrate']}kbps)"
                        )
                        problemas += 1
                    elif info["duration"] and info["duration"] < 30:
                        info["issue"] = (
                            f"Muito curto ({format_duration(info['duration'])})"
                        )
                        problemas += 1
                    elif f["size"] < 500 * 1024:
                        info["issue"] = "Arquivo muito pequeno"
                        problemas += 1
                except Exception as e:
                    info["valid"] = False
                    info["issue"] = f"Erro: {str(e)[:50]}"
                    problemas += 1

                results.append(info)
                progress.advance(task)

        # Tabela
        table = Table(
            title="Analise de Qualidade",
            box=box.ROUNDED,
            title_style="bold magenta",
        )
        table.add_column("#", style="dim", width=4, justify="right")
        table.add_column("Arquivo", style="white", max_width=35)
        table.add_column("Artista", style="cyan", max_width=20)
        table.add_column("Bitrate", justify="center")
        table.add_column("Duracao", justify="center", style="dim")
        table.add_column("Tamanho", justify="right", style="yellow")
        table.add_column("Status", justify="center")

        problem_results = [r for r in results if r["issue"]]
        ok_results = [r for r in results if not r["issue"]]

        for i, r in enumerate(problem_results, 1):
            f = r["file"]
            br = f"{r['bitrate']}k" if r["bitrate"] else "-"
            dur = format_duration(r["duration"]) if r["duration"] else "-"
            table.add_row(
                str(i), f["filename"][:35], f["artist"][:20],
                br, dur, format_size(f["size"]),
                f"[red]{r['issue']}[/]",
            )

        if problem_results and ok_results:
            table.add_section()

        for i, r in enumerate(
            ok_results[:20], len(problem_results) + 1,
        ):
            f = r["file"]
            br = f"{r['bitrate']}k" if r["bitrate"] else "-"
            dur = format_duration(r["duration"]) if r["duration"] else "-"
            table.add_row(
                str(i), f["filename"][:35], f["artist"][:20],
                br, dur, format_size(f["size"]),
                "[green]OK[/]",
            )

        if len(ok_results) > 20:
            console.print(
                f"  [dim]... +{len(ok_results) - 20} arquivos OK[/]"
            )

        console.print()
        console.print(table)

        total_size = sum(f["size"] for f in files)
        br_count = sum(1 for r in results if r["bitrate"])
        avg_bitrate = 0
        if br_count:
            avg_bitrate = sum(r["bitrate"] for r in results) / br_count

        console.print(Panel(
            f"[bold]{len(files)}[/] arquivos  |  "
            f"[bold yellow]{format_size(total_size)}[/]\n"
            f"Bitrate medio: [cyan]{avg_bitrate:.0f} kbps[/]  |  "
            f"Problemas: "
            f"[{'red' if problemas else 'green'}]{problemas}[/]",
            border_style="blue", padding=(0, 2),
        ))

    # --------------------------------------------------------
    # OPCAO 16: BUSCAR LETRAS DE MUSICAS
    # --------------------------------------------------------

    def opcao_letras(self):
        console.print(Panel(
            "[bold]Buscar letras de musicas[/]\n"
            "[dim]Busca online (Vagalume / lyrics.ovh)[/]",
            border_style="cyan",
        ))

        artista = input("  Artista: ").strip()
        if not artista:
            console.print("[yellow]Cancelado.[/]")
            return

        musica = input("  Musica: ").strip()
        if not musica:
            console.print("[yellow]Cancelado.[/]")
            return

        console.print(
            f"\n  [dim]Buscando letra de '{artista} - {musica}'...[/]"
        )

        letra = self._buscar_letra_vagalume(artista, musica)
        if not letra:
            letra = self._buscar_letra_lyricsovh(artista, musica)

        if not letra:
            console.print("[red]Letra nao encontrada.[/]")
            return

        console.print()
        console.print(Panel(
            f"[bold cyan]{artista}[/] - [bold green]{musica}[/]\n\n"
            f"{letra}",
            border_style="blue",
            padding=(1, 2),
        ))

        console.print("\n  Salvar como .txt? ([cyan]s[/]/n)")
        if input("  > ").strip().lower() in ("s", "sim", "y", "yes"):
            safe_name = sanitize_filename(f"{artista} - {musica}")
            txt_path = f"{safe_name}.txt"
            with open(txt_path, "w", encoding="utf-8") as f:
                f.write(f"{artista} - {musica}\n")
                f.write("=" * 40 + "\n\n")
                f.write(letra)
            console.print(f"  [green]Salvo em '{txt_path}'[/]")

    def _buscar_letra_vagalume(self, artista, musica):
        """Busca letra no Vagalume (API brasileira)."""
        try:
            resp = requests.get(
                "https://api.vagalume.com.br/search.php",
                params={"art": artista, "mus": musica},
                timeout=10,
            )
            data = resp.json()
            mus = data.get("mus", [])
            if mus:
                return mus[0].get("text", "")
        except Exception:
            pass
        return None

    def _buscar_letra_lyricsovh(self, artista, musica):
        """Busca letra no lyrics.ovh (fallback internacional)."""
        try:
            from urllib.parse import quote
            resp = requests.get(
                f"https://api.lyrics.ovh/v1/{quote(artista)}/{quote(musica)}",
                timeout=10,
            )
            if resp.status_code == 200:
                return resp.json().get("lyrics", "")
        except Exception:
            pass
        return None

    # --------------------------------------------------------
    # OPCAO 17: HISTORICO E ESTATISTICAS
    # --------------------------------------------------------

    def opcao_historico(self):
        console.print(Panel(
            "[bold]Historico e estatisticas[/]\n"
            "[dim]Mostra historico de downloads e uso[/]",
            border_style="cyan",
        ))

        historico = load_historico()
        downloads = historico.get("downloads", [])
        stats = historico.get("stats", {})

        if not downloads:
            console.print(
                "  [yellow]Nenhum download registrado ainda.[/]"
            )
            console.print(
                "  [dim]O historico comeca a ser registrado "
                "a partir de agora.[/]"
            )
            return

        total = stats.get("total_downloads", 0)
        total_bytes = stats.get("total_bytes", 0)

        # Downloads por artista
        artist_counter = Counter()
        for d in downloads:
            if d.get("status") == "ok":
                artist_counter[d.get("artist", "Desconhecido")] += 1

        # Downloads por mes
        month_counter = Counter()
        for d in downloads:
            ts = d.get("timestamp", "")
            if ts:
                month_counter[ts[:7]] += 1

        # Tabela artistas mais baixados
        table = Table(
            title="Artistas Mais Baixados",
            box=box.ROUNDED,
            title_style="bold magenta",
        )
        table.add_column("#", style="dim", width=4, justify="right")
        table.add_column("Artista", style="cyan bold")
        table.add_column("Downloads", justify="center", style="green")

        for i, (name, count) in enumerate(
            artist_counter.most_common(15), 1,
        ):
            table.add_row(str(i), name, str(count))

        console.print()
        console.print(table)

        # Downloads recentes
        table2 = Table(
            title="Downloads Recentes",
            box=box.ROUNDED,
            title_style="bold",
        )
        table2.add_column("Data", style="dim", width=16)
        table2.add_column("Artista", style="cyan")
        table2.add_column("Musica", style="green")
        table2.add_column("Status", justify="center")

        for d in reversed(downloads[-15:]):
            ts = d.get("timestamp", "")[:16].replace("T", " ")
            status = (
                "[green]OK[/]"
                if d.get("status") == "ok"
                else "[red]FALHA[/]"
            )
            table2.add_row(
                ts, d.get("artist", ""),
                d.get("title", ""), status,
            )

        console.print()
        console.print(table2)

        console.print(Panel(
            f"[bold]{total}[/] downloads totais  |  "
            f"[bold yellow]{format_size(total_bytes)}[/]\n"
            f"[bold]{len(artist_counter)}[/] artistas unicos  |  "
            f"[bold]{len(month_counter)}[/] meses de uso",
            border_style="blue", padding=(0, 2),
        ))

        console.print("\n  [dim][L] Limpar historico | [Enter] Voltar[/]")
        if input("  > ").strip().lower() == "l":
            save_historico({
                "downloads": [],
                "stats": {"total_downloads": 0, "total_bytes": 0},
            })
            console.print("  [green]Historico limpo.[/]")

    # --------------------------------------------------------
    # OPCAO 18: BAIXAR POR LINK DIRETO
    # --------------------------------------------------------

    def opcao_baixar_link(self):
        console.print(Panel(
            "[bold]Baixar por link direto[/]\n"
            "[dim]Cole uma URL do Deezer ou YouTube[/]",
            border_style="cyan",
        ))

        url = input("  URL: ").strip()
        if not url:
            console.print("[yellow]Cancelado.[/]")
            return

        if "deezer.com" in url and "/album/" in url:
            self._baixar_deezer_album(url)
        elif "deezer.com" in url and "/track/" in url:
            self._baixar_deezer_track(url)
        elif "youtube.com" in url or "youtu.be" in url:
            self._baixar_youtube(url)
        elif "spotify.com" in url:
            console.print(
                "[yellow]Links do Spotify nao sao suportados "
                "diretamente.[/]"
            )
            console.print(
                "  [dim]Dica: copie o nome do album e use a opcao 5 "
                "para buscar no Deezer.[/]"
            )
        else:
            console.print("[yellow]URL nao reconhecida.[/]")
            console.print(
                "  [dim]Suportados: Deezer (album/track), YouTube[/]"
            )

    def _baixar_deezer_album(self, url):
        """Baixa album completo pelo link do Deezer."""
        match = re.search(r"/album/(\d+)", url)
        if not match:
            console.print("[red]URL de album invalida.[/]")
            return

        album_id = match.group(1)
        console.print(
            f"  [dim]Buscando album {album_id} no Deezer...[/]"
        )

        try:
            resp = requests.get(
                f"{DEEZER_API}/album/{album_id}", timeout=10,
            )
            data = resp.json()
        except Exception as e:
            console.print(f"[red]Erro: {e}[/]")
            return

        artist_name = data.get("artist", {}).get("name", "Desconhecido")
        album_name = data.get("title", "Album")
        nb_tracks = data.get("nb_tracks", 0)

        console.print(
            f"\n  [cyan]{artist_name}[/] - [green]{album_name}[/] "
            f"({nb_tracks} faixas)"
        )
        console.print("  Baixar? ([cyan]s[/]/n)")
        if input("  > ").strip().lower() not in ("s", "sim", "y", "yes"):
            console.print("[yellow]Cancelado.[/]")
            return

        ok, fail = self.download_album_by_deezer_url(
            url, artist_name, album_name,
        )
        self._show_download_result(ok, fail)

    def _baixar_deezer_track(self, url):
        """Baixa uma faixa pelo link do Deezer."""
        match = re.search(r"/track/(\d+)", url)
        if not match:
            console.print("[red]URL de track invalida.[/]")
            return

        track_id = match.group(1)
        console.print(
            f"  [dim]Buscando track {track_id} no Deezer...[/]"
        )

        try:
            resp = requests.get(
                f"{DEEZER_API}/track/{track_id}", timeout=10,
            )
            data = resp.json()
        except Exception as e:
            console.print(f"[red]Erro: {e}[/]")
            return

        artist_name = data.get("artist", {}).get("name", "Desconhecido")
        title = data.get("title", "?")
        album_name = data.get("album", {}).get("title", "Singles")

        console.print(
            f"\n  [cyan]{artist_name}[/] - [green]{title}[/] "
            f"(album: {album_name})"
        )
        console.print("  Baixar? ([cyan]s[/]/n)")
        if input("  > ").strip().lower() not in ("s", "sim", "y", "yes"):
            console.print("[yellow]Cancelado.[/]")
            return

        dl_tracks = [{"title": title, "number": 1, "album": album_name}]
        ok, fail = self.download_tracks_with_progress(
            dl_tracks, artist_name,
        )
        self._show_download_result(ok, fail)

    def _baixar_youtube(self, url):
        """Baixa audio de um video/playlist do YouTube."""
        console.print(f"\n  [dim]Baixando via yt-dlp...[/]")
        console.print("  Baixar como:")
        console.print("  [cyan][1][/] Musica unica (extrair audio)")
        console.print("  [cyan][2][/] Playlist completa")

        choice = input("  > ").strip()

        pasta_dest = os.path.join(self.output_dir, "YouTube")
        os.makedirs(pasta_dest, exist_ok=True)

        output_template = os.path.join(
            pasta_dest, "%(title)s.%(ext)s",
        )
        cmd = [
            YTDLP_PATH,
            "-x", "--audio-format", self.formato,
            "--audio-quality", "0",
            "-o", output_template,
            "--progress", "--newline",
        ]
        if choice != "2":
            cmd.append("--no-playlist")
        cmd.append(url)

        console.print(f"\n  Salvando em [cyan]{pasta_dest}[/]\n")

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            for line in proc.stdout:
                line = line.strip()
                if "[download]" in line:
                    console.print(f"  [dim]{line}[/]")
                elif "[ExtractAudio]" in line:
                    console.print(f"  [green]{line}[/]")

            proc.wait(timeout=600)

            if proc.returncode == 0:
                console.print(Panel(
                    f"[bold green]Download completo![/]\n"
                    f"[dim]{pasta_dest}[/]",
                    border_style="green", padding=(0, 2),
                ))
            else:
                console.print(
                    f"[red]yt-dlp retornou codigo "
                    f"{proc.returncode}[/]"
                )
        except subprocess.TimeoutExpired:
            proc.kill()
            console.print("[red]Timeout (10 min)[/]")
        except Exception as e:
            console.print(f"[red]Erro: {e}[/]")

    # --------------------------------------------------------
    # OPCAO 19: SINCRONIZAR COM PENDRIVE
    # --------------------------------------------------------

    def opcao_sincronizar(self):
        console.print(Panel(
            "[bold]Sincronizar com pendrive / pasta externa[/]\n"
            "[dim]Compara e copia apenas as diferencas[/]",
            border_style="cyan",
        ))

        default_path = self.config.get("default_scan_path", "./final")
        console.print(
            f"  Pasta origem (local) (Enter = {default_path}):"
        )
        origem = input("  > ").strip() or default_path
        origem = os.path.expanduser(origem)

        if not os.path.isdir(origem):
            console.print(f"[red]Pasta '{origem}' nao encontrada.[/]")
            return

        default_export = self.config.get("default_export_path", "")
        hint = f" (Enter = {default_export})" if default_export else ""
        console.print(
            f"  Pasta destino (pendrive){hint}:"
        )
        destino = input("  > ").strip() or default_export
        if not destino:
            console.print("[yellow]Cancelado.[/]")
            return
        destino = os.path.expanduser(destino)

        if not os.path.isdir(destino):
            console.print(
                f"  Criar pasta [cyan]{destino}[/]? ([cyan]s[/]/n)"
            )
            if input("  > ").strip().lower() in (
                "s", "sim", "y", "yes",
            ):
                try:
                    os.makedirs(destino, exist_ok=True)
                except OSError as e:
                    console.print(f"[red]Erro: {e}[/]")
                    return
            else:
                console.print("[yellow]Cancelado.[/]")
                return

        console.print("\n  [dim]Analisando pastas...[/]")

        files_origem = scan_audio_files(origem)
        files_destino = scan_audio_files(destino)

        # Comparar por caminho relativo
        set_origem = {}
        for f in files_origem:
            rel = os.path.relpath(f["path"], origem)
            set_origem[rel] = f

        set_destino = {}
        for f in files_destino:
            rel = os.path.relpath(f["path"], destino)
            set_destino[rel] = f

        novos = [r for r in set_origem if r not in set_destino]
        removidos = [r for r in set_destino if r not in set_origem]
        comuns = [r for r in set_origem if r in set_destino]

        novos_size = sum(set_origem[r]["size"] for r in novos)

        console.print(
            f"\n  Origem:  [green]{len(files_origem)}[/] musicas"
        )
        console.print(
            f"  Destino: [green]{len(files_destino)}[/] musicas"
        )
        console.print()
        console.print(
            f"  [green]+{len(novos)}[/] novas ({format_size(novos_size)})"
        )
        console.print(
            f"  [red]-{len(removidos)}[/] removidas da origem"
        )
        console.print(f"  [dim]={len(comuns)}[/] em comum")

        if not novos and not removidos:
            console.print(Panel(
                "[bold green]Tudo sincronizado![/]",
                border_style="green", padding=(0, 2),
            ))
            return

        console.print("\n  Acoes:")
        if novos:
            console.print(
                f"  [cyan][1][/] Copiar {len(novos)} novas "
                f"para destino"
            )
        if removidos:
            console.print(
                f"  [cyan][2][/] Remover {len(removidos)} extras "
                f"do destino"
            )
        if novos and removidos:
            console.print(
                f"  [cyan][3][/] Ambos (sincronizar completo)"
            )
        console.print("  [cyan][0][/] Cancelar")

        acao = input("\n  > ").strip()

        copiar = acao in ("1", "3")
        remover = acao in ("2", "3")

        if not copiar and not remover:
            console.print("[yellow]Cancelado.[/]")
            return

        # Copiar novos
        if copiar and novos:
            console.print(
                f"\n  [dim]Copiando {len(novos)} musicas...[/]"
            )
            ok_copy = 0

            with Progress(
                SpinnerColumn("dots"),
                TextColumn(
                    "[progress.description]{task.description}"
                ),
                BarColumn(bar_width=30),
                MofNCompleteColumn(),
                console=console,
            ) as progress:
                task = progress.add_task(
                    "[cyan]Copiando[/]", total=len(novos),
                )

                for rel in novos:
                    f = set_origem[rel]
                    dest_path = os.path.join(destino, rel)
                    dest_dir = os.path.dirname(dest_path)

                    progress.update(
                        task,
                        description=(
                            f"[cyan]{os.path.basename(rel)}[/]"
                        ),
                    )

                    try:
                        os.makedirs(dest_dir, exist_ok=True)
                        shutil.copy2(f["path"], dest_path)
                        ok_copy += 1
                    except Exception as e:
                        console.print(
                            f"    [red]x[/] {rel}: [dim]{e}[/]"
                        )

                    progress.advance(task)

            console.print(
                f"  [green]{ok_copy}[/] musicas copiadas"
            )

        # Remover extras
        if remover and removidos:
            console.print(
                f"\n  [bold yellow]ATENCAO:[/] Remover "
                f"{len(removidos)} musicas do destino? ([cyan]s[/]/n)"
            )
            if input("  > ").strip().lower() in (
                "s", "sim", "y", "yes",
            ):
                ok_del = 0
                for rel in removidos:
                    f = set_destino[rel]
                    try:
                        os.remove(f["path"])
                        ok_del += 1
                    except Exception:
                        pass

                cleanup_empty_dirs(destino)
                console.print(
                    f"  [green]{ok_del}[/] musicas removidas"
                )

        console.print(Panel(
            f"[bold green]Sincronizacao concluida![/]\n"
            f"[dim]{os.path.abspath(destino)}[/]",
            border_style="green", padding=(0, 2),
        ))

    # --------------------------------------------------------
    # HELPERS UI
    # --------------------------------------------------------

    def _download_to_folder(self, artist, title, number, folder, progress_callback=None):
        """Baixa uma faixa direto numa pasta (sem subpastas Artist/Album)."""
        import glob as _glob
        num_str = f"{number:02d}" if number else "00"

        # Verificar se ja existe arquivo com esse numero na pasta
        existing = _glob.glob(os.path.join(folder, f"{num_str}. *.{self.formato}"))
        if existing:
            return (True, f"Ja existe: {os.path.basename(existing[0])}")

        output_template = os.path.join(
            folder, f"{num_str}. %(title)s.%(ext)s"
        )
        query = f"ytsearch1:{artist} - {title}"
        cmd = [
            YTDLP_PATH,
            "-x", "--audio-format", self.formato,
            "--audio-quality", "0",
            "-o", output_template,
            "--no-playlist",
            "--progress", "--newline",
            query,
        ]
        try:
            proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True,
            )
            for line in proc.stdout:
                line = line.strip()
                if progress_callback and "[download]" in line:
                    m = re.search(r"(\d+(?:\.\d+)?)%\s+of\s+~?\s*([\d.]+\w+)\s+at\s+([\d.]+\w+/s)", line)
                    if m:
                        progress_callback(float(m.group(1)), m.group(2), m.group(3))
            proc.wait(timeout=300)
            return (proc.returncode == 0, "" if proc.returncode == 0 else "yt-dlp erro")
        except subprocess.TimeoutExpired:
            proc.kill()
            return (False, "Timeout")
        except Exception as e:
            return (False, str(e))

    def _show_download_result(self, ok, fail):
        """Mostra resultado final de download."""
        console.print()
        if fail == 0 and ok > 0:
            console.print(Panel(
                f"[bold green]Download completo![/] {ok} faixas baixadas\n"
                f"[dim]{self.output_dir}[/]",
                border_style="green", padding=(0, 2),
            ))
        elif ok == 0 and fail > 0:
            console.print(Panel(
                f"[bold red]Download falhou[/] ({fail} erros)",
                border_style="red", padding=(0, 2),
            ))
        elif ok > 0 and fail > 0:
            console.print(Panel(
                f"[bold yellow]Parcial:[/] [green]{ok}[/] OK, "
                f"[red]{fail}[/] falha(s)\n"
                f"[dim]{self.output_dir}[/]",
                border_style="yellow", padding=(0, 2),
            ))

    def show_menu(self):
        """Exibe o menu principal."""
        console.print()
        console.print(Panel(
            Text("Top 50 Brasil - Cliente de Musicas", style="bold white"),
            border_style="blue",
            padding=(0, 2),
        ))
        console.print("  [dim]───── Descobrir ─────[/]")
        console.print("  [cyan][ 1][/] Ver artistas do Top 50 Brasil")
        console.print("  [cyan][ 2][/] Ver os mais frequentes no Top 50")
        console.print("  [cyan][ 3][/] Baixar albuns dos mais frequentes")
        console.print("  [cyan][ 4][/] Baixar top musicas de um artista")
        console.print("  [cyan][ 5][/] Buscar albuns de um artista (metricas)")
        console.print("  [cyan][ 6][/] Top musicas do momento (Brasil)")
        console.print("  [dim]───── Gerenciamento ─────[/]")
        console.print("  [cyan][ 7][/] Exportar musicas (mover para outra pasta)")
        console.print("  [cyan][ 8][/] Organizar pasta de musicas")
        console.print("  [cyan][ 9][/] Resumo da pasta (artistas/musicas/disco)")
        console.print("  [cyan][10][/] Classificar por genero (Deezer)")
        console.print("  [cyan][11][/] Exportar shuffle (caixa de som)")
        console.print("  [cyan][12][/] Buscar na biblioteca local")
        console.print("  [cyan][13][/] Sincronizar com pendrive")
        console.print("  [dim]───── Ferramentas ─────[/]")
        console.print("  [cyan][14][/] Editor de tags / metadados")
        console.print("  [cyan][15][/] Gerador de playlists M3U")
        console.print("  [cyan][16][/] Verificar qualidade dos arquivos")
        console.print("  [cyan][17][/] Buscar letras de musicas")
        console.print("  [cyan][18][/] Baixar por link direto (Deezer/YouTube)")
        console.print("  [cyan][19][/] Historico e estatisticas")
        console.print("  [dim][ 0] Sair[/]")
        console.print()

    def menu_loop(self):
        """Loop principal do menu."""
        # Verificar dependencias
        ytdlp = check_ytdlp()
        ffmpeg = check_ffmpeg()

        if not ytdlp:
            console.print("[bold red]yt-dlp nao encontrado![/]")
            console.print("  Instale: [cyan]pip install yt-dlp[/]")
            sys.exit(1)
        if not ffmpeg:
            console.print("[bold red]ffmpeg nao encontrado![/]")
            console.print("  macOS: [cyan]brew install ffmpeg[/]")
            sys.exit(1)

        console.print(
            f"  [dim]yt-dlp {ytdlp} | ffmpeg OK | "
            f"formato: {self.formato} | saida: {self.output_dir}[/]"
        )

        opcoes = {
            "1": self.opcao_ver_top50,
            "2": self.opcao_mais_frequentes,
            "3": self.opcao_baixar_frequentes,
            "4": self.opcao_top_musicas,
            "5": self.opcao_buscar_albuns,
            "6": self.opcao_top_musicas_momento,
            "7": self.opcao_exportar,
            "8": self.opcao_organizar,
            "9": self.opcao_resumo,
            "10": self.opcao_genero,
            "11": self.opcao_shuffle_export,
            "12": self.opcao_buscar_local,
            "13": self.opcao_sincronizar,
            "14": self.opcao_editar_tags,
            "15": self.opcao_gerar_playlist,
            "16": self.opcao_verificar_qualidade,
            "17": self.opcao_letras,
            "18": self.opcao_baixar_link,
            "19": self.opcao_historico,
        }

        while True:
            self.show_menu()
            choice = input("  Opcao: ").strip()

            if choice == "0":
                console.print("\n[dim]Ate mais![/]\n")
                break

            handler = opcoes.get(choice)
            if handler:
                try:
                    handler()
                except KeyboardInterrupt:
                    console.print("\n[yellow]Cancelado.[/]")
            else:
                console.print("[red]Opcao invalida.[/]")


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Cliente de musicas - Top 50 Brasil",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Exemplos:
  python clienteMusica.py                          # Menu interativo
  python clienteMusica.py --resumo ./final         # Resumo rapido da pasta
  python clienteMusica.py --shuffle /Volumes/USB   # Shuffle no pendrive
  python clienteMusica.py --buscar ./final pagode  # Buscar na biblioteca
  python clienteMusica.py --baixar URL             # Baixar por link direto
        """,
    )
    parser.add_argument(
        "--resumo", metavar="PASTA",
        help="Mostra resumo rapido da pasta e sai",
    )
    parser.add_argument(
        "--shuffle", metavar="PASTA",
        help="Faz shuffle in-place na pasta e sai",
    )
    parser.add_argument(
        "--buscar", nargs=2, metavar=("PASTA", "TERMO"),
        help="Busca na biblioteca local e sai",
    )
    parser.add_argument(
        "--baixar", metavar="URL",
        help="Baixa por link direto (Deezer/YouTube) e sai",
    )

    args = parser.parse_args()

    cliente = ClienteMusica()

    # ── Modo CLI direto ──
    if args.resumo:
        pasta = os.path.expanduser(args.resumo)
        if not os.path.isdir(pasta):
            console.print(f"[red]Pasta '{pasta}' nao encontrada.[/]")
            return
        files = scan_audio_files(pasta)
        if not files:
            console.print(f"[yellow]Nenhum arquivo em '{pasta}'[/]")
            return
        artists = {}
        for f in files:
            key = f["artist"]
            if key not in artists:
                artists[key] = {"albums": set(), "tracks": 0, "size": 0}
            artists[key]["albums"].add(f["album"])
            artists[key]["tracks"] += 1
            artists[key]["size"] += f["size"]

        table = Table(box=box.ROUNDED, title_style="bold")
        table.add_column("Artista", style="cyan bold")
        table.add_column("Albuns", justify="center", style="green")
        table.add_column("Musicas", justify="center")
        table.add_column("Tamanho", justify="right", style="yellow")
        for name, info in sorted(
            artists.items(),
            key=lambda x: x[1]["tracks"],
            reverse=True,
        ):
            table.add_row(
                name, str(len(info["albums"])),
                str(info["tracks"]), format_size(info["size"]),
            )
        console.print(table)
        total_size = sum(a["size"] for a in artists.values())
        console.print(
            f"\n  {len(artists)} artistas | {len(files)} musicas | "
            f"{format_size(total_size)}"
        )
        return

    if args.shuffle:
        cliente._shuffle_in_place(
            pasta_preenchida=os.path.expanduser(args.shuffle),
        )
        return

    if args.buscar:
        pasta, termo = args.buscar
        pasta = os.path.expanduser(pasta)
        if not os.path.isdir(pasta):
            console.print(f"[red]Pasta '{pasta}' nao encontrada.[/]")
            return
        files = scan_audio_files(pasta)
        termo_lower = termo.lower()
        resultados = [
            f for f in files
            if termo_lower in f["filename"].lower()
            or termo_lower in f["artist"].lower()
            or termo_lower in f["album"].lower()
        ]
        if not resultados:
            console.print(f"[yellow]Nenhum resultado para '{termo}'[/]")
            return
        for f in resultados:
            console.print(
                f"  [cyan]{f['artist']}[/] / "
                f"[green]{f['album']}[/] / {f['filename']}"
            )
        console.print(f"\n  {len(resultados)} resultado(s)")
        return

    if args.baixar:
        url = args.baixar
        if "deezer.com" in url and "/album/" in url:
            cliente._baixar_deezer_album(url)
        elif "deezer.com" in url and "/track/" in url:
            cliente._baixar_deezer_track(url)
        elif "youtube.com" in url or "youtu.be" in url:
            cliente._baixar_youtube(url)
        else:
            console.print("[red]URL nao reconhecida.[/]")
        return

    # ── Modo interativo (menu) ──
    try:
        cliente.menu_loop()
    except KeyboardInterrupt:
        console.print("\n\n[dim]Saindo...[/]\n")


if __name__ == "__main__":
    main()
