"""
down_albuns.py - Baixa albuns do resultado.json usando yt-dlp
=============================================================
Le o arquivo gerado pelo scraper, busca a tracklist via Deezer API
e baixa cada faixa do YouTube via yt-dlp.

Uso:
    python down_albuns.py                           # Baixa todos
    python down_albuns.py --artista "Gusttavo Lima"  # So um artista
    python down_albuns.py --formato flac             # Muda formato
    python down_albuns.py --listar                   # So mostra o que vai baixar
    python down_albuns.py --interativo               # Escolhe quais baixar
"""

import glob
import json
import subprocess
import sys
import os
import argparse
import re
import time
import threading

import requests

from rich.console import Console
from rich.progress import (
    Progress,
    SpinnerColumn,
    BarColumn,
    TextColumn,
    TimeElapsedColumn,
    MofNCompleteColumn,
    DownloadColumn,
    TransferSpeedColumn,
)
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.live import Live
from rich.columns import Columns
from rich import box


# ============================================================
# CONFIG
# ============================================================

RESULTADO_FILE = "resultado.json"
DEFAULT_OUTPUT_DIR = "./musicas"
DEFAULT_FORMAT = "mp3"
FORMATOS_VALIDOS = ["mp3", "flac", "ogg", "opus", "m4a", "wav"]
DEEZER_API = "https://api.deezer.com"

# Caminho do yt-dlp dentro do venv (mesmo diretorio do script)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
YTDLP_PATH = os.path.join(SCRIPT_DIR, "env", "bin", "yt-dlp")
# Fallback para yt-dlp global se nao encontrar no venv
if not os.path.exists(YTDLP_PATH):
    YTDLP_PATH = "yt-dlp"

console = Console()


# ============================================================
# VERIFICACOES
# ============================================================

def check_ytdlp():
    """Verifica se yt-dlp esta instalado. Retorna versao ou None."""
    try:
        result = subprocess.run(
            [YTDLP_PATH, "--version"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None


def check_ffmpeg():
    """Verifica se FFmpeg esta disponivel (necessario para converter audio)."""
    try:
        result = subprocess.run(
            ["ffmpeg", "-version"],
            capture_output=True, text=True, timeout=10,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


# ============================================================
# DADOS
# ============================================================

def load_resultado(filepath):
    """Carrega o arquivo resultado.json."""
    if not os.path.exists(filepath):
        console.print(f"\n[bold red]Arquivo '{filepath}' nao encontrado![/]")
        console.print(f"Execute primeiro: [cyan]python scrapper.py[/]")
        sys.exit(1)

    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data


def sanitize_filename(name):
    """Remove caracteres invalidos para nome de pasta/arquivo."""
    # Canonical em utils.py, mantido aqui para compatibilidade de import
    from utils import sanitize_filename as _sf
    return _sf(name)


# ============================================================
# DEEZER API - BUSCAR TRACKLIST
# ============================================================

def get_deezer_tracklist(deezer_url):
    """
    Busca a lista de faixas de um album via Deezer API publica.
    Retorna lista de dicts com title, number, duration.
    """
    match = re.search(r"/album/(\d+)", deezer_url or "")
    if not match:
        return []

    album_id = match.group(1)
    try:
        resp = requests.get(
            f"{DEEZER_API}/album/{album_id}/tracks",
            timeout=10,
        )
        if resp.status_code != 200:
            return []

        tracks = []
        for track in resp.json().get("data", []):
            tracks.append({
                "title": track.get("title", "Desconhecido"),
                "number": track.get("track_position", 0),
                "duration": track.get("duration", 0),
                "artist": track.get("artist", {}).get("name", ""),
            })
        return tracks
    except Exception:
        return []


# ============================================================
# DOWNLOAD VIA YT-DLP
# ============================================================

def download_track(artist, track_title, album_name, track_number,
                   output_dir, audio_format, progress_callback=None):
    """
    Baixa uma faixa do YouTube via yt-dlp.
    Retorna (sucesso: bool, mensagem: str).

    progress_callback(percent, size_str, speed_str) e chamado durante o download.
    """
    safe_artist = sanitize_filename(artist)
    safe_album = sanitize_filename(album_name)
    output_path = os.path.join(output_dir, safe_artist, safe_album)
    os.makedirs(output_path, exist_ok=True)

    # Formatar numero da faixa com zero-padding
    num_str = f"{track_number:02d}" if track_number else "00"

    # Verificar se ja existe arquivo com esse numero na pasta
    existing = glob.glob(os.path.join(output_path, f"{num_str}. *.{audio_format}"))
    if existing:
        return True, f"Ja existe: {os.path.basename(existing[0])}"

    # Template de saida: "01. Nome da Musica.mp3"
    output_template = os.path.join(
        output_path, f"{num_str}. %(title)s.%(ext)s"
    )

    # Query de busca no YouTube
    query = f"ytsearch1:{artist} - {track_title}"

    cmd = [
        YTDLP_PATH,
        "-x",                           # Extrair somente audio
        "--audio-format", audio_format,  # Formato de saida
        "--audio-quality", "0",          # Melhor qualidade
        "-o", output_template,           # Template de saida
        "--no-playlist",                 # Nao baixar playlists
        "--progress",                    # Mostrar progresso
        "--newline",                     # Progresso em linhas separadas
        "--no-warnings",                 # Sem warnings
        query,
    ]

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        # Ler saida linha por linha para progresso
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue

            # Parsear linhas de progresso do yt-dlp
            # Ex: "[download]  45.2% of   3.05MiB at  12.34MiB/s ETA 00:00"
            if "[download]" in line and "%" in line:
                pct_match = re.search(r"(\d+\.?\d*)%", line)
                size_match = re.search(r"of\s+([\d.]+\w+)", line)
                speed_match = re.search(r"at\s+([\d.]+\w+/s)", line)

                if pct_match and progress_callback:
                    pct = float(pct_match.group(1))
                    size_str = size_match.group(1) if size_match else ""
                    speed_str = speed_match.group(1) if speed_match else ""
                    progress_callback(pct, size_str, speed_str)

        proc.wait(timeout=120)

        if proc.returncode == 0:
            return True, "OK"
        return False, f"yt-dlp retornou codigo {proc.returncode}"

    except subprocess.TimeoutExpired:
        proc.kill()
        return False, "Timeout"
    except Exception as e:
        return False, str(e)[:120]


# ============================================================
# EXIBICAO
# ============================================================

def list_albums(data):
    """Lista todos os albuns em uma tabela formatada."""
    table = Table(
        title="Albuns Disponiveis para Download",
        box=box.ROUNDED,
        title_style="bold magenta",
    )
    table.add_column("#", style="dim", width=4, justify="right")
    table.add_column("Artista", style="cyan bold")
    table.add_column("Album", style="green")
    table.add_column("Data", style="yellow", width=12)

    idx = 1
    for artist in data:
        for album in artist.get("albums", []):
            table.add_row(
                str(idx),
                artist["name"],
                album["name"],
                album.get("release_date", "N/A"),
            )
            idx += 1

    console.print()
    console.print(table)
    console.print(f"\n  Total: [bold]{idx - 1}[/bold] albuns\n")


def show_header(data, audio_format, output_dir):
    """Mostra o cabecalho com info geral."""
    total_artists = len(data)
    total_albums = sum(len(a.get("albums", [])) for a in data)

    header = Text()
    header.append("Download de Albuns via yt-dlp\n", style="bold white")
    header.append("Top 50 Brasil\n\n", style="dim")
    header.append(f"{total_artists} artistas", style="cyan")
    header.append("  |  ")
    header.append(f"{total_albums} albuns", style="green")
    header.append("  |  formato: ")
    header.append(audio_format.upper(), style="yellow bold")
    header.append("  |  saida: ")
    header.append(os.path.abspath(output_dir), style="dim")

    console.print()
    console.print(Panel(header, border_style="blue", padding=(0, 2)))


def show_dependencies(ytdlp_version, ffmpeg_ok):
    """Mostra status das dependencias."""
    table = Table(box=None, show_header=False, padding=(0, 1))
    table.add_column("status", width=6)
    table.add_column("dep", width=10)
    table.add_column("info")

    if ytdlp_version:
        table.add_row("[green]OK[/]", "yt-dlp", f"[dim]v{ytdlp_version}[/]")
    else:
        table.add_row("[red]FALTA[/]", "yt-dlp", "[red]nao encontrado[/]")

    if ffmpeg_ok:
        table.add_row("[green]OK[/]", "ffmpeg", "[dim]disponivel[/]")
    else:
        table.add_row("[red]FALTA[/]", "ffmpeg", "[red]necessario para converter audio[/]")

    console.print()
    console.print(Panel(table, title="Dependencias", border_style="dim", padding=(0, 1)))


def show_summary(results, output_dir):
    """Mostra tabela resumo e totais."""
    table = Table(
        title="Resumo dos Downloads",
        box=box.ROUNDED,
        title_style="bold",
    )
    table.add_column("Artista", style="cyan")
    table.add_column("Album", style="green")
    table.add_column("Faixas", justify="center")
    table.add_column("Status", justify="center")

    grand_ok = 0
    grand_fail = 0

    for r in results:
        ok = r["tracks_ok"]
        fail = r["tracks_fail"]
        total = ok + fail
        grand_ok += ok
        grand_fail += fail

        if fail == 0 and ok > 0:
            status = "[bold green]OK[/]"
        elif ok == 0:
            status = "[bold red]FALHOU[/]"
        else:
            status = f"[yellow]parcial ({ok}/{total})[/]"

        table.add_row(
            r["artist"],
            r["album"],
            f"[green]{ok}[/]/[dim]{total}[/]",
            status,
        )

    console.print()
    console.print(table)

    # Painel final
    console.print()
    if grand_fail == 0 and grand_ok > 0:
        console.print(Panel(
            f"[bold green]Download completo![/]\n"
            f"{grand_ok} faixas baixadas com sucesso\n"
            f"[dim]{os.path.abspath(output_dir)}[/]",
            border_style="green", padding=(0, 2),
        ))
    elif grand_ok == 0:
        console.print(Panel(
            f"[bold red]Nenhuma faixa baixada[/]\n"
            f"Verifique sua conexao e dependencias.",
            border_style="red", padding=(0, 2),
        ))
    else:
        console.print(Panel(
            f"[bold yellow]Download finalizado com erros[/]\n"
            f"[green]{grand_ok}[/] faixas OK  |  "
            f"[red]{grand_fail}[/] faixas com falha\n"
            f"[dim]{os.path.abspath(output_dir)}[/]",
            border_style="yellow", padding=(0, 2),
        ))
    console.print()


# ============================================================
# INTERACTIVE SELECT
# ============================================================

def interactive_select(data):
    """Modo interativo: usuario escolhe quais albuns baixar."""
    albums_flat = []
    idx = 1
    for artist in data:
        for album in artist.get("albums", []):
            albums_flat.append({
                "idx": idx,
                "artist": artist["name"],
                "album": album["name"],
                "data": album,
            })
            idx += 1

    list_albums(data)

    console.print("Digite os numeros dos albuns para baixar.")
    console.print("Exemplos: [cyan]1,3,5[/] ou [cyan]todos[/] ou [cyan]1-5[/]\n")

    choice = input("  Sua escolha: ").strip().lower()

    if choice in ("todos", "all", "*"):
        return albums_flat

    selected = []
    for part in choice.split(","):
        part = part.strip()
        if "-" in part:
            try:
                start, end = part.split("-")
                for i in range(int(start), int(end) + 1):
                    match = next((a for a in albums_flat if a["idx"] == i), None)
                    if match:
                        selected.append(match)
            except ValueError:
                console.print(f"  [yellow]Ignorando: {part}[/]")
        else:
            try:
                i = int(part)
                match = next((a for a in albums_flat if a["idx"] == i), None)
                if match:
                    selected.append(match)
            except ValueError:
                console.print(f"  [yellow]Ignorando: {part}[/]")

    return selected


# ============================================================
# DOWNLOAD PRINCIPAL
# ============================================================

def download_all(data, output_dir, audio_format):
    """
    Baixa todos os albuns com progresso visual.
    Para cada album, busca tracklist no Deezer e baixa faixa por faixa via yt-dlp.
    """
    os.makedirs(output_dir, exist_ok=True)
    total_albums = sum(len(a.get("albums", [])) for a in data)
    results = []
    album_num = 0

    with Progress(
        SpinnerColumn("dots"),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=30),
        MofNCompleteColumn(),
        TextColumn("[dim]|[/]"),
        TimeElapsedColumn(),
        console=console,
        expand=False,
    ) as progress:

        album_task = progress.add_task(
            "[bold blue]Albuns[/]", total=total_albums,
        )
        track_task = progress.add_task(
            "[cyan]Faixa[/]", total=0, visible=False,
        )
        dl_task = progress.add_task(
            "[dim]Download[/]", total=100, visible=False,
        )

        for artist in data:
            artist_name = artist["name"]

            for album in artist.get("albums", []):
                album_name = album["name"]
                deezer_url = album.get("deezer_url")
                album_num += 1

                # Separador visual
                console.print()
                console.print(Panel(
                    f"[cyan]{artist_name}[/]  -  [green]{album_name}[/]",
                    border_style="dim",
                    padding=(0, 1),
                ))

                # --- Buscar tracklist no Deezer ---
                progress.update(
                    track_task,
                    description="[dim]Buscando tracklist...[/]",
                    total=1, completed=0, visible=True,
                )
                progress.update(dl_task, visible=False)

                tracks = get_deezer_tracklist(deezer_url)
                tracks_ok = 0
                tracks_fail = 0
                failed_tracks = []

                if tracks:
                    total_tracks = len(tracks)
                    progress.update(
                        track_task,
                        total=total_tracks,
                        completed=0,
                        description=f"[cyan]Faixas[/] [dim](0/{total_tracks})[/]",
                    )

                    for track in tracks:
                        title = track["title"]
                        num = track["number"]

                        progress.update(
                            track_task,
                            description=(
                                f"[cyan]{num:02d}[/] [white]{title}[/]"
                            ),
                        )
                        progress.update(
                            dl_task,
                            total=100, completed=0, visible=True,
                            description="[dim]...[/]",
                        )

                        # Callback para atualizar barra de download
                        def on_progress(pct, size_str, speed_str):
                            desc = f"[dim]{size_str}[/]"
                            if speed_str:
                                desc += f" [dim]@ {speed_str}[/]"
                            progress.update(
                                dl_task,
                                completed=pct,
                                description=desc,
                            )

                        success, msg = download_track(
                            artist_name, title, album_name, num,
                            output_dir, audio_format,
                            progress_callback=on_progress,
                        )

                        if success:
                            tracks_ok += 1
                            progress.update(dl_task, completed=100)
                        else:
                            tracks_fail += 1
                            failed_tracks.append(title)
                            console.print(
                                f"    [red]x[/] {title}: [dim]{msg}[/]"
                            )

                        progress.advance(track_task)

                else:
                    # Sem tracklist: tentar busca direta do album
                    progress.update(
                        track_task,
                        total=1, completed=0, visible=True,
                        description=f"[yellow]Sem tracklist[/] [dim]> busca direta[/]",
                    )
                    progress.update(
                        dl_task,
                        total=100, completed=0, visible=True,
                        description="[dim]...[/]",
                    )

                    def on_progress(pct, size_str, speed_str):
                        desc = f"[dim]{size_str}[/]"
                        if speed_str:
                            desc += f" [dim]@ {speed_str}[/]"
                        progress.update(dl_task, completed=pct, description=desc)

                    success, msg = download_track(
                        artist_name, album_name, album_name, 1,
                        output_dir, audio_format,
                        progress_callback=on_progress,
                    )

                    if success:
                        tracks_ok = 1
                    else:
                        tracks_fail = 1
                        console.print(
                            f"    [red]x[/] {album_name}: [dim]{msg}[/]"
                        )

                    progress.advance(track_task)

                # Resultado do album
                if tracks_fail == 0:
                    console.print(
                        f"    [green]OK[/] {tracks_ok} faixas baixadas"
                    )
                elif tracks_ok > 0:
                    console.print(
                        f"    [yellow]![/] {tracks_ok} OK, "
                        f"{tracks_fail} falha(s)"
                    )

                results.append({
                    "artist": artist_name,
                    "album": album_name,
                    "tracks_ok": tracks_ok,
                    "tracks_fail": tracks_fail,
                })

                progress.advance(album_task)
                progress.update(dl_task, visible=False)

        # Limpar barras ao final
        progress.update(track_task, visible=False)
        progress.update(dl_task, visible=False)
        progress.update(
            album_task,
            description="[bold green]Concluido[/]",
        )

    return results


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Baixa albuns do resultado.json via yt-dlp",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python down_albuns.py                           # Baixa todos em MP3
  python down_albuns.py --formato flac            # Baixa em FLAC
  python down_albuns.py --artista "Gusttavo Lima"  # So um artista
  python down_albuns.py --interativo              # Escolhe quais baixar
  python down_albuns.py --listar                  # So lista os albuns
        """,
    )

    parser.add_argument(
        "--arquivo", default=RESULTADO_FILE,
        help="Arquivo JSON de entrada (default: resultado.json)",
    )
    parser.add_argument(
        "--saida", default=DEFAULT_OUTPUT_DIR,
        help="Pasta de saida (default: ./musicas)",
    )
    parser.add_argument(
        "--formato", default=DEFAULT_FORMAT,
        choices=FORMATOS_VALIDOS,
        help="Formato de audio (default: mp3)",
    )
    parser.add_argument(
        "--artista", default=None,
        help="Baixar so de um artista especifico",
    )
    parser.add_argument(
        "--listar", action="store_true",
        help="Apenas listar albuns disponiveis",
    )
    parser.add_argument(
        "--interativo", action="store_true",
        help="Modo interativo para escolher albuns",
    )

    args = parser.parse_args()

    # --- Carregar dados ---
    data = load_resultado(args.arquivo)

    # --- Filtrar por artista ---
    if args.artista:
        data = [a for a in data if args.artista.lower() in a["name"].lower()]
        if not data:
            console.print(
                f"\n[bold red]Artista '{args.artista}' nao encontrado![/]\n"
            )
            sys.exit(1)

    # --- Modo listar ---
    if args.listar:
        list_albums(data)
        return

    # --- Verificar dependencias ---
    ytdlp_version = check_ytdlp()
    ffmpeg_ok = check_ffmpeg()
    show_dependencies(ytdlp_version, ffmpeg_ok)

    if not ytdlp_version:
        console.print("\n[bold red]yt-dlp e necessario![/]")
        console.print("  Instale com: [cyan]pip install yt-dlp[/]\n")
        sys.exit(1)

    if not ffmpeg_ok:
        console.print(
            "\n[bold red]ffmpeg e necessario para converter audio![/]"
        )
        console.print("  macOS:   [cyan]brew install ffmpeg[/]")
        console.print("  Ubuntu:  [cyan]sudo apt install ffmpeg[/]")
        console.print("  Windows: [cyan]choco install ffmpeg[/]\n")
        sys.exit(1)

    # --- Cabecalho ---
    show_header(data, args.formato, args.saida)

    # --- Modo interativo ---
    if args.interativo:
        selected = interactive_select(data)
        if not selected:
            console.print("[yellow]Nenhum album selecionado.[/]")
            return

        # Reconstruir estrutura de dados
        artist_map = {}
        for item in selected:
            key = item["artist"]
            if key not in artist_map:
                artist_map[key] = {"name": key, "albums": []}
            artist_map[key]["albums"].append(item["data"])

        filtered_data = list(artist_map.values())
        results = download_all(filtered_data, args.saida, args.formato)
        show_summary(results, args.saida)
        return

    # --- Modo automatico: baixar tudo ---
    results = download_all(data, args.saida, args.formato)
    show_summary(results, args.saida)


if __name__ == "__main__":
    main()
