"""
organizar_musicas.py - Organiza a pasta de musicas na estrutura final
=====================================================================
Varre a pasta de musicas baixadas e move tudo para uma pasta "final/"
com a estrutura:  Artista / Album / musicas

Detecta e trata arquivos duplicados por hash (SHA-256).
Usa shutil.move para nao duplicar espaco em disco.

Uso:
    python organizar_musicas.py                         # Padrao: ./musicas -> ./final
    python organizar_musicas.py --origem ./musicas      # Pasta de origem
    python organizar_musicas.py --destino ./final       # Pasta de destino
    python organizar_musicas.py --dry-run               # Simula sem mover
"""

import argparse
import hashlib
import os
import shutil
import sys

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, MofNCompleteColumn
from rich import box

# ============================================================
# CONFIG
# ============================================================

DEFAULT_ORIGEM = "./musicas"
DEFAULT_DESTINO = "./final"
AUDIO_EXTENSIONS = {".mp3", ".flac", ".ogg", ".opus", ".m4a", ".wav", ".aac", ".wma"}

console = Console()


# ============================================================
# UTILIDADES
# ============================================================

def file_hash(filepath, chunk_size=8192):
    """Calcula SHA-256 de um arquivo para detectar duplicatas."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def sanitize_name(name):
    """Remove caracteres invalidos para nomes de pasta/arquivo."""
    invalid = '<>:"/\\|?*'
    for c in invalid:
        name = name.replace(c, "")
    return name.strip()


def scan_audio_files(origem):
    """
    Varre a pasta de origem e retorna lista de dicts com info de cada arquivo.
    Espera estrutura: origem/Artista/Album/musica.ext
    Tambem aceita: origem/Artista/musica.ext (album = "Singles")
    """
    files = []
    if not os.path.isdir(origem):
        return files

    for root, dirs, filenames in os.walk(origem):
        for fname in filenames:
            ext = os.path.splitext(fname)[1].lower()
            if ext not in AUDIO_EXTENSIONS:
                continue

            full_path = os.path.join(root, fname)
            rel_path = os.path.relpath(full_path, origem)
            parts = rel_path.split(os.sep)

            if len(parts) >= 3:
                # Artista/Album/musica.ext
                artist = parts[0]
                album = parts[1]
            elif len(parts) == 2:
                # Artista/musica.ext -> album = "Singles"
                artist = parts[0]
                album = "Singles"
            else:
                # musica.ext na raiz -> desconhecido
                artist = "Desconhecido"
                album = "Singles"

            files.append({
                "path": full_path,
                "filename": fname,
                "artist": artist,
                "album": album,
                "rel_path": rel_path,
                "size": os.path.getsize(full_path),
            })

    return files


def detect_duplicates(files):
    """
    Detecta duplicatas por hash SHA-256.
    Retorna (unicos, duplicatas) onde duplicatas e lista de dicts com info.
    """
    hash_map = {}  # hash -> primeiro arquivo
    unicos = []
    duplicatas = []

    for f in files:
        h = file_hash(f["path"])
        if h in hash_map:
            duplicatas.append({
                **f,
                "hash": h,
                "original": hash_map[h]["path"],
            })
        else:
            hash_map[h] = f
            unicos.append({**f, "hash": h})

    return unicos, duplicatas


def move_files(unicos, destino, dry_run=False):
    """
    Move arquivos unicos para a pasta destino na estrutura:
    destino/Artista/Album/musica.ext

    Usa shutil.move (nao copia, move direto).
    Retorna (movidos, erros).
    """
    movidos = 0
    erros = []

    with Progress(
        SpinnerColumn("dots"),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=30),
        MofNCompleteColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("[cyan]Organizando[/]", total=len(unicos))

        for f in unicos:
            artist_dir = sanitize_name(f["artist"])
            album_dir = sanitize_name(f["album"])
            dest_dir = os.path.join(destino, artist_dir, album_dir)
            dest_path = os.path.join(dest_dir, f["filename"])

            progress.update(
                task,
                description=f"[cyan]{artist_dir}[/]/[green]{album_dir}[/]",
            )

            # Se origem == destino, pular
            if os.path.abspath(f["path"]) == os.path.abspath(dest_path):
                movidos += 1
                progress.advance(task)
                continue

            # Se ja existe no destino, verificar se e o mesmo arquivo
            if os.path.exists(dest_path):
                existing_hash = file_hash(dest_path)
                if existing_hash == f["hash"]:
                    # Mesmo arquivo, remover origem
                    if not dry_run:
                        os.remove(f["path"])
                    movidos += 1
                    progress.advance(task)
                    continue
                else:
                    # Arquivo diferente com mesmo nome - renomear
                    base, ext = os.path.splitext(f["filename"])
                    counter = 1
                    while os.path.exists(dest_path):
                        dest_path = os.path.join(dest_dir, f"{base} ({counter}){ext}")
                        counter += 1

            try:
                if not dry_run:
                    os.makedirs(dest_dir, exist_ok=True)
                    shutil.move(f["path"], dest_path)
                movidos += 1
            except Exception as e:
                erros.append({"file": f["path"], "error": str(e)})

            progress.advance(task)

    return movidos, erros


def cleanup_empty_dirs(path):
    """Remove pastas vazias recursivamente."""
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


def show_summary(files, unicos, duplicatas, destino):
    """Mostra tabela resumo por artista."""
    # Agrupar por artista
    artists = {}
    for f in unicos:
        key = f["artist"]
        if key not in artists:
            artists[key] = {"albums": set(), "tracks": 0, "size": 0}
        artists[key]["albums"].add(f["album"])
        artists[key]["tracks"] += 1
        artists[key]["size"] += f["size"]

    table = Table(
        title="Estrutura Final",
        box=box.ROUNDED,
        title_style="bold magenta",
    )
    table.add_column("#", style="dim", width=4, justify="right")
    table.add_column("Artista", style="cyan bold")
    table.add_column("Albuns", justify="center", style="green")
    table.add_column("Faixas", justify="center")
    table.add_column("Tamanho", justify="right", style="yellow")

    sorted_artists = sorted(artists.items(), key=lambda x: x[1]["tracks"], reverse=True)
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

    total_mb = total_size / (1024 * 1024)
    console.print(
        f"\n  [bold]Total:[/] {len(artists)} artistas, "
        f"{total_tracks} faixas, {total_mb:.1f} MB"
    )
    if duplicatas:
        dup_size = sum(d["size"] for d in duplicatas) / (1024 * 1024)
        console.print(
            f"  [yellow]Duplicatas encontradas:[/] {len(duplicatas)} "
            f"arquivos ({dup_size:.1f} MB economizados)"
        )


def show_duplicates_detail(duplicatas):
    """Mostra tabela detalhada de duplicatas."""
    if not duplicatas:
        return

    table = Table(
        title="Arquivos Duplicados",
        box=box.ROUNDED,
        title_style="bold yellow",
    )
    table.add_column("#", style="dim", width=4, justify="right")
    table.add_column("Duplicata", style="red")
    table.add_column("Original", style="green")
    table.add_column("Tamanho", justify="right", style="yellow")

    for i, d in enumerate(duplicatas, 1):
        size_mb = d["size"] / (1024 * 1024)
        table.add_row(
            str(i),
            d["rel_path"],
            os.path.relpath(d["original"], os.path.dirname(d["path"]) + "/.."),
            f"{size_mb:.1f} MB",
        )

    console.print()
    console.print(table)


# ============================================================
# FUNCAO PRINCIPAL (usavel como modulo)
# ============================================================

def organizar(origem=DEFAULT_ORIGEM, destino=DEFAULT_DESTINO, dry_run=False, verbose=True):
    """
    Funcao principal de organizacao. Pode ser chamada por outros scripts.
    Retorna dict com estatisticas.
    """
    if not os.path.isdir(origem):
        if verbose:
            console.print(f"\n[bold red]Pasta '{origem}' nao encontrada![/]\n")
        return {"ok": False, "error": "Pasta de origem nao encontrada"}

    if verbose:
        console.print(Panel(
            f"[bold]Organizador de Musicas[/]\n\n"
            f"Origem:  [cyan]{os.path.abspath(origem)}[/]\n"
            f"Destino: [green]{os.path.abspath(destino)}[/]"
            + ("\n[yellow]MODO SIMULACAO (dry-run)[/]" if dry_run else ""),
            border_style="blue",
            padding=(0, 2),
        ))

    # 1. Varrer arquivos
    if verbose:
        console.print("\n  [dim]Varrendo arquivos de audio...[/]")
    files = scan_audio_files(origem)
    if not files:
        if verbose:
            console.print("[yellow]Nenhum arquivo de audio encontrado.[/]")
        return {"ok": True, "total": 0, "moved": 0, "duplicates": 0}

    if verbose:
        console.print(f"  Encontrados: [green]{len(files)}[/] arquivos de audio\n")

    # 2. Detectar duplicatas
    if verbose:
        console.print("  [dim]Verificando duplicatas (SHA-256)...[/]")
    unicos, duplicatas = detect_duplicates(files)

    if verbose and duplicatas:
        console.print(
            f"  [yellow]{len(duplicatas)}[/] duplicata(s) encontrada(s)\n"
        )
        show_duplicates_detail(duplicatas)

    # 3. Mostrar resumo
    if verbose:
        show_summary(files, unicos, duplicatas, destino)

    # 4. Confirmar
    if verbose and not dry_run:
        console.print(
            f"\n  Mover [green]{len(unicos)}[/] arquivos para "
            f"[cyan]{destino}/[/]? ([cyan]s[/]/n)"
        )
        resp = input("  > ").strip().lower()
        if resp not in ("s", "sim", "y", "yes"):
            console.print("[yellow]Cancelado.[/]")
            return {"ok": True, "total": len(files), "moved": 0, "duplicates": len(duplicatas), "cancelled": True}

    # 5. Mover
    movidos, erros = move_files(unicos, destino, dry_run=dry_run)

    # 6. Remover duplicatas da origem
    if not dry_run:
        for d in duplicatas:
            try:
                if os.path.exists(d["path"]):
                    os.remove(d["path"])
            except OSError:
                pass

    # 7. Limpar pastas vazias na origem
    if not dry_run:
        removed_dirs = cleanup_empty_dirs(origem)
        if verbose and removed_dirs:
            console.print(f"  [dim]Removidas {removed_dirs} pastas vazias[/]")

    # 8. Resultado final
    if verbose:
        console.print()
        if erros:
            console.print(Panel(
                f"[bold yellow]Organizacao concluida com erros[/]\n"
                f"[green]{movidos}[/] movidos | [red]{len(erros)}[/] erros | "
                f"[yellow]{len(duplicatas)}[/] duplicatas removidas\n"
                f"[dim]{os.path.abspath(destino)}[/]",
                border_style="yellow", padding=(0, 2),
            ))
            for e in erros[:5]:
                console.print(f"  [red]x[/] {e['file']}: [dim]{e['error']}[/]")
        else:
            console.print(Panel(
                f"[bold green]Organizacao completa![/]\n"
                f"{movidos} arquivos organizados | "
                f"{len(duplicatas)} duplicatas removidas\n"
                f"[dim]{os.path.abspath(destino)}[/]",
                border_style="green", padding=(0, 2),
            ))
        console.print()

    return {
        "ok": True,
        "total": len(files),
        "moved": movidos,
        "duplicates": len(duplicatas),
        "errors": len(erros),
    }


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Organiza pasta de musicas: Artista/Album/musica",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python organizar_musicas.py                       # Padrao: ./musicas -> ./final
  python organizar_musicas.py --origem ./musicas    # Mudar origem
  python organizar_musicas.py --destino ~/Musicas   # Mudar destino
  python organizar_musicas.py --dry-run             # Simular sem mover
        """,
    )
    parser.add_argument(
        "--origem", default=DEFAULT_ORIGEM,
        help="Pasta com as musicas baixadas (default: ./musicas)",
    )
    parser.add_argument(
        "--destino", default=DEFAULT_DESTINO,
        help="Pasta de destino organizada (default: ./final)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Simula sem mover nenhum arquivo",
    )

    args = parser.parse_args()
    organizar(origem=args.origem, destino=args.destino, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
