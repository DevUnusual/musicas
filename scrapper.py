"""
Bot de Scraping - Top Cantores Brasil
======================================
Busca os 10 cantores mais populares no Brasil,
seus 2 melhores álbuns e links para download/streaming.

✅ SEM necessidade de conta Premium ou API key
Fontes: Spotify Web (scraping), Deezer API (pública), SuaMusica, YouTube
"""

import requests
import json
import re
import time
from urllib.parse import quote
from dataclasses import dataclass, field
from typing import Optional
from collections import Counter


# ============================================================
# CONFIG
# ============================================================

SPOTIFY_PLAYLIST_URL = "https://open.spotify.com/playlist/37i9dQZEVXbMXbN3EUUhlg"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
}


# ============================================================
# DATA CLASSES
# ============================================================

@dataclass
class Album:
    name: str
    artist: str
    release_date: str
    total_tracks: int
    cover_url: Optional[str] = None
    spotify_url: Optional[str] = None
    deezer_url: Optional[str] = None
    download_links: dict = field(default_factory=dict)


@dataclass
class Artist:
    name: str
    track_count: int = 0
    genres: list = field(default_factory=list)
    popularity: int = 0
    spotify_url: Optional[str] = None
    deezer_id: Optional[int] = None
    top_albums: list = field(default_factory=list)


# ============================================================
# 1. SCRAPING DO SPOTIFY WEB (sem API)
# ============================================================

def parse_spotify_page(html_text: str) -> list[str]:
    """
    Parseia a página do Spotify extraindo nomes de artistas.
    O open.spotify.com renderiza server-side metadata suficiente.

    Formato do conteúdo extraído:
        [Nome da Música](/track/ID)
        E                               <-- flag explicit (opcional)
        Artista1, Artista2, Artista3    <-- linha de artistas
    """
    artists = []
    lines = html_text.split('\n')
    prev_was_track = False
    skip_items = {'Home', 'Search', 'Your Library', 'Premium', '', 'E', ','}

    for line in lines:
        line = line.strip()

        # Detectar linha de track: [Nome](/track/xxx)
        if '/track/' in line and line.startswith('['):
            prev_was_track = True
            continue

        # Flag "E" (explícito) - pular mas manter estado
        if line == 'E':
            continue

        # Se linha anterior era track, esta linha tem os artistas
        if prev_was_track and line:
            if not line.startswith('[') and not line.startswith('!') and '/track/' not in line:
                for artist in line.split(','):
                    name = artist.strip()
                    if name and name not in skip_items and len(name) > 1 and not name.startswith('/'):
                        artists.append(name)
                prev_was_track = False
                continue

        if line and not line.startswith('[') and not line.startswith('!'):
            prev_was_track = False

    return artists


# ============================================================
# 2. DEEZER API (100% gratuita, sem autenticação)
# ============================================================

class DeezerClient:
    """
    API pública do Deezer - sem key necessária.
    Busca artistas, álbuns e gera links de streaming.
    """

    BASE_URL = "https://api.deezer.com"

    def _get(self, endpoint: str, params: dict = None) -> dict:
        try:
            resp = requests.get(
                f"{self.BASE_URL}/{endpoint}",
                params=params,
                headers=HEADERS,
                timeout=10,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"  ⚠️  Deezer: {e}")
            return {}

    def enrich_artist(self, artist: Artist) -> Artist:
        """Busca ID e dados extras do artista no Deezer."""
        data = self._get("search/artist", {"q": artist.name, "limit": 1})
        items = data.get("data", [])
        if items:
            artist.deezer_id = items[0].get("id")
        return artist

    def get_top_albums(self, artist: Artist, limit: int = 2) -> list[Album]:
        """Busca os melhores álbuns de um artista."""
        if not artist.deezer_id:
            self.enrich_artist(artist)

        if not artist.deezer_id:
            return self._search_albums(artist, limit)

        data = self._get(f"artist/{artist.deezer_id}/albums", {
            "limit": 20,
            "order": "RANKING",
        })

        albums = []
        seen_names = set()

        for item in data.get("data", []):
            record_type = item.get("record_type", "")
            if record_type not in ("album", ""):
                continue

            clean_name = re.sub(r'\s*[\(\[].*?[\)\]]\s*', '', item.get("title", "")).strip().lower()
            if clean_name in seen_names:
                continue
            seen_names.add(clean_name)

            albums.append(Album(
                name=item.get("title", "Desconhecido"),
                artist=artist.name,
                release_date=item.get("release_date", "N/A"),
                total_tracks=item.get("nb_tracks", 0),
                cover_url=item.get("cover_big"),
                deezer_url=item.get("link"),
            ))

        return albums[:limit]

    def _search_albums(self, artist: Artist, limit: int) -> list[Album]:
        """Fallback: busca álbuns via search."""
        data = self._get("search/album", {"q": artist.name, "limit": limit * 3})

        albums = []
        for item in data.get("data", []):
            album_artist = item.get("artist", {}).get("name", "")
            if artist.name.lower() not in album_artist.lower():
                continue
            albums.append(Album(
                name=item.get("title", "Desconhecido"),
                artist=artist.name,
                release_date="N/A",
                total_tracks=item.get("nb_tracks", 0),
                cover_url=item.get("cover_big"),
                deezer_url=item.get("link"),
            ))

        return albums[:limit]


# ============================================================
# 3. FALLBACK: CHARTS VIA DEEZER
# ============================================================

class ChartsFallback:
    """Busca charts brasileiros via Deezer se Spotify scraping falhar."""

    BASE_URL = "https://api.deezer.com"

    def get_top_brazil(self, limit: int = 10) -> list[Artist]:
        print("🔍 Buscando charts via Deezer API...")

        artist_counter = Counter()

        try:
            # Buscar playlists brasileiras no Deezer
            search = requests.get(
                f"{self.BASE_URL}/search/playlist",
                params={"q": "top brasil hits", "limit": 5},
                headers=HEADERS,
                timeout=10,
            )
            search.raise_for_status()
            playlists = search.json().get("data", [])

            for pl in playlists[:3]:
                pl_id = pl.get("id")
                if not pl_id:
                    continue
                tracks_resp = requests.get(
                    f"{self.BASE_URL}/playlist/{pl_id}/tracks",
                    params={"limit": 50},
                    headers=HEADERS,
                    timeout=10,
                )
                if tracks_resp.status_code == 200:
                    for track in tracks_resp.json().get("data", []):
                        name = track.get("artist", {}).get("name", "")
                        if name:
                            artist_counter[name] += 1
                time.sleep(0.3)

        except Exception as e:
            print(f"  ❌ Erro: {e}")
            return []

        artists = [
            Artist(name=name, track_count=count)
            for name, count in artist_counter.most_common(limit)
        ]

        if artists:
            print(f"  ✅ {len(artists)} artistas encontrados via Deezer!\n")
        return artists


# ============================================================
# 4. BUSCADOR DE LINKS DE DOWNLOAD
# ============================================================

class DownloadFinder:
    """Busca links de download e streaming em várias plataformas."""

    def find_all_links(self, album: Album) -> dict:
        links = {}
        query = f"{album.artist} {album.name}"
        encoded = quote(query)

        # Spotify search
        links["spotify"] = {
            "url": f"https://open.spotify.com/search/{quote(query)}",
            "type": "streaming",
            "nota": "Buscar no Spotify",
        }

        # Deezer (link direto se disponível)
        if album.deezer_url:
            links["deezer"] = {
                "url": album.deezer_url,
                "type": "streaming",
                "nota": f"Deezer - {album.name}",
            }

        # YouTube Music
        links["youtube_music"] = {
            "url": f"https://music.youtube.com/search?q={encoded}",
            "type": "streaming",
            "nota": "YouTube Music",
        }

        # SuaMusica
        sm_query = quote(album.artist)
        links["suamusica"] = {
            "url": f"https://www.suamusica.com.br/search?q={sm_query}",
            "type": "download",
            "nota": "SuaMusica - Download gratuito (artistas BR)",
        }

        # Apple Music
        links["apple_music"] = {
            "url": f"https://music.apple.com/br/search?term={encoded}",
            "type": "streaming",
            "nota": "Apple Music",
        }

        # Ferramentas de download CLI
        links["download_tools"] = {
            "spotdl": {
                "comando": f'spotdl download "{album.artist} {album.name}"',
                "install": "pip install spotdl",
                "nota": "Baixa do YouTube com metadata do Spotify",
                "url": "https://github.com/spotDL/spotify-downloader",
            },
            "yt_dlp": {
                "comando": f'yt-dlp -x --audio-format mp3 ytsearch10:"{album.artist} {album.name}"',
                "install": "pip install yt-dlp",
                "nota": "Download direto do YouTube em MP3",
                "url": "https://github.com/yt-dlp/yt-dlp",
            },
        }

        # Buscas manuais no Google
        yt_query = quote(f"{album.artist} {album.name} álbum completo")
        links["busca_manual"] = {
            "google": f"https://www.google.com/search?q={encoded}+download+mp3",
            "youtube": f"https://www.youtube.com/results?search_query={yt_query}",
        }

        return links


# ============================================================
# 5. RELATÓRIO
# ============================================================

def generate_report(artists: list[Artist]) -> str:
    lines = []
    lines.append("=" * 70)
    lines.append("🎵 TOP 10 CANTORES DO BRASIL - ÁLBUNS E DOWNLOADS")
    lines.append("=" * 70)
    lines.append("")

    for i, artist in enumerate(artists, 1):
        lines.append(f"{'─' * 65}")
        lines.append(f"  #{i}  {artist.name}")
        lines.append(f"       Aparições no Top 50: {artist.track_count}x")
        lines.append(f"{'─' * 65}")

        if not artist.top_albums:
            lines.append("  ❌ Nenhum álbum encontrado\n")
            continue

        for j, album in enumerate(artist.top_albums, 1):
            lines.append(f"\n  📀 Álbum {j}: {album.name}")
            lines.append(f"     Lançamento: {album.release_date}")
            lines.append(f"     Faixas: {album.total_tracks}")

            if album.download_links:
                lines.append(f"\n     🔗 Streaming:")
                for platform in ("spotify", "deezer", "youtube_music", "apple_music", "suamusica"):
                    info = album.download_links.get(platform)
                    if info:
                        tipo = info["type"]
                        lines.append(f"        [{tipo:^10}] {platform}: {info['nota']}")
                        lines.append(f"                    {info['url']}")

                tools = album.download_links.get("download_tools", {})
                if tools:
                    lines.append(f"\n     🛠️  Download via terminal:")
                    for tool, tinfo in tools.items():
                        lines.append(f"        • {tool}: {tinfo['nota']}")
                        lines.append(f"          $ {tinfo['comando']}")

                manual = album.download_links.get("busca_manual", {})
                if manual:
                    lines.append(f"\n     🔎 Busca manual:")
                    for name, url in manual.items():
                        lines.append(f"        • {name}: {url}")

        lines.append("")

    lines.append("=" * 70)
    lines.append("🛠️  GUIA RÁPIDO DE DOWNLOAD")
    lines.append("=" * 70)
    lines.append("")
    lines.append("  OPÇÃO 1 - spotdl (recomendado, mais fácil)")
    lines.append("    $ pip install spotdl")
    lines.append('    $ spotdl download "ARTISTA ALBUM"')
    lines.append('    $ spotdl download https://open.spotify.com/playlist/...')
    lines.append("")
    lines.append("  OPÇÃO 2 - yt-dlp (download do YouTube)")
    lines.append("    $ pip install yt-dlp")
    lines.append('    $ yt-dlp -x --audio-format mp3 "URL_DO_VIDEO"')
    lines.append("")
    lines.append("  OPÇÃO 3 - SuaMusica.com.br")
    lines.append("    Download gratuito direto pelo site")
    lines.append("    Foco em: sertanejo, forró, pagode, funk")
    lines.append("")
    lines.append("=" * 70)

    return "\n".join(lines)


def save_json(artists: list[Artist], filename: str = "resultado.json"):
    data = []
    for artist in artists:
        a_dict = {
            "rank": len(data) + 1,
            "name": artist.name,
            "aparicoes_top50": artist.track_count,
            "albums": [],
        }
        for album in artist.top_albums:
            album_dict = {
                "name": album.name,
                "release_date": album.release_date,
                "total_tracks": album.total_tracks,
                "deezer_url": album.deezer_url,
                "links": {},
                "cli_commands": {},
            }
            for k, v in album.download_links.items():
                if k == "download_tools":
                    album_dict["cli_commands"] = {
                        tool: info["comando"] for tool, info in v.items()
                    }
                elif k == "busca_manual":
                    album_dict["links"]["busca_manual"] = v
                else:
                    album_dict["links"][k] = v.get("url", "")
            a_dict["albums"].append(album_dict)
        data.append(a_dict)

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"💾 JSON salvo em {filename}")


# ============================================================
# 6. MAIN
# ============================================================

def main():
    print()
    print("🎵 Bot de Scraping - Top Músicas Brasil")
    print("   ✅ Sem API key necessária!")
    print("=" * 45)
    print()

    deezer = DeezerClient()
    finder = DownloadFinder()

    # ── Passo 1: Buscar Top 50 Brasil ──
    print("📊 PASSO 1/3: Buscando Top 50 Brasil...\n")

    raw_artists = []
    try:
        print(f"  Acessando {SPOTIFY_PLAYLIST_URL}...")
        resp = requests.get(SPOTIFY_PLAYLIST_URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        raw_artists = parse_spotify_page(resp.text)
        print(f"  Extraídos {len(raw_artists)} nomes de artistas do HTML")
    except Exception as e:
        print(f"  ⚠️  Spotify indisponível: {e}")

    if raw_artists:
        counter = Counter(raw_artists)
        artists = [
            Artist(name=name, track_count=count)
            for name, count in counter.most_common(10)
        ]
        print(f"  ✅ {len(artists)} artistas únicos identificados!")
    else:
        print("  Usando Deezer como fallback...")
        fallback = ChartsFallback()
        artists = fallback.get_top_brazil(limit=10)

    if not artists:
        print("\n❌ Não foi possível obter artistas. Verifique sua conexão.")
        return

    print(f"\n  🏆 Top {len(artists)}:")
    for i, a in enumerate(artists, 1):
        print(f"     {i:2}. {a.name} ({a.track_count}x)")

    # ── Passo 2: Buscar álbuns ──
    print(f"\n📀 PASSO 2/3: Buscando álbuns...\n")

    for artist in artists:
        print(f"  → {artist.name}...", end=" ", flush=True)
        artist.top_albums = deezer.get_top_albums(artist, limit=2)
        if artist.top_albums:
            names = " | ".join(a.name for a in artist.top_albums)
            print(f"✅ {names}")
        else:
            print("❌ sem álbuns")
        time.sleep(0.3)

    # ── Passo 3: Buscar links ──
    print(f"\n🔗 PASSO 3/3: Buscando links...\n")

    for artist in artists:
        for album in artist.top_albums:
            print(f"  → {album.artist} - {album.name}...", end=" ", flush=True)
            album.download_links = finder.find_all_links(album)
            print("✅")
            time.sleep(0.2)

    # ── Gerar output ──
    print()
    report = generate_report(artists)
    print(report)

    with open("relatorio.txt", "w", encoding="utf-8") as f:
        f.write(report)
    print("\n📄 Relatório salvo em relatorio.txt")

    save_json(artists)
    print("\n✅ Concluído!")


if __name__ == "__main__":
    main()