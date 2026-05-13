import logging
import mimetypes

from pathlib import Path
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, Response
from jinja2 import Environment, FileSystemLoader
from airflow.api_fastapi.core_api.security import requires_authenticated

from .config import STATIC_FILES_EMBED_PARAM, STATIC_FILES_INLINE_MIMES, STATIC_FILES_ROOT
from .helpers import static_file_path


logger = logging.getLogger(__name__)


files_router = APIRouter(
    tags=["Static Files"],
    dependencies=[Depends(requires_authenticated())],
)


_templates = Environment(
    loader=FileSystemLoader(Path(__file__).parent / "templates"),
    autoescape=True,
)


def _entry_href(entry: Path, embed: bool) -> tuple[str, str]:
    """
    Returns link info (href, label) for a given directory entry given embedding status
    """
    label = entry.name + ("/" if entry.is_dir() else "")
    href = quote(label, safe="/")
    if embed:
        href += f"?{STATIC_FILES_EMBED_PARAM}=1"
    return href, label


def _get_mime(path: Path, embed: bool) -> str:
    """
    Returns the MIME type for the file based on whether it's embedded in AirFlow's sandboxed iframe

    Downloads are disabled in the sandbox and browsers don't always respect Content-Disposition for
    some filetypes. Work around that by returning text/plain when embedded and returning a file
    that the browser would normally force to download.
    """
    mime = mimetypes.guess_type(path.name)[0] or "text/plain"
    if embed and not mime.startswith(STATIC_FILES_INLINE_MIMES):
        return "text/plain"
    return mime


def _safe_entries(path: Path) -> list[Path]:
    """
    Return iterdir() entries that are safe to surface in a listing.

    Filters out broken links and links that resolve outside of STATIC_FILES_ROOT.
    """
    safe: list[Path] = []
    for entry in path.iterdir():
        try:
            resolved = entry.resolve(strict=True)
        except (OSError, RuntimeError) as e:
            logger.warning("Skipping unreadable listing entry %s: %s", entry, e)
            continue
        if not resolved.is_relative_to(STATIC_FILES_ROOT):
            logger.warning("Skipping out-of-root listing entry %s -> %s", entry, resolved)
            continue
        safe.append(entry)
    return safe


def _render_listing(path: Path, embed: bool) -> HTMLResponse:
    """
    Render an HTML directory listing of ``path``, with subdirectories first.

    Entries that escape STATIC_FILES_ROOT or fail to stat are dropped — see
    ``_safe_entries``.
    """
    subpath = path.relative_to(STATIC_FILES_ROOT).as_posix()
    title = f"/{subpath}" if subpath != "." else "/"

    entries = _safe_entries(path)

    def sort_key(p: Path) -> tuple[bool, str]:
        try:
            is_file = not p.is_dir()
        except OSError:
            is_file = True
        return (is_file, p.name)

    sorted_entries = sorted(entries, key=sort_key)

    return HTMLResponse(
        _templates.get_template("index.html").render(
            title=title,
            entries=[_entry_href(e, embed) for e in sorted_entries],
        )
    )


@files_router.get("/")
@files_router.get("/{subpath:path}")
def serve_static_file(subpath: str = "", embed: bool = False) -> Response:
    """
    Serves a file or directory listing for the given path

    If subpath points to a directory and that directory contains an index.html
    file it returns that file, otherwise it serves a constructed directory listing.

    Query Params:
    - embed: When true (1, on, true, True, yes), files that the browser would otherwise
      download are returned as text/plain to force them to render inline.
    """
    try:
        path = static_file_path(subpath)
    except ValueError:
        raise HTTPException(status_code=403, detail="Path resolves outside the storage root")

    if path.is_dir():
        try:
            # Don't serve an index.html that symlinked outside of the storage root
            index = static_file_path((path.relative_to(STATIC_FILES_ROOT) / "index.html").as_posix())
        except ValueError:
            raise HTTPException(status_code=403, detail="Path resolves outside the storage root")

        if index.is_file():
            return FileResponse(index)
        return _render_listing(path, embed)

    if path.is_file():
        return FileResponse(path, media_type=_get_mime(path, embed))

    raise HTTPException(status_code=404, detail=f"Not found: /{subpath}")
