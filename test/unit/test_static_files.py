"""Unit tests for the static_files plugin.

Fixtures provision tmpdirs under the configured ``STATIC_FILES_ROOT`` (typically
``/opt/airflow/files`` -> host ``./files``) rather than under pytest's
container-local ``tmp_path``. Tests may run from a container separate from the
one serving HTTP requests; the shared mount is the only filesystem location
both can see. Plugin config is read as-is — no monkeypatching of module-level
constants.
"""

import shutil
import tempfile
import uuid

from collections.abc import Iterator
from pathlib import Path

import pytest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from mokelumne.plugins.static_files import config as sf_config
from mokelumne.plugins.static_files import routes as sf_routes
from mokelumne.plugins.static_files.helpers import (
    static_file_path,
    static_files_root,
    static_files_run_dir,
    static_path_to_url,
)


@pytest.fixture
def fixture_dir() -> Iterator[tuple[str, Path]]:
    """Per-test empty dir under STATIC_FILES_ROOT.

    Yields ``(subpath_under_root, absolute_path)``. The subpath is suitable for
    addressing the dir via the static-files routes (e.g. ``GET /<subpath>/``).
    """
    sf_config.STATIC_FILES_ROOT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=sf_config.STATIC_FILES_ROOT, prefix="pytest-") as d:
        path = Path(d)
        yield path.relative_to(sf_config.STATIC_FILES_ROOT).as_posix(), path


@pytest.fixture
def outside_dir() -> Iterator[Path]:
    """Per-test dir located outside STATIC_FILES_ROOT, used by escape tests."""
    base = sf_config.STATIC_FILES_ROOT.parent / "test-outside"
    base.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=base, prefix="pytest-") as d:
        yield Path(d)


@pytest.fixture
def test_client() -> TestClient:
    """TestClient mounting only ``files_router`` with auth stubbed out."""
    app = FastAPI()
    app.include_router(sf_routes.files_router)
    for dep in sf_routes.files_router.dependencies:
        app.dependency_overrides[dep.dependency] = lambda: None
    return TestClient(app)


class TestStaticFilePath:
    """Tests for the path-resolution helper."""

    def test_empty_subpath_returns_root(self):
        assert static_file_path("") == sf_config.STATIC_FILES_ROOT

    def test_resolves_nested_subpath(self, fixture_dir):
        sub, root = fixture_dir
        nested = root / "a" / "b"
        nested.mkdir(parents=True)
        assert static_file_path(f"{sub}/a/b") == nested

    def test_traversal_dotdot_raises(self):
        with pytest.raises(ValueError):
            static_file_path("../etc/passwd")

    def test_absolute_escape_raises(self):
        with pytest.raises(ValueError):
            static_file_path("/etc/passwd")

    def test_symlink_escape_raises(self, fixture_dir, outside_dir):
        _, root = fixture_dir
        (outside_dir / "secret.txt").write_text("nope")
        (root / "link").symlink_to(outside_dir / "secret.txt")
        sub = root.relative_to(sf_config.STATIC_FILES_ROOT).as_posix()
        with pytest.raises(ValueError):
            static_file_path(f"{sub}/link")


class TestStaticFilesRoot:
    """Tests for the files-root accessor."""

    def test_returns_root(self):
        assert static_files_root() == sf_config.STATIC_FILES_ROOT


class TestStaticFilesRunDir:
    """Tests for the per-DAG-run directory helper."""

    def test_builds_dag_run_path(self):
        # Use a unique dag/run id so concurrent test runs don't collide,
        # then clean up after ourselves.
        dag_id = f"pytest-{uuid.uuid4().hex[:8]}"
        run_id = "runY"
        try:
            result = static_files_run_dir(dag_id, run_id)
            assert result == sf_config.STATIC_FILES_ROOT / dag_id / run_id
            assert result.is_dir()
        finally:
            shutil.rmtree(sf_config.STATIC_FILES_ROOT / dag_id, ignore_errors=True)


class TestStaticPathToUrl:
    """Tests for the path-to-URL helper."""

    def test_builds_url(self, fixture_dir):
        sub, root = fixture_dir
        result = static_path_to_url(root / "a" / "b.txt")
        expected_base = sf_config.STATIC_FILES_BASE_URL.rstrip("/")
        expected_prefix = sf_config.STATIC_FILES_URL_PREFIX.strip("/")
        assert result == f"{expected_base}/{expected_prefix}/{sub}/a/b.txt"

    def test_appends_embed_query_when_true(self, fixture_dir):
        sub, root = fixture_dir
        result = static_path_to_url(root / "x.txt", embed=True)
        expected_base = sf_config.STATIC_FILES_BASE_URL.rstrip("/")
        expected_prefix = sf_config.STATIC_FILES_URL_PREFIX.strip("/")
        assert result == f"{expected_base}/{expected_prefix}/{sub}/x.txt?embed=1"

    def test_omits_embed_query_when_false(self, fixture_dir):
        _, root = fixture_dir
        result = static_path_to_url(root / "x.txt", embed=False)
        assert "embed" not in result


class TestServeStaticFile:
    """Tests for the serve_static_file route via TestClient."""

    def test_empty_dir_renders_no_files(self, test_client, fixture_dir):
        sub, _ = fixture_dir
        response = test_client.get(f"/{sub}/")
        assert response.status_code == 200
        assert "No files." in response.text
        assert f"<title>/{sub}</title>" in response.text

    def test_lists_dirs_before_files_with_escaped_hrefs(self, test_client, fixture_dir):
        sub, root = fixture_dir
        (root / "a-file.txt").write_text("hi")
        (root / "z-dir").mkdir()
        (root / 'name with "quotes".txt').write_text("x")

        response = test_client.get(f"/{sub}/")
        assert response.status_code == 200
        body = response.text

        # subdir listed first (dirs sort before files), with trailing slash
        assert body.index("z-dir/") < body.index("a-file.txt")
        # quote in filename is HTML-escaped in href
        assert "&quot;" in body or "&#34;" in body

    def test_special_chars_in_filename_are_url_encoded_in_href(self, test_client, fixture_dir):
        sub, root = fixture_dir
        (root / "report?v2 final.txt").write_text("x")

        response = test_client.get(f"/{sub}/")
        assert response.status_code == 200
        body = response.text

        # href is percent-encoded so '?' and space don't break the link
        assert "report%3Fv2%20final.txt" in body
        # raw filename does not appear inside an href attribute
        assert 'href="report?v2 final.txt"' not in body

    def test_text_file_served_as_text_plain(self, test_client, fixture_dir):
        sub, root = fixture_dir
        (root / "hello.txt").write_text("hello world\n")
        response = test_client.get(f"/{sub}/hello.txt")
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/plain")
        assert response.text == "hello world\n"

    def test_csv_served_with_csv_mime_in_standalone_mode(self, test_client, fixture_dir):
        sub, root = fixture_dir
        (root / "data.csv").write_text("a,b\n1,2\n")
        response = test_client.get(f"/{sub}/data.csv")
        assert response.status_code == 200
        # Standalone (non-embedded) responses preserve the guessed MIME so the
        # browser can decide whether to render or download. Browsers treat
        # text/csv as a download by default — no explicit Content-Disposition
        # is set.
        assert response.headers["content-type"].startswith("text/csv")

    def test_csv_downgraded_to_text_plain_when_embedded(self, test_client, fixture_dir):
        sub, root = fixture_dir
        (root / "data.csv").write_text("a,b\n1,2\n")
        response = test_client.get(f"/{sub}/data.csv?embed=1")
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/plain")
        # No download disposition in embed mode
        assert "attachment" not in response.headers.get("content-disposition", "")

    def test_image_kept_renderable_when_embedded(self, test_client, fixture_dir):
        sub, root = fixture_dir
        # Minimal PNG header bytes — content doesn't matter, only the extension
        # drives mimetypes.guess_type.
        (root / "pic.png").write_bytes(b"\x89PNG\r\n\x1a\n")
        response = test_client.get(f"/{sub}/pic.png?embed=1")
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("image/png")

    def test_directory_with_index_serves_index(self, test_client, fixture_dir):
        sub, root = fixture_dir
        nested = root / "child"
        nested.mkdir()
        (nested / "index.html").write_text("<p>indexed</p>")
        response = test_client.get(f"/{sub}/child")
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/html")
        assert "<p>indexed</p>" in response.text

    def test_directory_without_index_renders_listing(self, test_client, fixture_dir):
        sub, root = fixture_dir
        nested = root / "child"
        nested.mkdir()
        (nested / "a.txt").write_text("x")
        response = test_client.get(f"/{sub}/child")
        assert response.status_code == 200
        assert "a.txt" in response.text
        assert f"<title>/{sub}/child</title>" in response.text

    def test_missing_path_returns_404(self, test_client, fixture_dir):
        sub, _ = fixture_dir
        response = test_client.get(f"/{sub}/missing.txt")
        assert response.status_code == 404

    def test_symlink_outside_root_returns_403(self, test_client, fixture_dir, outside_dir):
        sub, root = fixture_dir
        (outside_dir / "secret.txt").write_text("nope")
        (root / "escape").symlink_to(outside_dir / "secret.txt")
        response = test_client.get(f"/{sub}/escape")
        assert response.status_code == 403

    def test_listing_omits_symlinks_pointing_outside_root(
        self, test_client, fixture_dir, outside_dir
    ):
        sub, root = fixture_dir
        (outside_dir / "secret.txt").write_text("nope")
        (root / "safe.txt").write_text("ok")
        (root / "escape").symlink_to(outside_dir / "secret.txt")

        response = test_client.get(f"/{sub}/")
        assert response.status_code == 200
        assert "safe.txt" in response.text
        # The out-of-root symlink name must not be surfaced in the listing.
        assert "escape" not in response.text

    def test_listing_skips_broken_symlinks_without_500(
        self, test_client, fixture_dir
    ):
        sub, root = fixture_dir
        (root / "ok.txt").write_text("ok")
        (root / "dangling").symlink_to(root / "does-not-exist")

        response = test_client.get(f"/{sub}/")
        assert response.status_code == 200
        assert "ok.txt" in response.text
        assert "dangling" not in response.text

    def test_auth_dependency_is_wired(self):
        """Sanity check: without the override, the router rejects requests.

        Confirms the unit-test bypass is real (auth would actually gate the
        route in production), not a side effect of accidentally-permissive
        defaults.
        """
        app = FastAPI()
        app.include_router(sf_routes.files_router)
        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/")
        # Without an auth manager configured, the dependency raises — the exact
        # status code depends on Airflow's wiring, but it must not be a 200.
        assert response.status_code != 200
