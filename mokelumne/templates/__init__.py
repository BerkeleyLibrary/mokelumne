"""Shared Jinja2 rendering for Dag-produced reports.

Templates live in per-Dag subfolders next to this module, for example
``pdf_creation/results.html``, and are addressed relative to this directory.
"""

from collections.abc import Mapping
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined

_env = Environment(
    loader=FileSystemLoader(Path(__file__).parent),
    autoescape=True,
    undefined=StrictUndefined,
)


def render(template_name: str, **context: object) -> str:
    """Render a template with HTML autoescaping.

    :param template_name: Template path relative to this package, e.g.
        ``pdf_creation/results.html``.
    :type template_name: str
    :param **context: Variables made available to the template.
    :type **context: object
    :returns: The rendered document.
    :rtype: str
    :raises jinja2.TemplateNotFound: If the template does not exist.
    :raises jinja2.UndefinedError: If the template uses a variable not in ``context``.
    """
    return _env.get_template(template_name).render(**context)
