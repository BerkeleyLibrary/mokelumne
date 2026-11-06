"""PyTest cases for the shared template renderer."""

import pytest
from jinja2 import TemplateNotFound, UndefinedError

from mokelumne import templates


class TestRender:
    """Tests for the shared renderer."""

    def test_missing_template_raises(self):
        """Raise when the template does not exist."""
        with pytest.raises(TemplateNotFound):
            templates.render("nope/missing.html")

    def test_missing_variable_raises(self):
        """Raise when a template variable is not supplied."""
        with pytest.raises(UndefinedError):
            templates.render("pdf_creation/results.html")
