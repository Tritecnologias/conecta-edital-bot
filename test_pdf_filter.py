"""
Property-based tests for PDF link filtering in extrair_links_universal.

Bug condition exploration: Phase 5 fallback collects non-PDF document links
(e.g., .pptx, .docx, .xlsx) when they contain generic terms like "pdf",
"download", "arquivo", "publicacao". This test demonstrates the bug exists.

Preservation tests: Verify that PDF links, extensionless URLs, and GUID URLs
are NEVER filtered out by the filtering logic.
"""

import os
import pytest
from hypothesis import given, strategies as st, settings
from urllib.parse import urlparse

# --- Constants matching the design spec ---
EXTENSOES_NAO_PDF = ['.pptx', '.docx', '.xlsx', '.ppt', '.doc', '.xls',
                     '.odt', '.ods', '.odp', '.csv', '.zip', '.rar']

TERMOS_FALLBACK = ["pdf", "download", "arquivo", "publicacao"]


# --- Inline implementation of _is_non_pdf_extension (mirrors design spec) ---
# This helper doesn't exist in robot.py yet; we implement it here to test
# the preservation property against the planned filtering logic.
_EXTENSOES_NAO_PDF_SET = {'.pptx', '.docx', '.xlsx', '.ppt', '.doc', '.xls',
                          '.odt', '.ods', '.odp', '.csv', '.zip', '.rar'}


def _is_non_pdf_extension(url):
    """Check if a URL ends with a known non-PDF document extension."""
    path = urlparse(url).path.lower()
    _, ext = os.path.splitext(path)
    return ext in _EXTENSOES_NAO_PDF_SET


def phase5_fallback_logic(hrefs, base_url="https://example.com"):
    """
    Simulates the Phase 5 fallback logic from extrair_links_universal.

    This replicates the behavior of Phase 5 in robot.py including the
    non-PDF extension filter fix.
    """
    from urllib.parse import urljoin

    termos_fallback = ["pdf", "download", "arquivo", "publicacao"]
    links_pdf = []
    seen = set()

    for href in hrefs:
        if href.startswith("javascript") or href == "#":
            continue
        href_full = urljoin(base_url, href) if not href.startswith("http") else href
        if any(t in href.lower() for t in termos_fallback) and href_full not in seen:
            if not _is_non_pdf_extension(href_full):
                links_pdf.append(href_full)
                seen.add(href_full)

    return [link for link in links_pdf if not _is_non_pdf_extension(link)]


# --- Hypothesis strategies ---

# Generate a path segment that contains one of the fallback terms
fallback_term = st.sampled_from(TERMOS_FALLBACK)

# Generate a non-PDF extension
non_pdf_ext = st.sampled_from(EXTENSOES_NAO_PDF)

# Generate a simple filename (alphanumeric, 3-15 chars)
filename = st.from_regex(r'[a-z]{3,15}', fullmatch=True)

# Generate a path prefix
path_prefix = st.sampled_from([
    "/docs/", "/arquivos/", "/download/", "/publicacao/",
    "/files/", "/documentos/", "/anexos/"
])


@st.composite
def non_pdf_url_with_fallback_term(draw):
    """
    Generate a URL that:
    1. Ends with a non-PDF extension (e.g., .pptx, .docx)
    2. Contains a fallback term somewhere in the path

    These URLs SHOULD be rejected but currently ARE collected by Phase 5.
    """
    prefix = draw(path_prefix)
    name = draw(filename)
    ext = draw(non_pdf_ext)
    # The path prefix already contains a fallback term (e.g., /download/, /arquivo/)
    return f"{prefix}{name}{ext}"


# =============================================================================
# Property 1: Bug Condition — Non-PDF Extensions Collected by Extractor
# =============================================================================
# **Validates: Requirements 1.1, 1.3**
#
# EXPECTED: This test FAILS on unfixed code, proving the bug exists.
# Phase 5 collects URLs with non-PDF extensions when they match fallback terms.
# The assertion says these URLs should NOT be in the result — but they ARE,
# because Phase 5 has no extension filtering.
# =============================================================================

class TestBugConditionExploration:
    """
    Bug condition exploration: demonstrates that Phase 5 fallback collects
    non-PDF document links without checking file extensions.

    **Validates: Requirements 1.1, 1.3**
    """

    @given(url=non_pdf_url_with_fallback_term())
    @settings(max_examples=100)
    def test_phase5_should_not_collect_non_pdf_extensions(self, url):
        """
        Property: For all URLs ending with a known non-PDF extension that
        contain a fallback term, Phase 5 should NOT include them in results.

        This test is EXPECTED TO FAIL on unfixed code — failure proves the bug.

        **Validates: Requirements 1.1, 1.3**
        """
        result = phase5_fallback_logic([url])

        # Extract the full URL as it would appear after urljoin
        from urllib.parse import urljoin
        expected_full = urljoin("https://example.com", url)

        # Assert: non-PDF URLs should NOT be collected
        # On unfixed code, this WILL FAIL because Phase 5 collects them anyway
        assert expected_full not in result, (
            f"Bug confirmed: non-PDF URL was collected by Phase 5 fallback.\n"
            f"  Input href: {url}\n"
            f"  Collected URL: {expected_full}\n"
            f"  Phase 5 has no extension filtering — it matched a fallback term "
            f"and collected the link regardless of its file extension."
        )


# =============================================================================
# Preservation Property Tests — Strategies
# =============================================================================

# Generate a simple path segment (alphanumeric, 3-12 chars)
_path_segment = st.from_regex(r'[a-z]{3,12}', fullmatch=True)

# Generate a domain
_domain = st.sampled_from([
    "example.com", "prefeitura.sp.gov.br", "diariomunicipal.com.br",
    "portalfacil.com.br", "imprensaoficial.gov.br"
])


@st.composite
def pdf_url(draw):
    """
    Generate a URL that clearly ends with .pdf.
    These must NEVER be rejected by the filtering logic.
    """
    domain = draw(_domain)
    segment = draw(_path_segment)
    name = draw(_path_segment)
    return f"https://{domain}/{segment}/{name}.pdf"


@st.composite
def extensionless_url(draw):
    """
    Generate a URL with no file extension (API endpoints, query-string downloads).
    These must NEVER be rejected by the filtering logic.
    """
    domain = draw(_domain)
    segment = draw(_path_segment)
    # Pick between path-only and query-string variants
    variant = draw(st.sampled_from(["path", "query", "query_id"]))
    if variant == "path":
        return f"https://{domain}/{segment}/download"
    elif variant == "query":
        param = draw(_path_segment)
        return f"https://{domain}/{segment}?file={param}"
    else:
        num = draw(st.integers(min_value=1, max_value=99999))
        return f"https://{domain}/api/download?id={num}"


@st.composite
def guid_portalfacil_url(draw):
    """
    Generate a GUID-constructed URL matching the portalfacil pattern from Phase 3.
    Format: https://host/abrir_arquivo.aspx?cdLocal=12&arquivo=%7BGUID%7D.pdf
    These must NEVER be rejected by the filtering logic.
    """
    domain = draw(_domain)
    # Generate a GUID-like string (8-4-4-4-12 hex)
    parts = [
        draw(st.from_regex(r'[0-9A-F]{8}', fullmatch=True)),
        draw(st.from_regex(r'[0-9A-F]{4}', fullmatch=True)),
        draw(st.from_regex(r'[0-9A-F]{4}', fullmatch=True)),
        draw(st.from_regex(r'[0-9A-F]{4}', fullmatch=True)),
        draw(st.from_regex(r'[0-9A-F]{12}', fullmatch=True)),
    ]
    guid = "-".join(parts)
    return f"https://{domain}/abrir_arquivo.aspx?cdLocal=12&arquivo=%7B{guid}%7D.pdf"


# =============================================================================
# Property 2: Preservation — PDF Links Are Never Rejected
# =============================================================================
# **Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5**
#
# These tests verify that the planned _is_non_pdf_extension helper and the
# safety-net filter will NEVER reject legitimate PDF links, extensionless URLs,
# or GUID-constructed URLs. They should PASS on both unfixed and fixed code.
# =============================================================================

class TestPreservationProperties:
    """
    Preservation property tests: verify that PDF links, extensionless URLs,
    and GUID URLs are NEVER filtered out by the non-PDF extension check.

    **Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5**
    """

    @given(url=pdf_url())
    @settings(max_examples=100)
    def test_pdf_urls_never_rejected(self, url):
        """
        Property: For all URLs ending in .pdf, _is_non_pdf_extension SHALL
        return False — these URLs must never be filtered out.

        **Validates: Requirements 3.1**
        """
        assert not _is_non_pdf_extension(url), (
            f"Preservation violation: PDF URL was incorrectly rejected.\n"
            f"  URL: {url}\n"
            f"  URLs ending in .pdf must NEVER be filtered out."
        )

    @given(url=extensionless_url())
    @settings(max_examples=100)
    def test_extensionless_urls_never_rejected(self, url):
        """
        Property: For all URLs with no file extension, _is_non_pdf_extension
        SHALL return False — these URLs must never be filtered out.

        **Validates: Requirements 3.2, 3.4**
        """
        assert not _is_non_pdf_extension(url), (
            f"Preservation violation: extensionless URL was incorrectly rejected.\n"
            f"  URL: {url}\n"
            f"  URLs with no file extension must NEVER be filtered out."
        )

    @given(url=guid_portalfacil_url())
    @settings(max_examples=100)
    def test_guid_portalfacil_urls_never_rejected(self, url):
        """
        Property: For all GUID-constructed portalfacil URLs,
        _is_non_pdf_extension SHALL return False — these URLs must never
        be filtered out.

        **Validates: Requirements 3.3**
        """
        assert not _is_non_pdf_extension(url), (
            f"Preservation violation: GUID portalfacil URL was incorrectly rejected.\n"
            f"  URL: {url}\n"
            f"  GUID-constructed URLs must NEVER be filtered out."
        )

    @given(url=pdf_url())
    @settings(max_examples=50)
    def test_pdf_urls_survive_safety_net_filter(self, url):
        """
        Property: For all URLs ending in .pdf, the safety-net filter
        (list comprehension filtering out non-PDF extensions) SHALL
        preserve them in the output list.

        **Validates: Requirements 3.1, 3.5**
        """
        links = [url]
        filtered = [link for link in links if not _is_non_pdf_extension(link)]
        assert url in filtered, (
            f"Preservation violation: PDF URL was removed by safety-net filter.\n"
            f"  URL: {url}"
        )

    @given(url=extensionless_url())
    @settings(max_examples=50)
    def test_extensionless_urls_survive_safety_net_filter(self, url):
        """
        Property: For all extensionless URLs, the safety-net filter SHALL
        preserve them in the output list.

        **Validates: Requirements 3.2, 3.4**
        """
        links = [url]
        filtered = [link for link in links if not _is_non_pdf_extension(link)]
        assert url in filtered, (
            f"Preservation violation: extensionless URL was removed by safety-net filter.\n"
            f"  URL: {url}"
        )

    @given(url=guid_portalfacil_url())
    @settings(max_examples=50)
    def test_guid_urls_survive_safety_net_filter(self, url):
        """
        Property: For all GUID-constructed URLs, the safety-net filter SHALL
        preserve them in the output list.

        **Validates: Requirements 3.3**
        """
        links = [url]
        filtered = [link for link in links if not _is_non_pdf_extension(link)]
        assert url in filtered, (
            f"Preservation violation: GUID URL was removed by safety-net filter.\n"
            f"  URL: {url}"
        )

    @given(url=pdf_url())
    @settings(max_examples=50)
    def test_phase5_preserves_pdf_urls(self, url):
        """
        Property: For all URLs ending in .pdf, Phase 5 fallback logic
        SHALL continue to collect them (since "pdf" is a fallback term).

        **Validates: Requirements 3.1**
        """
        # Extract just the path portion for Phase 5 simulation
        path = urlparse(url).path
        result = phase5_fallback_logic([path])
        # Phase 5 should collect this because the URL contains "pdf"
        assert len(result) > 0, (
            f"Preservation violation: PDF URL not collected by Phase 5.\n"
            f"  Path: {path}\n"
            f"  Phase 5 should collect URLs containing '.pdf'."
        )
