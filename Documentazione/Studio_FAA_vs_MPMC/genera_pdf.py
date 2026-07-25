#!/usr/bin/env python3
"""Generate relazione.pdf from the local groff-like source, using only Python."""

from pathlib import Path
import re
import sys
import textwrap


HERE = Path(__file__).resolve().parent
SOURCE = Path(sys.argv[1]) if len(sys.argv) > 1 else HERE / "relazione.ms"
OUTPUT = Path(sys.argv[2]) if len(sys.argv) > 2 else HERE / "relazione.pdf"
FOOTER = sys.argv[3] if len(sys.argv) > 3 else "Studio FAA-MPMC vs CAS-MPMC"
PAGE_W, PAGE_H = 595, 842  # A4 points
LEFT, TOP, BOTTOM = 48, 794, 48


def pdf_escape(text: str) -> bytes:
    raw = text.encode("cp1252", errors="replace")
    result = bytearray()
    for byte in raw:
        if byte in (40, 41, 92):
            result.extend(b"\\" + bytes([byte]))
        elif byte < 32 or byte > 126:
            result.extend(f"\\{byte:03o}".encode())
        else:
            result.append(byte)
    return bytes(result)


def parse_source() -> list[tuple[str, str]]:
    lines = SOURCE.read_text(encoding="utf-8").splitlines()
    blocks: list[tuple[str, str]] = []
    next_kind = "body"
    in_table = False
    in_display = False
    for line in lines:
        stripped = line.strip()
        if stripped == ".TL":
            next_kind = "title"
            continue
        if stripped == ".AU":
            next_kind = "author"
            continue
        if stripped == ".AI":
            next_kind = "meta"
            continue
        if stripped.startswith(".NH"):
            level = stripped.split()[-1] if len(stripped.split()) > 1 else "1"
            next_kind = "h1" if level == "1" else "h2"
            continue
        if stripped == ".TS":
            in_table = True
            continue
        if stripped == ".TE":
            in_table = False
            blocks.append(("space", ""))
            continue
        if stripped.startswith(".DS"):
            in_display = True
            continue
        if stripped == ".DE":
            in_display = False
            blocks.append(("space", ""))
            continue
        if stripped.startswith(".IP"):
            marker = "-"
            match = re.match(r"\.IP\s+([^ ]+)", stripped)
            if match and match.group(1).rstrip(".").isdigit():
                marker = match.group(1)
            next_kind = "bullet:" + marker
            continue
        if stripped in (".PP", ".AB", ".AE"):
            if stripped == ".PP":
                blocks.append(("space", ""))
            continue
        if stripped.startswith("."):
            continue
        if not stripped:
            blocks.append(("space", ""))
            continue
        if in_table:
            # Ignore tbl layout declarations, retain actual pipe-separated rows.
            if "|" not in stripped:
                continue
            blocks.append(("table", stripped))
            continue
        kind = "code" if in_display else next_kind
        blocks.append((kind, stripped.replace("\\(bu", "-")))
        if next_kind in ("title", "author", "meta", "h1", "h2") or next_kind.startswith("bullet:"):
            next_kind = "body"
    return blocks


def layout(blocks: list[tuple[str, str]]) -> list[list[tuple[float, float, str, str]]]:
    pages: list[list[tuple[float, float, str, str]]] = [[]]
    y = TOP

    def ensure(height: float) -> None:
        nonlocal y
        if y - height < BOTTOM:
            pages.append([])
            y = TOP

    def add_line(text: str, font: str, size: float, indent: float = 0, leading: float = 14) -> None:
        nonlocal y
        ensure(leading)
        pages[-1].append((LEFT + indent, y, font, f"{size}|{text}"))
        y -= leading

    for kind, text in blocks:
        if kind == "space":
            y -= 5
            continue
        if kind == "title":
            y -= 28
            for line in textwrap.wrap(text, 55):
                add_line(line, "F2", 20, 0, 25)
            y -= 12
            continue
        if kind in ("author", "meta"):
            add_line(text, "F3", 11, 0, 16)
            continue
        if kind == "h1":
            ensure(34)
            y -= 10
            add_line(text, "F2", 15, 0, 21)
            continue
        if kind == "h2":
            ensure(28)
            y -= 6
            add_line(text, "F2", 12, 0, 18)
            continue
        if kind == "table":
            columns = [part.strip() for part in text.split("|")]
            rendered = "  ".join(columns)
            for line in textwrap.wrap(rendered, 88, subsequent_indent="  "):
                add_line(line, "F4", 8.2, 2, 11)
            continue
        if kind == "code":
            for line in textwrap.wrap(text, 82, subsequent_indent="  "):
                add_line(line, "F4", 8.8, 12, 12)
            continue
        if kind.startswith("bullet:"):
            marker = kind.split(":", 1)[1]
            wrapped = textwrap.wrap(text, 88)
            for index, line in enumerate(wrapped):
                prefix = (marker + " ") if index == 0 else "  "
                add_line(prefix + line, "F1", 10.2, 12, 13.5)
            continue
        for line in textwrap.wrap(text, 94):
            add_line(line, "F1", 10.2, 0, 13.5)

    return pages


def build_pdf(pages: list[list[tuple[float, float, str, str]]]) -> bytes:
    objects: list[bytes] = []

    def add(obj: bytes) -> int:
        objects.append(obj)
        return len(objects)

    catalog_id = add(b"")
    pages_id = add(b"")
    font_regular = add(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>")
    font_bold = add(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold /Encoding /WinAnsiEncoding >>")
    font_italic = add(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Oblique /Encoding /WinAnsiEncoding >>")
    font_mono = add(b"<< /Type /Font /Subtype /Type1 /BaseFont /Courier /Encoding /WinAnsiEncoding >>")
    page_ids = []
    for page_number, page in enumerate(pages, 1):
        commands = bytearray()
        for x, y, font, payload in page:
            size_text, text = payload.split("|", 1)
            commands.extend(
                b"BT /" + font.encode() + b" " + size_text.encode() + b" Tf "
                + f"{x:.1f} {y:.1f} Td ".encode()
                + b"(" + pdf_escape(text) + b") Tj ET\n"
            )
        footer = f"{FOOTER}   -   pagina {page_number} di {len(pages)}"
        commands.extend(
            b"BT /F1 8 Tf 170 25 Td (" + pdf_escape(footer) + b") Tj ET\n"
        )
        content_id = add(
            f"<< /Length {len(commands)} >>\nstream\n".encode()
            + bytes(commands) + b"endstream"
        )
        page_id = add(
            (
                f"<< /Type /Page /Parent {pages_id} 0 R "
                f"/MediaBox [0 0 {PAGE_W} {PAGE_H}] "
                f"/Resources << /Font << /F1 {font_regular} 0 R /F2 {font_bold} 0 R "
                f"/F3 {font_italic} 0 R /F4 {font_mono} 0 R >> >> "
                f"/Contents {content_id} 0 R >>"
            ).encode()
        )
        page_ids.append(page_id)
    objects[catalog_id - 1] = f"<< /Type /Catalog /Pages {pages_id} 0 R >>".encode()
    kids = " ".join(f"{page_id} 0 R" for page_id in page_ids)
    objects[pages_id - 1] = f"<< /Type /Pages /Kids [{kids}] /Count {len(page_ids)} >>".encode()

    output = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]
    for object_id, obj in enumerate(objects, 1):
        offsets.append(len(output))
        output.extend(f"{object_id} 0 obj\n".encode() + obj + b"\nendobj\n")
    xref = len(output)
    output.extend(f"xref\n0 {len(objects) + 1}\n".encode())
    output.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        output.extend(f"{offset:010d} 00000 n \n".encode())
    output.extend(
        f"trailer\n<< /Size {len(objects) + 1} /Root {catalog_id} 0 R >>\n"
        f"startxref\n{xref}\n%%EOF\n".encode()
    )
    return bytes(output)


def main() -> None:
    blocks = parse_source()
    pages = layout(blocks)
    OUTPUT.write_bytes(build_pdf(pages))
    print(f"Creato {OUTPUT} ({len(pages)} pagine)")


if __name__ == "__main__":
    main()
