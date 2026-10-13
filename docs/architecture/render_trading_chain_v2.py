#!/usr/bin/env python3
"""Render the local V2 trading design Markdown as a standalone HTML page."""

from __future__ import annotations

import html
import re
from pathlib import Path


HERE = Path(__file__).resolve().parent
SOURCE = HERE / "TRADING_CHAIN_DESIGN_V2.md"
OUTPUT = HERE / "TRADING_CHAIN_DESIGN_V2.html"
INLINE = re.compile(r"(\[[^\]]+\]\([^)]+\)|`[^`]+`|\*\*[^*]+\*\*)")
HEADING = re.compile(r"^(#{1,3}) (.+)$")
ORDERED = re.compile(r"^\d+\. (.+)$")


def inline(value: str) -> str:
    parts = []
    for token in INLINE.split(value):
        if not token:
            continue
        if token.startswith("[") and "](" in token:
            label, target = token[1:-1].split("](", 1)
            parts.append(
                f'<a href="{html.escape(target, quote=True)}">'
                f"{html.escape(label)}</a>"
            )
        elif token.startswith("`"):
            parts.append(f"<code>{html.escape(token[1:-1])}</code>")
        elif token.startswith("**"):
            parts.append(f"<strong>{inline(token[2:-2])}</strong>")
        else:
            parts.append(html.escape(token))
    return "".join(parts)


def table_row(line: str) -> list[str]:
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def render(markdown: str) -> tuple[str, str, str]:
    lines = markdown.splitlines()
    title = lines[0].removeprefix("# ")
    headings = []
    for index, line in enumerate(lines):
        match = HEADING.match(line)
        if match and len(match.group(1)) > 1:
            headings.append((index, len(match.group(1)), match.group(2),
                             f"section-{len(headings) + 1}"))
    heading_ids = {index: identifier for index, _, _, identifier in headings}
    toc = "\n".join(
        f'<a class="level-{level}" href="#{identifier}">{html.escape(label)}</a>'
        for _, level, label, identifier in headings
    )

    blocks = []
    index = 1
    while index < len(lines):
        line = lines[index]
        if not line.strip():
            index += 1
            continue
        heading = HEADING.match(line)
        if heading:
            level = len(heading.group(1))
            identifier = heading_ids.get(index, "")
            blocks.append(f'<h{level} id="{identifier}">{inline(heading.group(2))}</h{level}>')
            index += 1
            continue
        if line.startswith("```"):
            language = line[3:].strip()
            index += 1
            code = []
            while index < len(lines) and not lines[index].startswith("```"):
                code.append(lines[index])
                index += 1
            if index == len(lines):
                raise ValueError("unclosed code fence")
            code_text = "\n".join(code)
            kind = "diagram" if "┌" in code_text else "code-block"
            blocks.append(
                f'<div class="code-card {kind}">'
                f'<div class="code-label">{html.escape(language or "text")}</div>'
                f'<pre><code>{html.escape(code_text)}</code></pre></div>'
            )
            index += 1
            continue
        if line.startswith("|"):
            rows = []
            while index < len(lines) and lines[index].startswith("|"):
                rows.append(table_row(lines[index]))
                index += 1
            if len(rows) < 2 or not all(re.fullmatch(r":?-+:?", cell)
                                           for cell in rows[1]):
                raise ValueError("invalid Markdown table")
            header = "".join(f"<th scope=\"col\">{inline(cell)}</th>" for cell in rows[0])
            body = "".join(
                "<tr>" + "".join(f"<td>{inline(cell)}</td>" for cell in row) + "</tr>"
                for row in rows[2:]
            )
            blocks.append(
                '<div class="table-wrap"><table><thead><tr>' + header +
                "</tr></thead><tbody>" + body + "</tbody></table></div>"
            )
            continue
        ordered = ORDERED.match(line)
        if ordered:
            items = []
            while index < len(lines):
                match = ORDERED.match(lines[index])
                if not match:
                    break
                items.append(f"<li>{inline(match.group(1))}</li>")
                index += 1
            blocks.append("<ol>" + "".join(items) + "</ol>")
            continue
        if line.startswith("- "):
            items = []
            while index < len(lines) and lines[index].startswith("- "):
                items.append(f"<li>{inline(lines[index][2:])}</li>")
                index += 1
            blocks.append("<ul>" + "".join(items) + "</ul>")
            continue
        paragraph = [line.strip()]
        index += 1
        while index < len(lines) and lines[index].strip() and not (
            HEADING.match(lines[index]) or lines[index].startswith(("```", "|", "- "))
            or ORDERED.match(lines[index])
        ):
            paragraph.append(lines[index].strip())
            index += 1
        blocks.append(f"<p>{inline(' '.join(paragraph))}</p>")

    return title, toc, "\n".join(blocks)


TEMPLATE = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="description" content="hquant 交易链路 V2：账户级订单跟踪、REST 与私有 WS 回报、双 SQLite 持久化和恢复。">
<title>__TITLE__</title>
<style>
:root{color-scheme:light dark;--bg:#f3f6f8;--paper:#fff;--ink:#17242c;--muted:#586b75;--line:#d9e2e6;--soft:#edf3f6;--accent:#185b74;--accent-soft:#dceff5;--shadow:0 12px 32px #18374810}
@media(prefers-color-scheme:dark){:root{--bg:#10191f;--paper:#19262e;--ink:#e5eef2;--muted:#adbec6;--line:#35454d;--soft:#21333d;--accent:#91cce0;--accent-soft:#22404c;--shadow:none}}
*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.7 system-ui,-apple-system,"PingFang SC","Microsoft YaHei",sans-serif}
.layout{max-width:1530px;margin:auto;display:grid;grid-template-columns:230px minmax(0,1fr);gap:32px;padding:30px 26px 90px}.sidebar{position:sticky;top:24px;align-self:start;max-height:calc(100vh - 48px);overflow:auto}.sidebar-label{font-size:11px;font-weight:800;letter-spacing:.14em;text-transform:uppercase;color:var(--muted);padding:0 10px 10px}.sidebar a{display:block;color:var(--muted);text-decoration:none;font-size:13px;line-height:1.45;padding:6px 10px;border-radius:7px}.sidebar a:hover{background:var(--soft);color:var(--accent)}.sidebar .level-2{font-weight:700;color:var(--ink);margin-top:5px}.sidebar .level-3{padding-left:22px}main{min-width:0;max-width:1190px}
.hero{padding:30px 36px;background:linear-gradient(135deg,var(--paper),var(--accent-soft));border:1px solid var(--line);border-radius:18px;box-shadow:var(--shadow)}.eyebrow{font:700 11px/1.3 ui-monospace,SFMono-Regular,Menlo,monospace;letter-spacing:.14em;text-transform:uppercase;color:var(--accent)}h1{font-size:clamp(28px,3.6vw,43px);line-height:1.22;margin:12px 0 15px}.hero p{color:var(--muted);margin:0}.hero-links{display:flex;flex-wrap:wrap;gap:10px;margin-top:20px}.hero-links a{color:var(--accent);background:var(--paper);border:1px solid var(--line);border-radius:999px;padding:5px 12px;text-decoration:none;font-size:12px;font-weight:700}.hero-links a:hover{text-decoration:underline}
article{padding:0 5px}article h2{font-size:clamp(23px,2.5vw,29px);line-height:1.3;scroll-margin-top:24px;margin:55px 0 18px;padding-top:25px;border-top:1px solid var(--line)}article h3{font-size:19px;scroll-margin-top:24px;margin:36px 0 14px}article p{max-width:100ch;margin:13px 0 19px}article a{color:var(--accent);text-underline-offset:3px}article code{font:.88em ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;background:var(--soft);padding:.1em .3em;border-radius:4px;overflow-wrap:anywhere}article ol,article ul{max-width:103ch;padding-left:1.7em}article li{margin:8px 0}
.table-wrap,.code-card{border:1px solid var(--line);border-radius:12px;background:var(--paper);box-shadow:var(--shadow);margin:19px 0 26px;overflow:auto}.table-wrap table{width:100%;border-collapse:collapse;font-size:13px;line-height:1.55}.table-wrap th,.table-wrap td{padding:11px 13px;text-align:left;vertical-align:top;border-bottom:1px solid var(--line)}.table-wrap th{background:var(--soft);white-space:nowrap;font-size:12px;color:var(--muted)}.table-wrap tr:last-child td{border-bottom:0}.table-wrap td:first-child{font-weight:650;min-width:135px}.code-label{padding:8px 15px;border-bottom:1px solid var(--line);font:700 10px ui-monospace,SFMono-Regular,Menlo,monospace;letter-spacing:.13em;text-transform:uppercase;color:var(--muted)}.code-card pre{margin:0;padding:17px 20px;overflow:auto}.code-card pre code{background:none;padding:0;white-space:pre;font:12px/1.6 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;color:var(--ink)}.diagram{border-left:4px solid var(--accent)}.diagram pre code{font-size:11.5px;line-height:1.45}
footer{border-top:1px solid var(--line);padding-top:18px;margin-top:65px;font-size:12px;color:var(--muted)}footer a{color:var(--accent)}@media(max-width:1050px){.layout{display:block;padding:16px 14px 65px}.sidebar{position:static;max-height:none;display:flex;flex-wrap:wrap;gap:2px;margin:0 0 16px}.sidebar-label{width:100%;padding-left:3px}.sidebar a{border:1px solid var(--line);background:var(--paper)}.sidebar .level-3{display:none}.hero{padding:24px}.table-wrap th,.table-wrap td{padding:9px 10px}}@media print{body{background:#fff}.layout{display:block;padding:0}.sidebar{display:none}.hero,.table-wrap,.code-card{box-shadow:none}article h2,article h3{break-after:avoid}.table-wrap,.code-card{break-inside:avoid}}
</style>
</head>
<body>
<div class="layout">
<nav class="sidebar" aria-label="文档目录"><div class="sidebar-label">V2 设计目录</div>__TOC__</nav>
<main>
<header class="hero"><div class="eyebrow">hquant / Trading Chain / V2</div><h1>__TITLE__</h1><p>账户级订单跟踪、资金占用、可靠日志与外部交易所故障恢复的设计及本机实施记录。</p><div class="hero-links"><a href="TRADING_CHAIN_DESIGN.html">查看 V1 HTML</a><a href="TRADING_CHAIN_DESIGN_V2.md">查看 V2 Markdown</a><a href="#section-1">与 V1 的区别</a></div></header>
<article>__BODY__</article>
<footer>本页由 <a href="TRADING_CHAIN_DESIGN_V2.md">TRADING_CHAIN_DESIGN_V2.md</a> 生成；以 Markdown 源文件为准。可离线打开。</footer>
</main>
</div>
</body>
</html>
"""


def main() -> None:
    title, toc, body = render(SOURCE.read_text(encoding="utf-8"))
    page = (TEMPLATE.replace("__TITLE__", html.escape(title))
            .replace("__TOC__", toc).replace("__BODY__", body))
    OUTPUT.write_text(page, encoding="utf-8")
    print(OUTPUT)


if __name__ == "__main__":
    main()
