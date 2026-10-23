#!/usr/bin/env python3
"""Smoke test giao diện **bản Vue** (`/`) — thay cho smoke_ui.py cũ của giao diện vanilla.

Vì sao vẫn cần: UI là chỗ duy nhất người dùng thao tác; lỗi build/SSE/selector chỉ lộ ra khi mở
trình duyệt thật. Test này chạy Chromium headless, KHÔNG cần token thật:

  python3 web/smoke_ui.py                      # http://127.0.0.1:8099, 3 token giả
  python3 web/smoke_ui.py --url http://... --tokens 5 --shot /tmp/ui.png

Nó dùng token giả + proxy chết (127.0.0.1:1) để job fail ngay, nên không tiêu proxy thật và
không phụ thuộc pool. Kiểm: trang tải sạch (không pageerror/404), CSS áp, sidebar render, bấm
Run tạo job, task card hiện ra, tab lọc + đổi số cột hoạt động, đổi theme được.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import sys
import time


def fake_jwt(email: str) -> str:
    def b64(d: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(d).encode()).rstrip(b"=").decode()

    return (f"{b64({'alg': 'RS256'})}."
            f"{b64({'https://api.openai.com/profile': {'email': email}, 'exp': 9999999999})}.sig")


async def run(url: str, n_tokens: int, shot: str | None) -> int:
    from playwright.async_api import async_playwright

    checks: list[tuple[str, bool, str]] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        checks.append((name, bool(ok), detail))
        print(f"  {'✓' if ok else '✗'} {name}" + (f" — {detail}" if detail and not ok else ""))

    async with async_playwright() as p:
        launch: dict = {"args": ["--no-sandbox"]}
        for exe in ("/usr/bin/chromium", "/usr/bin/google-chrome-stable",
                    "/usr/bin/google-chrome",
                    os.path.expanduser("~/.cache/ms-playwright/chromium-1217/chrome-linux64/chrome")):
            if os.path.exists(exe):
                launch["executable_path"] = exe
                break
        browser = await p.chromium.launch(**launch)
        page = await browser.new_page(viewport={"width": 1600, "height": 1000})
        errors: list[str] = []
        bad: list[str] = []
        page.on("pageerror", lambda e: errors.append(str(e)))
        page.on("response", lambda r: bad.append(f"{r.status} {r.url}") if r.status >= 400 else None)

        print("1. Tải trang + asset")
        await page.goto(url + "/", wait_until="domcontentloaded", timeout=45000)
        await page.evaluate("() => localStorage.clear()")
        await page.reload(wait_until="networkidle")
        await page.wait_for_timeout(1500)
        check("không pageerror", not errors, "; ".join(errors)[:160])
        check("không asset 4xx/5xx", not bad, "; ".join(bad)[:160])
        bg = await page.evaluate("() => getComputedStyle(document.body).backgroundColor")
        check("CSS đã áp", bg not in ("rgba(0, 0, 0, 0)", "transparent"), bg)
        check("sidebar có ô token", await page.locator("#tokens").count() == 1)
        check("nút Run tồn tại", await page.locator("#submit").count() == 1)
        check("có công tắc lọc token", await page.locator("#prefilter").count() == 1)
        check("có nút Check tokens", await page.locator("#check-tokens").count() == 1)

        print("2. Bộ lọc token (prefilter)")
        await page.fill("#tokens", "\n".join(fake_jwt(f"smoke{i}@example.com")
                                             for i in range(n_tokens)))
        await page.click("#check-tokens")
        await page.wait_for_timeout(4000)
        aside = await page.locator("aside").inner_text()
        check("bộ lọc chạy và báo kết quả", "giữ" in aside and "loại" in aside, aside[:120])

        print("3. Chạy job bằng token giả + proxy chết")
        # Token giả bị bộ lọc loại hết -> tắt bộ lọc để test phần chạy job.
        if await page.locator("#prefilter").is_checked():
            await page.click("#prefilter")
        await page.wait_for_timeout(300)
        await page.get_by_role("button", name="Advanced settings").click()
        await page.wait_for_timeout(400)
        if not await page.locator("#use-proxy").is_checked():
            await page.click("#use-proxy")
        await page.wait_for_timeout(300)
        await page.fill("#proxies", "127.0.0.1:1")
        await page.click("#submit")
        await page.wait_for_timeout(6000)
        check("không pageerror sau khi Run", not errors, "; ".join(errors)[:200])

        print("4. Task card render")
        cards = await page.locator('[data-slot="card"]').count()
        check("có task card", cards >= n_tokens, f"{cards} card")
        chips = await page.locator("aside ~ * span, main span").count()
        check("có chip trạng thái", chips > 0, str(chips))
        stats = await page.get_by_text("RUNTIME OVERVIEW").count()
        check("khối Runtime overview hiện", stats == 1)

        print("5. Lọc tab + số cột + theme")
        await page.get_by_role("tab", name="Failed").click()
        await page.wait_for_timeout(800)
        failed_cards = await page.locator('[data-slot="card"]').count()
        check("tab Failed lọc được", failed_cards >= 1, f"{failed_cards}")
        await page.get_by_role("button", name="6", exact=True).click()
        await page.wait_for_timeout(600)
        cols = await page.evaluate(
            """() => { const g = document.querySelector('[data-testid="task-grid"]');
                 return g ? getComputedStyle(g).gridTemplateColumns.split(' ').length : 0 }""")
        check("chọn 6 cột có hiệu lực", cols == 6, str(cols))
        await page.locator("header button").first.click()
        await page.wait_for_timeout(500)
        theme = await page.evaluate("() => localStorage.getItem('upi-theme')")
        check("đổi theme được", theme in ("dark", "light"), str(theme))

        if shot:
            await page.screenshot(path=shot)
            print(f"  ảnh: {shot}")

        print("6. Dọn job test")
        job_id = await page.evaluate(
            """() => new URL(window.location.href).searchParams.get('job') || ''"""
        )
        if job_id:
            await page.evaluate("""async (id) => {
                await fetch('/api/clear/' + id, {method: 'POST',
                  headers: {'Content-Type': 'application/json'},
                  body: JSON.stringify({scope: 'all'})})
            }""", job_id)

        await browser.close()

    failed = [c for c in checks if not c[1]]
    print(f"\n{len(checks) - len(failed)}/{len(checks)} check pass")
    return 0 if not failed else 1


def main() -> int:
    ap = argparse.ArgumentParser(description="Smoke test UI bản Vue")
    ap.add_argument("--url", default="http://127.0.0.1:8099")
    ap.add_argument("--tokens", type=int, default=3)
    ap.add_argument("--shot", default="")
    args = ap.parse_args()
    started = time.time()
    code = asyncio.run(run(args.url.rstrip("/"), max(1, args.tokens), args.shot or None))
    print(f"xong sau {time.time() - started:.1f}s")
    return code


if __name__ == "__main__":
    sys.exit(main())
