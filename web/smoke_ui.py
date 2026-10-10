#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""smoke_ui.py — test giao diện thật bằng browser (Playwright + Chromium).

Vì sao cần: các bug UI (row không render, status sai, asset 404) không lộ ra khi
chỉ curl HTML/JS. Script này mở trang bằng browser thật, chạy 1 job token giả rồi
assert DOM — bắt được lỗi runtime mà đọc code không thấy.

Chạy:
    python3 smoke_ui.py                      # tự khởi động backend giả lập offline
    python3 smoke_ui.py --url http://127.0.0.1:9000 --tokens 6

Yêu cầu: pip install playwright  +  chromium (hoặc google-chrome).
Mặc định dùng tests/preview_server.py: không gọi payment/proxy, không ghi log thật.
--url chỉ dùng với server kiểm thử riêng; không trỏ vào job/tài khoản thật.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import sys

FAILURES: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f"  ({detail})" if detail else ""))
    if not ok:
        FAILURES.append(name)


def fake_jwt(email: str) -> str:
    def b64(d: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(d).encode()).rstrip(b"=").decode()
    return (f"{b64({'alg': 'RS256'})}."
            f"{b64({'https://api.openai.com/profile': {'email': email}, 'exp': 9999999999})}.sig")


async def run(url: str, n_tokens: int, headed: bool, shot: str | None) -> int:
    from playwright.async_api import async_playwright

    tokens = "\n".join(fake_jwt(f"smoke{i}@example.com") for i in range(n_tokens))

    async with async_playwright() as p:
        launch_kwargs = {"args": ["--no-sandbox"]}
        for exe in ("/usr/bin/chromium", "/usr/bin/google-chrome-stable", "/usr/bin/google-chrome"):
            import os
            if os.path.exists(exe):
                launch_kwargs["executable_path"] = exe
                break
        browser = await p.chromium.launch(headless=not headed, **launch_kwargs)
        page = await browser.new_page(viewport={"width": 1280, "height": 900})

        errors: list[str] = []
        bad_responses: list[str] = []
        page.on("pageerror", lambda e: errors.append(str(e)))
        page.on("response", lambda r: bad_responses.append(f"{r.status} {r.url}") if r.status >= 400 else None)

        print("1. Load trang + asset")
        await page.goto(url + "/", wait_until="domcontentloaded")
        # xoa form da luu de test sach (khong lay token con lai tu lan truoc)
        await page.evaluate("() => localStorage.clear()")
        await page.reload(wait_until="domcontentloaded")
        await page.wait_for_timeout(800)
        check("không có pageerror khi load", not errors, "; ".join(errors)[:160])
        check("không có asset 404", not bad_responses, "; ".join(bad_responses)[:160])
        css = await page.evaluate("() => getComputedStyle(document.body).backgroundColor")
        check("CSS đã áp (body có background)", css not in ("rgba(0, 0, 0, 0)", "transparent"), css)

        print("2. Submit job")
        await page.fill("#tokens", tokens)
        # Ép dùng proxy CHẾT (127.0.0.1:1) để task fail ngay lập tức.
        # Không làm vậy thì job lấy pool từ proxy.txt — sau 2026-10-04 file đó là
        # pool thật (cliproxy region-IN), mỗi task chạy ~200 giây, và bước 7
        # (xoá task) sẽ fail oan vì server cấm xoá task của job đang chạy.
        # Test giao diện không nên phụ thuộc (và không nên tiêu) pool thật.
        await page.check("#use-proxy")
        await page.fill("#proxies", "127.0.0.1:1")
        await page.click("#submit")
        await page.wait_for_timeout(5000)
        check("không có pageerror sau khi chạy", not errors, "; ".join(errors)[:200])

        print("3. Row task render")
        info = await page.evaluate("""() => {
          const list = document.getElementById('task-list');
          const rows = [...list.children];
          return {
            rows: rows.length,
            hidden: rows.filter(r => r.hidden).length,
            height: Math.round(list.getBoundingClientRect().height),
            emails: rows.map(r => (r.querySelector('.task-email') || {}).textContent || '').filter(Boolean),
            firstText: rows[0] ? rows[0].innerText.replace(/\\n+/g, ' | ') : '',
            counts: {
              all: document.getElementById('c-all').textContent,
              running: document.getElementById('c-running').textContent,
              queued: document.getElementById('c-queued').textContent,
            },
            statsRunning: document.getElementById('runtime-running').textContent,
            overall: document.getElementById('overall-count').textContent,
            // Ô Job ID giờ là <input id="job-id-input"> (điền id để xem lịch sử),
            // không còn <span id="job-id"> như trước.
            jobBadge: (document.getElementById('job-id-input') || {}).value || '',
          };
        }""")
        check("số row = số token", info["rows"] == n_tokens, f"{info['rows']}/{n_tokens}")
        check("list có chiều cao (>0)", info["height"] > 0, f"{info['height']}px")
        check("row có email", len(info["emails"]) == n_tokens, f"{len(info['emails'])} email")
        check("tab 'Tất cả' đếm đúng", info["counts"]["all"] == str(n_tokens), info["counts"]["all"])
        check("sidebar có số đang chạy/đang chờ",
              int(info["statsRunning"]) + int(info["counts"]["queued"]) <= n_tokens,
              f"running={info['statsRunning']} queued={info['counts']['queued']}")
        check("có job id", info["jobBadge"] not in ("", "—"), info["jobBadge"])

        print("4. Mở detail của row đầu")
        await page.click(".task-row")
        await page.wait_for_timeout(700)
        det = await page.evaluate("""() => {
          const d = document.querySelector('.task-detail:not([hidden])');
          if (!d) return null;
          return {
            text: d.innerText.replace(/\\n+/g, ' | '),
            steps: d.querySelectorAll('.step, .step-row').length,
            hasFlowBadge: !!d.querySelector('.flow-badge, .badge-flow'),
          };
        }""")
        check("detail mở được", det is not None)
        if det:
            check("detail hiển thị email", "smoke0@example.com" in det["text"])
            check("detail hiển thị 'Run'", "Run" in det["text"])
            check("detail có progress", "progress" in det["text"].lower() or "%" in det["text"])

        print("5. Tab lọc — mỗi tab phải hiện ĐÚNG số ghi trên nhãn")
        keys = await page.evaluate(
            "() => [...document.querySelectorAll('.tab')].map(b => b.dataset.filter)")
        for key in keys:
            await page.click('.tab[data-filter="' + key + '"]')
            await page.wait_for_timeout(250)
            # Đọc nhãn và số dòng **trong cùng 1 lần evaluate**.
            # Trước đây lấy nhãn 1 lần ở đầu vòng lặp rồi so với số dòng đo sau đó
            # ~1s: 6 token giả fail rất nhanh nên khi bấm tới tab 'fail' thì 2 task
            # cuối vừa chuyển sang fail -> 6 dòng hiện mà nhãn cũ ghi 4, test báo
            # lỗi oan. Task đang chạy thì nhãn là giá trị sống, phải đọc cùng nhịp.
            vis, live = await page.evaluate("""(k) => {
                const b = document.querySelector('.tab[data-filter="' + k + '"]');
                return [
                    [...document.getElementById('task-list').children].filter(r => !r.hidden).length,
                    parseInt((b.querySelector('.tab-count') || {}).textContent, 10) || 0,
                ];
            }""", key)
            check("tab '" + key + "': hiện " + str(vis) + " = nhãn " + str(live),
                  vis == live, str(vis) + " vs " + str(live))
        await page.click('.tab[data-filter="all"]')
        await page.wait_for_timeout(250)
        back = await page.evaluate("() => [...document.getElementById('task-list').children].filter(r => !r.hidden).length")
        check("quay lại tab 'Tất cả' hiện đủ row", back == n_tokens, str(back))

        print("6. Persistence (localStorage)")
        stored = await page.evaluate("() => !!localStorage.getItem('upi-web-form-v1')")
        check("form được lưu vào localStorage", stored)

        print("7. Xoá task: ✕ từng card + nút Dọn dẹp (phải xoá cả trên server)")
        # Chờ job kết thúc: server CẤM xoá task đang chạy/chờ của job đang chạy
        # (chúng thuộc worker thread), nên test phải đợi thay vì fail oan.
        await page.wait_for_function(
            "async () => { const j = state.jobId; if (!j) return true;"
            " const d = await (await fetch('/api/state/' + j)).json();"
            " return d.status !== 'running'; }", timeout=60000)
        # Hộp xác nhận giờ là modal của app (#modal), KHÔNG còn confirm() của
        # browser -> không còn dialog event; phải bấm nút trong modal.
        async def confirm_modal():
            await page.wait_for_selector("#modal:not([hidden])", timeout=3000)
            title = await page.text_content("#modal-title")
            await page.click("#modal-ok")
            return title

        async def server_task_ids():
            return await page.evaluate(
                "async () => (await (await fetch('/api/state/' + state.jobId)).json())"
                ".tasks.map(t => t.task_id)")

        before = await server_task_ids()
        check("server có đủ task trước khi xoá", len(before) == n_tokens, str(len(before)))
        # ✕ trên card đầu: phải biến mất khỏi DOM *và* khỏi /api/state
        await page.hover(".task-row")
        await page.click(".task-row .task-remove")
        m_title = await confirm_modal()
        check("✕ mở modal xác nhận của app (không phải confirm() browser)",
              "Remove this task" in m_title, m_title)
        await page.wait_for_timeout(900)
        rows_now = await page.evaluate("() => document.getElementById('task-list').children.length")
        after = await server_task_ids()
        check("✕ xoá 1 task khỏi DOM", rows_now == n_tokens - 1, str(rows_now))
        check("✕ xoá 1 task trên server (không quay lại sau F5)",
              len(after) == n_tokens - 1 and before[0] not in after,
              str(len(after)) + " còn lại")
        await page.reload(wait_until="domcontentloaded")
        await page.wait_for_timeout(2500)
        persisted = await page.evaluate("() => document.getElementById('task-list').children.length")
        check("F5 xong vẫn đúng số task đã xoá", persisted == n_tokens - 1, str(persisted))

        # nút Dọn dẹp: nhãn phải nói rõ tab + số lượng
        label = await page.evaluate("() => document.getElementById('btn-clear-tab-label').textContent")
        check("nút Dọn dẹp ghi rõ tab và số lượng", "(" in label and label.strip().endswith(")"), label)
        await page.click('.tab[data-filter="all"]')
        await page.wait_for_timeout(250)
        # Huỷ trong modal thì KHÔNG được xoá gì (làm trước, vì xoá xong nút bị disable)
        await page.click("#btn-clear-tab")
        await page.wait_for_selector("#modal:not([hidden])", timeout=3000)
        await page.click("#modal-cancel")
        await page.wait_for_timeout(400)
        hidden = await page.evaluate("() => document.getElementById('modal').hidden")
        still = await server_task_ids()
        check("nút Huỷ đóng modal và KHÔNG xoá gì",
              hidden is True and len(still) == n_tokens - 1,
              "hidden=%s còn %d task" % (hidden, len(still)))
        await page.click("#btn-clear-tab")
        m_title = await confirm_modal()
        check("Dọn dẹp mở modal nói rõ số task", "task" in m_title, m_title)
        await page.wait_for_timeout(1200)
        left_server = await server_task_ids()
        left_dom = await page.evaluate("() => document.getElementById('task-list').children.length")
        check("Dọn dẹp xoá hết trên server", left_server == [], str(len(left_server)))
        check("Dọn dẹp xoá hết trên DOM", left_dom == 0, str(left_dom))
        await page.reload(wait_until="domcontentloaded")
        await page.wait_for_timeout(2000)
        still = await page.evaluate("() => document.getElementById('task-list').children.length")
        check("F5 sau Dọn dẹp vẫn trống", still == 0, str(still))

        if shot:
            await page.screenshot(path=shot)
            print(f"  -> ảnh: {shot}")

        await browser.close()

    print()
    if FAILURES:
        print(f">>> {len(FAILURES)} TEST FAIL: " + ", ".join(FAILURES))
        return 1
    print(">>> TAT CA PASS")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="UI smoke test cho upi-checkout/web")
    ap.add_argument("--url", default=None,
                    help="test server đang chạy sẵn (mặc định: tự spawn server riêng)")
    ap.add_argument("--tokens", type=int, default=6)
    ap.add_argument("--headed", action="store_true", help="hiện cửa sổ browser")
    ap.add_argument("--shot", default=None, help="lưu ảnh chụp màn hình")
    args = ap.parse_args()

    # Mặc định spawn server riêng: job registry nằm trong RAM của tiến trình server,
    # nên tiến trình mới = registry sạch -> test không đụng job của người dùng.
    proc = None
    url = args.url
    try:
        if url is None:
            import socket
            import subprocess
            import time
            import urllib.request

            with socket.socket() as s:
                s.bind(("127.0.0.1", 0))
                port = s.getsockname()[1]
            url = f"http://127.0.0.1:{port}"
            from pathlib import Path
            preview = Path(__file__).resolve().parents[1] / "tests" / "preview_server.py"
            proc = subprocess.Popen(
                [sys.executable, str(preview), "--port", str(port), "--demo-tasks", "0"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            for _ in range(60):
                try:
                    urllib.request.urlopen(url + "/api/jobs", timeout=1)
                    break
                except Exception:
                    time.sleep(0.25)
            else:
                print("Server test không khởi động được", file=sys.stderr)
                return 2
            print(f"Server offline riêng: {url}  (backend giả lập, state tạm)\n")

        return asyncio.run(run(url, args.tokens, args.headed, args.shot))
    except ImportError:
        print("Thiếu playwright: pip install playwright", file=sys.stderr)
        return 2
    finally:
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()
                proc.wait()


if __name__ == "__main__":
    raise SystemExit(main())
