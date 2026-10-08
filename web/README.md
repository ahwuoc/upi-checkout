# web/ — UI trích xuất QR UPI (dán nhiều AT)

Web dashboard cho `upi-checkout`: dán nhiều access token → xem **progress từng bước**
theo thời gian thực, biết mỗi task đang chạy **luồng backend nào** (OAICS hay CS).

```bash
cd upi-checkout/web
python3 app.py                 # http://127.0.0.1:8099
python3 app.py --port 9000     # đổi cổng
```

Mở `http://127.0.0.1:8099`, dán token (mỗi dòng 1 JWT), chọn luồng, bấm chạy.

## Cấu trúc

```
web/
├── app.py               FastAPI: HTTP + SSE
├── engine.py            queue/SSE orchestration, phát event
└── static/
    ├── index.html       layout 2 cột (sidebar + danh sách task)
    ├── app.js           EventSource, cập nhật DOM tại chỗ theo task_id
    └── style.css        dark theme
```

Root backend modules: `backend.py` (shared seam), `cs_backend.py` (in-process CS adapter), `extract_cs.py` (CS protocol), and `cli.py` (OAICS/detection + CLI compatibility).

`engine.py` **không chứa logic protocol** — nó gọi `../backend.py` qua một
request và hooks nhỏ (`step`, `on_log`, `on_geo`, `should_stop`).

Backend CS có 2 chế độ, chọn bằng `UPI_WEB_CS_MODE`:

| giá trị | việc | song song |
|---|---|---|
| `subprocess` (**mặc định**) | mỗi task một tiến trình `extract_cs.py` (`cli.cs_subprocess_one`), state riêng, ~70–100MB RAM/task | **thật**, đúng bằng số worker |
| `inprocess` | chạy trong chính process web qua `cs_backend.run_in_process` | ❌ bị `cs_backend._RUN_LOCK` khoá suốt flow CS (vì `extract_cs` có state toàn cục) → mỗi lúc chỉ 1 task CS |

Vì sao mặc định là `subprocess`: đo thật job 100 task / 20 worker ở chế độ
`inprocess` — 20 task hiện "đang chạy" nhưng 13 con đứng im ở bước `runner` suốt 7
phút (không có log nào), chỉ 5 task xong ⇒ ~1 task/100s ⇒ 100 task mất ~5,5 giờ
thay vì ~17 phút. Cùng phép đo ở `subprocess`: 4 task xong trong 4,5s (chạy chồng
nhau) so với 10,9s (nối đuôi 4,6 → 6,2 → 8,7 → 10,9s). Chỉ dùng `inprocess` khi
cần tiết kiệm RAM hoặc để đối chiếu khi debug.

## Luồng backend hiển thị

| `flow` | Nhãn UI | Step |
|---|---|---|
| `oaics` | **LUỒNG OAICS** | Warmup → Create checkout → Apply promotion → Proxy egress → Update tax region → Create payment method → Confirm checkout → Confirm intent → Extract QR artifact |
| `cs` | **LUỒNG CS** | 10 bước: Initialize CS flow → Create checkout → Apply promotion → Initialize payment page → Update tax region → Create payment method → Confirm payment → Approve payment → Poll payment artifact → Extract payment artifact |
| `null` | ĐANG DÒ FLOW / — | Warmup → Detect provider |

Chế độ `auto` chạy bước **Detect provider** trước; khi biết kết quả, server gửi
`task_flow` và UI **thay danh sách step** bằng bộ step của luồng thật.
Nếu detect fail (sentinel lỗi / token 401) thì **không gán nhãn luồng nào** —
tránh hiện sai "LUỒNG CS" khi thực tế chưa chạy luồng nào.

## API

| Method | Path | Việc |
|---|---|---|
| POST | `/api/run` | `{tokens, mode, country, promo, workers, retries, proxies}` → `{job_id}`. `retries` mặc định 1 (0..5), điều khiển số lần chạy lại task lỗi và các giới hạn retry nội bộ của CS; CS vẫn chạy tối thiểu một attempt để thực hiện task. |
| GET | `/api/stream/{job_id}` | SSE. Frame đầu là `state` (snapshot đầy đủ), sau đó là event live |
| GET | `/api/state/{job_id}` | snapshot đầy đủ (dùng khi reconnect) |
| GET | `/api/jobs` | danh sách job gần đây |
| POST | `/api/retry/{job_id}/{task_id}` | chạy lại 1 task |
| POST | `/api/stop/{job_id}` | dừng job (task đang chạy không ngắt giữa chừng) |

`tokens` nhận: nhiều dòng JWT, mảng JSON `[{...}]`, hoặc `{accessToken: ...}`.
Tối đa 200 token / batch.

### Event SSE

`job_start`, `task_init`, `task_flow`, `step`, `egress`, `log`, `task_artifact`,
`task_done`, `job_done`, `error`, và 3 event nội bộ: `state` (snapshot đầu kết nối),
`task_start` (bắt đầu 1 run), `job_progress` (cập nhật bộ đếm).

`task_done.status` ∈ `LINK | FAIL | TIMEOUT | APPROVE_OK_NO_LINK | UNKNOWN | error | no_promo | …`.
Chỉ `LINK` và `APPROVE_OK_NO_LINK` được coi là có link.

## Ghi chú

- Job lưu **trong RAM**, giữ 20 job gần nhất; restart server là mất.
- Log của luồng `cs` được stream live, lưu tối đa 200 dòng/task.
- Client chậm: queue SSE giới hạn 5000 event, event cũ nhất bị bỏ — UI tự đồng bộ
  lại bằng `/api/state`.
- Thao tác `oaics`/`cs`/`auto` tạo **PaymentIntent thật** (không tự thanh toán UPI).
- Test nhanh không tạo giao dịch: dùng token giả — job sẽ fail ở bước checkout 401.

## Test

Có smoke test chạy **browser thật** (Playwright + Chromium) — bắt được lỗi runtime
mà đọc code hay curl không thấy (row không render, asset 404, status sai):

```bash
python3 smoke_ui.py --tokens 6 --shot /tmp/ui.png
```

Server phải đang chạy. Dùng token giả nên **không tạo giao dịch thật**.

## Lịch sử lỗi đã sửa (đều lọt qua kiểm tra bằng curl)

| Lỗi | Triệu chứng | Nguyên nhân |
|---|---|---|
| `createTaskEl` trả `wrap` thay vì object refs | **Không row nào render** — list trống, không thấy email/step | `t.el = createTaskEl(t)` → `t.el.wrap` undefined → `appendChild` throw; bị `try/catch` trong `es.onmessage` nuốt mất |
| `normalizeStatus` chỉ khớp status chữ HOA | Refresh xong mọi task hiện "chờ", progress 0% | Backend gửi `done`/`fail`/`error`/`no_promo`, hàm chỉ biết `LINK`/`FAIL`/`ERROR` |
| `index.html` dùng đường dẫn asset tương đối | Trang mất cả CSS lẫn JS | `href="style.css"` resolve thành `/style.css`, nhưng static mount ở `/static` |

## Proxy theo từng bước (luồng cs)

`extract_cs` rewrite region của proxy theo chặng, dùng cùng một sticky session:

| Bước | Country | Ghi chú |
|---|---|---|
| Create checkout | `UPI_BOOTSTRAP_COUNTRY` = IN | |
| Apply promotion | `UPI_PROMOTION_COUNTRY` = **IN** | đổi từ VN — promo phải cùng country với billing |
| Tax / PM / approve | `UPI_PROVIDER_COUNTRY` = IN | |

Log in ra `derived proxy chain: ... IN checkout=proxy#x; IN promotion=proxy#y; IN provider/approve=proxy#x`.
Proxy bị **redact** thành `proxy#hash` (`register_proxy_for_redaction`) nên log không lộ credential —
muốn xem region thì đọc chain line hoặc derive bằng `extract_cs.proxy_for_country()`.

Override qua env: `UPI_PROMOTION_COUNTRY`, `UPI_BOOTSTRAP_COUNTRY`, `UPI_PROVIDER_COUNTRY`.

## Step sáng theo tiến độ

Bước đang chạy được tô sáng: nền xanh + viền sáng + vạch accent trái + nhịp glow
(`@keyframes stepGlow`). Bước xong = xanh lá, thất bại = đỏ. Tiến độ **đơn điệu**
(chỉ tiến, không lùi) — các vòng retry lặp lại marker nhưng bước đã xong không bị tụt.

## Lọc proxy sạch / risk thấp trước khi chạy

Bật **"Chỉ dùng proxy sạch (risk thấp)"** + đặt **điểm tối thiểu** → server quét cả pool
trước, chỉ proxy đạt ngưỡng mới được đưa vào chạy.

Nút **"Quét proxy"** quét riêng để xem bảng xếp hạng (không chạy task).

### Cách chấm điểm (`cli.score_proxy`)

| Tiêu chí | Điểm | Ghi chú |
|---|---|---|
| Kết nối được | +45 | không được → 0 điểm, cờ `unreachable` |
| Đúng country | +25 | sai → **chặn trần 25 điểm**, cờ `wrong-country` |
| ISP không phải datacenter | +15 | khớp keyword cloud/hosting/vps → cờ `datacenter-isp` |
| IPv4 | +8 | IPv6 → cờ `ipv6` |
| Độ trễ < 2500ms | +7 | > 6000ms → cờ `slow` |
| Lịch sử thành công | +6/lần (tối đa +18) | đọc từ `proxy_state.json` |
| Lịch sử thất bại | −10/lần (tối đa −30) | cờ `history-fail` |

Xếp hạng: **A** ≥75 · **B** ≥55 · **C** ≥35 · **F** <35.

Lịch sử lấy từ `proxy_state.json` do chính `extract_cs` ghi lại (`record_proxy_result`),
tra theo `proxy_short()` — nên proxy từng fail vì "unusual activity" sẽ bị trừ điểm.

### API

```bash
curl -X POST localhost:8099/api/scan-proxies \
  -d '{"proxies":"host:port:user:pass","country":"IN","workers":12}' \
  -H 'Content-Type: application/json'
# -> {total, grades:{A,B,C,F}, usable, results:[{score,grade,ip,country,ip_type,org,latency_ms,flags,history}]}
```

`POST /api/run` nhận thêm `min_score` (0..100). `min_score > 0` → quét & lọc trước khi chạy;
không proxy nào đạt → HTTP 400 kèm điểm cao nhất đạt được.
