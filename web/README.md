# web/ — UI trích xuất QR UPI (dán nhiều AT)

Web dashboard cho `upi-checkout`: dán nhiều access token → xem **progress từng bước**
theo thời gian thực, biết mỗi task đang chạy **luồng backend nào** (OAICS hay CS).

## Điều phối song song và giao diện

- Mỗi job dùng **một `ThreadPoolExecutor`** chung cho batch ban đầu, thêm task và
  retry. Task chờ nằm trong queue, không tạo thread chờ riêng cho mỗi lần thêm.
- `workers`: 1–300 **mỗi job**; **không giới hạn** số task mỗi batch (đặt `UPI_MAX_BATCH=<n>` nếu muốn chặn).
  Tăng worker dần theo RAM và kết nối; giới hạn 200 không phải khuyến nghị chạy 200
  tiến trình CS trên mọi máy. Nhiều job cùng lúc cộng dồn tài nguyên.
- Không retry trùng task đang chờ/chạy. Chỉ đóng job khi mọi task đã hoàn tất,
  gồm cả phần thêm vào. Stop chặn task chưa bắt đầu; task đang chạy dùng cơ chế
  hủy sẵn có của backend, có thể cần chờ thao tác mạng đang thực hiện kết thúc.
- State backend tách theo `cs_state/<job_id>/`, tránh hai job ghi đè state của nhau.
- Snapshot lưu bằng atomic replace, checkpoint khi task hoàn tất (tối đa mỗi 2 giây)
  và lưu lần cuối khi job kết thúc. Sau restart, task chưa xong hiển thị **đã dừng**,
  không tự chạy lại thanh toán. Token không có trong snapshot.
- SSE gửi theo batch tối đa 128 event, không chiếm thread riêng cho browser đang
  chờ. Client chậm làm đầy queue sẽ nhận lại snapshot đầy đủ.
- Dashboard tiếng Việt, responsive: chỉ số worker/hàng đợi/thời gian/tốc độ,
  tìm email hoặc task ID, bộ lọc trạng thái, danh sách và lưới kết quả. DOM update
  gộp theo nhịp 32ms; chỉ render task thay đổi, giữ chi tiết mở khi đồng bộ lại.
- Tối đa 6 lần dò trạng thái link đồng thời trên mỗi tab; request trùng được gộp.
  Copy AT chỉ bật cho token xác định thuộc job vừa tạo trong phiên hiện tại;
  lịch sử/reload không suy đoán token từ nội dung ô nhập.

Không thay đổi giao thức thanh toán, điều kiện ưu đãi hay trạng thái thành công.

## Kiểm thử offline (không tạo giao dịch)

Chạy từ thư mục gốc `upi-checkout/`:

```bash
python3 -m unittest discover -s tests -v
node tests/frontend_performance.mjs
python3 tests/preview_server.py --port 8101
```

Preview tại `http://127.0.0.1:8101` dùng HTTP/SSE thật, backend giả lập, log/state
trong thư mục tạm và chặn request thanh toán/proxy. Ctrl+C để đóng preview.
Thêm `--demo-tasks 0` nếu muốn bắt đầu với hàng đợi trống.

Test bao gồm batch 1.000 task/16 worker, append/retry/Stop, lỗi chuẩn bị task,
cô lập state, checkpoint, SSE tràn queue và render 5.000 event trên 1.000 task.
Benchmark frontend là fake DOM offline, **không phải** tốc độ thanh toán thực tế.

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
    └── style.css        giao diện tối than chì, điểm nhấn xanh ngọc, responsive
```

Root backend modules: `backend.py` (shared seam), `cs_backend.py` (in-process CS adapter), `extract_cs.py` (CS protocol), and `cli.py` (OAICS/detection + CLI compatibility).

`engine.py` **không chứa logic protocol** — nó gọi `../backend.py` qua một
request và hooks nhỏ (`step`, `on_log`, `on_geo`, `should_stop`).

Backend CS có 2 chế độ, chọn bằng `UPI_WEB_CS_MODE`:

| giá trị | việc | song song |
|---|---|---|
| `subprocess` (**mặc định**) | mỗi task một tiến trình `extract_cs.py` (`cli.cs_subprocess_one`), cô lập bộ nhớ | theo giới hạn worker của job |
| `inprocess` | chạy qua `cs_backend.run_in_process` và `extract_cs.execution_context` | dùng thread và context riêng; không còn `_RUN_LOCK` trong mã hiện tại |

Giữ nguyên mặc định `subprocess`. Bộ test điều phối không đo hiệu suất của hai
backend với giao dịch thật; mức RAM và throughput còn phụ thuộc luồng và mạng.

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
| POST | `/api/run` | `{tokens, mode, country, promo, workers, retries, proxies}` → `{job_id}`. `retries` mặc định 3 (0..5), điều khiển số lần chạy lại task lỗi và các giới hạn retry nội bộ của CS; CS vẫn chạy tối thiểu một attempt để thực hiện task. |
| GET | `/api/stream/{job_id}` | SSE. Frame đầu là `state` (snapshot đầy đủ), sau đó là event live |
| GET | `/api/state/{job_id}` | snapshot đầy đủ (dùng khi reconnect) |
| GET | `/api/jobs` | danh sách job gần đây |
| POST | `/api/retry/{job_id}/{task_id}` | chạy lại 1 task |
| POST | `/api/append/{job_id}` | thêm token vào queue chung, bỏ token trùng, kiểm tra trần batch |
| POST | `/api/stop/{job_id}` | chặn task chờ và yêu cầu backend hủy task đang chạy |

`tokens` nhận: nhiều dòng JWT, mảng JSON `[{...}]`, hoặc `{accessToken: ...}`.
Tối đa 1.000 token / batch. Run và Append dùng cùng parser token.

### Event SSE

`job_start`, `task_init`, `task_flow`, `step`, `egress`, `log`, `task_artifact`,
`task_done`, `job_done`, `error`, và 3 event nội bộ: `state` (snapshot đầu kết nối),
`task_start` (bắt đầu 1 run), `job_progress` (cập nhật bộ đếm).

`task_done.status` ∈ `LINK | FAIL | TIMEOUT | APPROVE_OK_NO_LINK | UNKNOWN | error | no_promo | …`.
Chỉ `LINK` được coi là thành công; `APPROVE_OK_NO_LINK` không có link để giao.

## Ghi chú

- Job hoạt động lưu trong RAM; giữ 20 job gần đây, không đẩy job đang chạy khỏi RAM.
  Snapshot lịch sử lưu trên đĩa, đọc lại được sau restart; không tự tiếp tục task.
- Log của luồng `cs` được stream live, lưu tối đa 200 dòng/task.
- Client chậm: queue SSE giới hạn 5000 event; khi đầy server gửi snapshot mới qua SSE.
- Thao tác `oaics`/`cs`/`auto` tạo **PaymentIntent thật** (không tự thanh toán UPI).
- Test nhanh không tạo giao dịch: dùng token giả — job sẽ fail ở bước checkout 401.

## Smoke test bằng browser

Có smoke test chạy **browser thật** (Playwright + Chromium) — bắt được lỗi runtime
mà đọc code hay curl không thấy (row không render, asset 404, status sai):

Script mặc định tự khởi động backend giả lập `tests/preview_server.py`, dùng state
tạm và không tạo giao dịch. `--url` chỉ dành cho server kiểm thử riêng.

```bash
python3 smoke_ui.py --tokens 6 --shot /tmp/ui.png
```

Không cần khởi động server trước. Cần Playwright và Chromium đã cài trên máy.

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

## Thử nghiệm vượt `generic_decline` (id phiên Stripe)

`extract_cs.stripe_browser_id()` chỉ sinh chuỗi **cùng định dạng** (`uuid4` + 8 hex) chứ
chưa từng đăng ký với Stripe, trong khi Stripe.js thật đăng ký phiên tại
`https://m.stripe.com/6` rồi dùng id do Stripe cấp. Endpoint đó trả id cho cả HTTP client
thuần, nên có thể thử "đăng ký phiên" trước khi tạo SetupIntent:

```bash
UPI_STRIPE_TELEMETRY=1 python3 -c "import extract_cs; print(extract_cs.stripe_telemetry_ids())"
```

Bật `UPI_STRIPE_TELEMETRY=1` → `build_ctx()` dùng `muid/guid/sid` do Stripe cấp (gọi qua
đúng proxy của task), lỗi mạng thì tự rơi về id ngẫu nhiên như cũ. **Mặc định TẮT** để còn
A/B: chạy 2 batch account cùng nguồn (1 bật / 1 tắt), so tỉ lệ `generic_decline` mỗi lần chạy.

Lưu ý: TLS fingerprint **đã** là Chrome thật (`curl_cffi` + `impersonate=chrome146` trong
`new_session`), nên hướng "đổi JA3" không cần làm nữa; đòn còn lại là telemetry id, uy tín
account, và (đắt nhất) chạy payment element bằng browser thật.

## Skip Apply promotion khi account không đủ điều kiện

Đo trên job 7.302 task (`380a86aad3a0`):

| Trường hợp ngay sau `create_checkout` | Số task | Xử lý |
|---|---|---|
| `amount` đã = 0 | 789 | **đã skip** checkout/update từ trước (`checkout already 0 after create_checkout; skipping Apply promotion`) |
| `amount` khác 0 | 1.687 | trước đây vẫn gọi checkout/update — nhưng chỉ **1/1.687 (0,06%)** kéo được về 0 |

Nên giờ mặc định **bỏ luôn checkout/update** khi `amount` khác 0 và fail nhanh với
`promo_not_eligible` (đỡ 1 request checkout/update + 1 lần `stripe_init` ≈ 10–20s mỗi task,
trên batch 7k là ~7–9 giờ-worker). Bật lại để thử bằng `UPI_TRY_PROMO_UPDATE=1`.

## Radar Session — đã thử, Stripe không nhận, đã bỏ

Docs Stripe ([radar/optimize-risk-factors](https://docs.stripe.com/radar/optimize-risk-factors))
nói với tích hợp gọi API trực tiếp (không dùng Checkout/Elements) thì Stripe không tự thu tín
hiệu thiết bị, và **advanced risk factors** thu qua **Radar Session** là nhóm đáng giá nhất
(+36% model). Vì vậy đã thử: tạo `rse_…` bằng `POST /v1/radar/session` rồi đính
`radar_options[session]` khi tạo UPI PaymentMethod.

**Đo trên job thật — bỏ vì Stripe không nhận tham số này:**

| Ngày | Job | Kết quả |
|---|---|---|
| 16/10 | `4f9dd5274076` | 77/77 task không tạo được session (dạng body `key+mids[0]` bị `parameter_unknown`) |
| 16/10 | `b3971ef1e10b` | `rse_…` tạo được **ngay** ở dạng `key+muid+sid+guid` (89/89), nhưng `radar_options[session]` khi tạo PM trả `400 parameter_unknown — Received unknown parameter: radar_options` ⇒ **89/100 task chết** |
| 16/10 | `5f19288946e2` | bản tự hạ cấp chạy đúng: task tạo PM xong, nhưng 2/2 acc vào tới Stripe vẫn `generic_decline` |
| 16/10 | probe trực tiếp | **ghim header `Stripe-Version` cũng không cứu được**: `2025-03-31.basil` và `2025-06-30.basil` đều trả `400 parameter_unknown — Received unknown parameter: radar_options` (probe gọi thẳng `POST /v1/payment_methods` bằng `pk_live_…` thật + `rse_…` thật) |

Kết luận: `radar_options` **không phải** chuyện API version (đã thử ghim cả `2025-03-31.basil`
lẫn `2025-06-30.basil` — vẫn `parameter_unknown`); tham số này không khả dụng cho publishable
key/tài khoản này, nên Radar Session không thể
gắn vào luồng này; giữ lại chỉ tốn 1 request/task và có nguy cơ giết task nếu Stripe đổi lỗi.
⇒ **đã xoá** `stripe_radar_session()` và đoạn đính `radar_options`. Giữ lại
`stripe_telemetry_ids()` (m.stripe.com) vì nó còn dùng để điền `guid/muid` **thật** vào payload
init — đó là thứ duy nhất của nhóm tín hiệu này mà luồng gửi được.

Hai điểm khác từ docs vẫn đúng và không cần code: (1) Radar chỉ quét SetupIntent khi bật
*"Use Radar on payment methods saved for future use"* trong Radar settings — luồng này đang BẬT
(vì ta nhận `generic_decline` từ SetupIntent); (2) email/name/địa chỉ đã gửi đủ qua
`billing_details` / `customer_data[...]`.

## Dump toàn bộ API (HAR) — không phải bản ghi tay

Trước đây `extract_cs.dump_http()` chỉ ghi ở **24 điểm gắn tay trong 315 chỗ gọi HTTP** và
**không ghi header** — tức là mất đúng phần "giống browser" (UA, `sec-ch-ua`, cookie,
`Origin`/`Referer`, `x-openai-*`), và traffic của `sentinel_runner.js` (process Node) không
được ghi gì. Nay bật công tắc **"Dump all API traffic"** (hoặc `UPI_DUMP=1`) là ghi ở **tầng
transport**, phủ mọi call:

* `traffic_dump.py` nhận entry từ `extract_cs._counting_request` (bọc `Session.request` của cả
  `requests` lẫn `curl_cffi`) — mọi request của luồng Python;
* header lấy từ **request đã chuẩn hoá** (`response.request.headers`) nên có cả `cookie` do
  cookie jar chèn vào, `authorization`, `openai-sentinel-token`, `oai-*`, `sec-ch-ua`…;
* `sentinel_runner.js` ghi vào **cùng file** qua `UPI_DUMP_FILE` (env do
  `traffic_dump.child_env()` truyền xuống `check_upi_eligibility._run_node`), gồm cả nhánh
  python fallback bên trong nó.

Định dạng: `dumps/<task>/<task>.jsonl`, mỗi dòng 1 **HAR 1.2 entry**, gồm:

* request + response đầy đủ: `status`, header **nguyên văn** (không che), body **không cắt**
  (`UPI_DUMP_BODY_LIMIT=0` mặc định; đặt số nếu muốn chặn dung lượng),
* `cookies` **dạng cấu trúc** cho cả 2 chiều (request: từ cookie jar/Cookie header; response:
  `Set-Cookie` kèm `path`/`domain`/`secure`/`expires`),
* `time` (đo quanh call) và `serverMs` (`response.elapsed` — thời gian server+network),
* `redirects` + `redirectCount`: chuỗi redirect thật (`response.history`) mà HAR chuẩn không giữ.

Cả dump tay (`.txt`, 24 điểm `dump_http`) lẫn dump HAR (`.jsonl`) đều nằm trong **cùng một
thư mục theo task** `dumps/<task>/` — không rải file ra `dumps/` gốc; chạy trực tiếp
`extract_cs.py` mà không có nhãn thì vào `dumps/manual/`.

Traffic của **tiến trình cha** `cli.py` (xếp hạng pool, kiểm tra eligibility…) ghi riêng vào
`dumps/cli/cli.jsonl`; mỗi task con vẫn có file riêng `dumps/t<idx>/` (CLI truyền
`UPI_TASK_LABEL=t<idx>` xuống con) nên không trộn lẫn.

Body vẫn đi qua `_redact_text` (che Bearer/token **trong body**); muốn body cũng nguyên văn
(replay y nguyên request) thì đặt `UPI_DUMP_RAW_BODY=1`.

Đổi sang `.har` để mở bằng Chrome DevTools/Charles:

```bash
python3 dumps_to_har.py dumps/t12                  # 1 task -> dumps/t12/t12.har
python3 dumps_to_har.py dumps --merge job.har     # gộp cả job -> 1 file
python3 dumps_to_har.py dumps --summary           # thống kê theo host/status, không ghi file
```

**Cảnh báo:** vì header không bị che nên file dump chứa **access token thật**. `dumps/` đã nằm
trong `.gitignore` — đừng commit, đừng copy ra ngoài repo.

## Vì sao ghi `results.json` thất bại: btrfs hết METADATA (không phải hết đĩa)

Sự cố 19/10 lúc 22:46: `write_results()` báo `OSError: [Errno 28] No space left on device`…
**trong khi `df -h` còn 35 GB trống**. Chi tiết đo được:

```
$ findmnt -no FSTYPE,OPTIONS --target .        → btrfs, compress=zstd:1, space_cache=v2
$ btrfs filesystem usage /home
    Device allocated:     195.31GiB
    Device unallocated:     1.00MiB     ← đã cấp hết, không thể tạo chunk mới
    Free (statfs, df):     34.55GiB     ← "trống" nằm trong chunk DATA
    Metadata,DUP: Size 4.50GiB, Used 3.62GiB (80.36%)   ← METADATA sắp đầy
```

btrfs tách không gian thành chunk **data** và **metadata** (ở đây metadata nhân đôi = DUP 2×).
Khi metadata gần đầy mà **không còn không gian chưa cấp phát** để mở chunk mới, kernel trả
`ENOSPC` cho thao tác **tạo file mới** — dù dữ liệu vẫn còn 34 GB. Đó chính là lỗi ghi file.

Cái gì đẩy metadata tới đó:

| Nguồn | Đo được |
|---|---|
| `cs_state/web_seed_<job>_<task>.txt` — **mỗi task 1 file, không ai xoá** | **10.492 file** |
| `cs_state/<job>/cs_<idx>.json` — mỗi task 1 file | 1.000+ file |
| Thư mục job cũ (118 job) | `cs_state` **360.768 file** (1,4 GB) |
| `results.json` 11–24 MB bị **ghi lại mỗi 2 giây** khi job chạy | write amplification rất lớn |

Bằng chứng trực tiếp: lần ghi headline bị ENOSPC → xoá 118 thư mục job cũ (≈335k file) → **ghi
lại đúng file đó thành công ngay**.

Cách chữa đã áp dụng:

* `_cleanup_task_files()` — xoá seed + `cs_<idx>.json` **ngay khi task xong** (số file còn ~ số
  worker thay vì ~ số task).
* `_cleanup_job_files()` — job kết thúc thì xoá seed lẻ + cả thư mục `cs_state/<job>/`.
* `CHECKPOINT_SECS` 2s → **10s** (`UPI_CHECKPOINT_SECS`) cho `results.json`.
* `write_results()` **không nuốt lỗi** nữa: log + đẩy lên UI, và lưu `write_error` vào snapshot.
* `start_job()` **từ chối chạy** khi đĩa còn < `UPI_MIN_FREE_MB` (mặc định 1024 MB).
* `recover_results.py` dựng lại kết quả từ flow log nếu đã lỡ mất (mặc định dry-run).
* Dọn tay một lần: `cs_state` 360.768 → **4.007 file** (1,4 GB → 17 MB).

Khi `btrfs filesystem usage` cho thấy `Device unallocated` ≈ 0 hoặc Metadata > 85%, cần cân bằng
lại (quyền root): `sudo btrfs balance start -dusage=50 -musage=50 /home`.

## Link hiện "Link created" / "Not verified yet" — khác "Awaiting payment"

Chip trạng thái link có 3 mức khác nhau, đừng nhầm:

| Chip | Nghĩa |
|---|---|
| **Awaiting payment · 4m** | đã đọc được trang instructions: `intent_state=requires_action` → uỷ nhiệm đã tạo, khách **chưa** quét |
| **Not verified yet** (xám) | **chưa đọc được** trang để kết luận (SSLError/timeout/proxy) — link có thật nhưng trạng thái chưa biết |
| **Payment successful** | khách đã quét/duyệt (`succeeded`); dò Plus chạy ngay sau đó |

Đo thật trên job `28371763d2de`, task `t16` (card đầu tiên trong Tasks): lúc task xong, bước
verify link gặp `SSLError` (proxy TLS tới `payments.stripe.com`) → flow ghi
`link unverified (SSLError)`, trạng thái để trống → chip rơi về nhánh mặc định và hiện
"Link created". Vòng dò nền sau đó đọc được: lần đầu `waiting` (khách chưa quét), **2 phút sau
thành `succeeded`** — tức khách đã duyệt, chỉ là UI chưa kịp hiện.

Nay:

* `artifact_probe.probe()` **thử lại 3 lần** khi lỗi mạng (SSLError/timeout/proxy) với backoff
  ngắn, thay vì để chip treo tới lượt dò sau.
* Nếu vẫn lỗi: vòng dò **thử hộ bằng 2 proxy khác** đã từng đọc được link (`_working_proxies`),
  và **đổi luôn proxy ghim trong sổ** sang proxy đọc được. Đo thật 19/10 (task `t4077`,
  job `987dc3b8d9d7`): proxy ghim lỗi `SSLError` tới `payments.stripe.com`, nhưng qua 3 proxy
  khác (và đọc trực tiếp) trang trả `succeeded` — tức khách ĐÃ duyệt mà UI đứng ở
  "Not verified yet" chỉ vì một proxy hỏng.
* UI tách nhánh **"Not verified yet"** (chip xám, tooltip = lý do: `unverified … SSLError`) khỏi
  "Link created"/"Awaiting payment".

## Thứ tự trong danh sách: theo lúc trích xuất (cũ → mới)

Tab Success/Failed/Skipped/Stopped sắp theo `done_at` **tăng dần** (cái ra trước nằm trên),
task chưa xong (`done_at` trống) xuống cuối theo `index`; tab Queued/Running giữ thứ tự hàng đợi.
Mỗi card có thêm **giờ xong `HH:MM:SS`** ở góc phải để đối chiếu thứ tự (tooltip = ngày giờ đầy đủ).
Trước đó danh sách sắp mới-nhất-lên-đầu nên nhìn không khớp với thời điểm link được tạo.

## QR/link hiện ngay (không cần reload) + countdown hạn dùng

* **Bug đã sửa:** server phát event `task_artifact` với các field **phẳng**
  (`{type, task_id, upi_link, qr_png, qr_svg, amount_minor, …}`), nhưng FE cũ đọc `ev.artifact`
  → `undefined`, nên QR/link chỉ hiện khi reload (lấy snapshot). Nay merge đúng các field phẳng
  vào `task.artifact`, nên vừa có link là card hiện QR + payment link ngay.
* **Countdown:** khi `link_state = waiting`, FE gọi `GET /api/artifact?url=…` để lấy `expires_at`
  (server cache 120 s) và hiện chip `⏳ mm:ss remaining` (quá hạn → `expired`, màu đỏ). Có một
  đồng hồ chung 1 giây (`useNow`) thay vì mỗi card một interval. Vòng quét 10 s, tối đa 12 link
  mỗi lần, mỗi link làm mới tối đa 20 s/lần.

## Card task hiển thị đủ Billing details + Progress (steps)

Bản vanilla có 2 khối mà bản Vue port thiếu; nay đã thêm lại trong `TaskCard.vue`:

* **Progress / steps** — dòng gọn `FLOW CS · 7/10 · Approve payment` (bước đang chạy hoặc bước
  lỗi gần nhất); bấm mở ra danh sách đầy đủ 10 bước với ký hiệu `✓ done` `◐ active` `✕ fail`
  `⊘ skip (Skipped)` `○ pending`, kèm `detail`/`err` (lỗi dài tự xuống dòng).
* **Billing details** — `Address source` (tier1/tier2/hardcoded-list), `Address check`
  (ok/fixed/fixed-pin/bad-pin/mismatch-exit), `Billing name`, `Billing city + PIN`, `Exit geo`.
* **Proxy egress** — `Exit IP (country)`, `Billing IP`, `Billing geo (approx)`, `ISP`,
  `Proxy verdict · risk`.

Dữ liệu lấy từ snapshot (`steps`, `billing_*`, `geo_*`, `egress`) và cập nhật live qua event
`step`, `task_billing`, `egress` (trước đây `task_billing` và `egress` bị bỏ qua nên card không
bao giờ hiện hoá đơn).

## Số "Running" trên UI: vì sao lệch và đã sửa gì

Đo trên job thật 6.262 token / 300 worker: server luôn có **300 running** (sample 10 lần), nhưng
UI hiện **162**. Nguyên nhân **không phải** worker không lấp slot mà là **UI bỏ sót event**:

* `useDesk` chỉ xử lý `task_done`/`step`/`task_link`… mà **không xử lý `task_start`**, nên task
  đã chạy vẫn bị coi là `queued`; số Running đứng yên ở giá trị của snapshot đầu tiên.
* Hàng đợi SSE đầy thì server gửi `resync` — trước đây FE bỏ qua luôn event này.

Đã sửa: xử lý `task_start` (→ `running`), `task_init`, `task_removed`, `tasks_cleared`, `resync`
(lấy lại snapshot), và `job_progress` giờ mang thêm `running`/`queued` để UI dùng **số của
server** thay vì tự đếm từ Map. Ô Running cũng hiện `300/300` (số worker) để thấy rõ trần.

Đo lại sau khi sửa (cùng job): UI và API khớp **300 = 300** qua 8 lần sample.

Ngoài ra: checkpoint chuyển sang **thread nền** (`Job.start_checkpoint_thread`) — worker chỉ bật
cờ rồi nhận task kế tiếp, không tự ghi `results.json` (0,1-0,3 s/lần) trong `finally` nữa.

## Nút Stop: vì sao "trông như không tác dụng" và đã sửa gì

Bấm Stop **vẫn gửi `POST /api/stop/{job}` và server nhận** (`stop_requested=true`), nhưng trước
đây UI **không hiện gì** nên tưởng nút hỏng. Thực tế:

* Task **đang chạy** không thể cắt giữa một request HTTP; nó dừng ở **checkpoint kế tiếp**
  (`extract_cs.check_cancelled()` thấy `should_stop` → `FlowCancelled`) — thường vài giây, nhưng
  có request treo tới hàng chục giây.
* Task **trong hàng đợi** chỉ được đánh dấu `stopped` khi một worker nhấc nó lên (mỗi lần rất
  nhanh, nhưng với 300 worker và 6.000 task vẫn cần vài vòng).

Sửa ở UI (commit sau `fb92209`): nghe event `job_stopping` và hiện ngay banner
**“Stopping… N task(s) still finishing; queued tasks will be skipped”**, nút Stop đổi thành
**Stopping…** và bị khoá, nút Run cũng khoá; job xong thì banner tự biến mất và status = stopped.
Bấm Stop lần hai (job đã dừng) không còn hiện lỗi 400 — coi như đã dừng.

## Bộ lọc token trước khi chạy (`web/prefilter.py`)

**Endpoint dùng để check promo (lấy từ repo tham khảo `gpt-auto-register-main`, hàm `check_plus`)**:

```
GET https://chatgpt.com/backend-api/accounts/check/v4-2023-04-27?timezone_offset_min=-480
Authorization: Bearer <AT> · ChatGPT-Account-ID: <account_id từ JWT>
```

Đọc `accounts.<id>`: `account.is_deactivated` (bị khoá), `account.plan_type` /
`entitlement.has_active_subscription` (đã Plus), **`eligible_promo_campaigns`** (có
`plus-1-month-free` hay không). **1 request/token, không tạo checkout, không chạm Stripe.**

Vì sao KHÔNG "tạo checkout thử để xem promo": đo trên 4 job thật (2.549–6.015 task/ job) —
acc *ineligible* tốn ~5 request & ~35 s (create_checkout + stripe_init) mới biết; acc *đã Plus*
tốn ~10 request. Nếu prefilter cũng tạo checkout thì mỗi acc **đủ điều kiện** bị tạo checkout 2
lần (probe + chạy thật) → tăng bề mặt risk. Dùng GET ở trên thì rẻ hơn và không thêm checkout.

Kết luận (`verdict`): `eligible` (giữ) · `not_eligible` (giữ, để pipeline tự quyết — có thể do
proxy) · `plus_active`/`active_sub`/`banned`/`token_invalid`/`bad_token`/`expired` (loại, không
tốn proxy) · `network_error`/`rate_limited` (giữ).

Gặp thật (23/10): một AT còn hạn trên JWT (`exp` +6 ngày) vẫn bị 401
`{"code":"token_expired","message":"Your authentication token has expired. Please try refreshing
it."}` trên cả `/backend-api/me`, `/accounts/check`, `/subscriptions` — tức session đã bị thu hồi
chứ không phải hết hạn theo đồng hồ. Vì vậy probe đọc `error.code` để tách `token_expired`
(“hết hạn/thu hồi — cần refresh AT”) khỏi `token_invalid` (401 không rõ lý do), và **retry đổi
proxy** khi TLS lỗi (proxy đầu SSLError là chuyện thường gặp với pool 500).

Dùng:

### Hai chế độ dùng bộ lọc

**(1) Lọc trong worker — mặc định của nút Run.** Khi bật công tắc, mỗi task tự gọi 1 GET
`accounts/check` **ngay đầu `_run_task`, trước cả bước chấm/lấy proxy**; acc bị loại được đánh
dấu `status="skipped"` với lý do (`skipped_reason`) và **không chạy flow, không retry**. Nhờ vậy
batch 6.000 token **bắt đầu ngay**, không phải chờ quét cả batch như bản trước; chi phí lọc nằm
trong lúc các worker khác đang chạy. UI có tab + ô đếm **Skipped**, card hiện
“Skipped before run — <lý do>”, Batch picker hiện `⊘ N`.

**Âm thanh khi có link** — công tắc **“Sound on success”** trong *Advanced settings* (mặc định
bật, nhớ trong `localStorage['upi-notify-sound']`), có nút ▶ để nghe thử. Âm thanh là file thật
`web/fe/public/sounds/success.mp3` (tiếng thông báo Messenger, 34 KB, 2,0 s) — Vite copy sang
`dist/sounds/` và FastAPI mount `/sounds`. Client phát file này khi nhận `task_link` có
`link_state=succeeded`; nếu file lỗi/không tải được thì phát 2 nốt C6→E6 bằng Web Audio để vẫn
có tín hiệu. Audio được *mở khoá* ngay trong cú click **Run** (và khi bật công tắc) để tránh bị
chặn autoplay.

**Hiệu năng khi chạy 300 worker** — đo thật: pool giữ đúng **300 running** (3.000 task, sample
mỗi 0,7 s); cú tụt thoáng qua là do `write_results` cũ giữ `job._lock` trong lúc `json.dumps`
snapshot (0,16 s snapshot + 0,07 s dumps cho 6.000 task). Đã sửa: `snapshot()` copy nông thay
`deepcopy` (0,163 s → 0,048 s cho 6.000 task) và serialize/ghi file **ngoài lock**.

**(2) Preview check (tuỳ chọn, không chặn Run).** Nút **Preview check (N)** + ô **Sample**
(mặc định 200, `0` = tất cả) + **Threads** (2–32) để xem trước tỉ lệ acc đủ điều kiện, rồi bấm
**Use N** nếu muốn cắt bớt danh sách.

* Sidebar → công tắc **“Filter tokens before run”** (mặc định bật) + nút **Check N tokens**;
  kết quả hiện `checked/total · kept N · dropped M` (+ tốc độ `/s` và ETA khi đang chạy) + badge
  theo lý do, nút **Use N** thay ô token bằng acc đủ điều kiện, nút **Stop** dừng giữa chừng.
* **Không bắt buộc check hết**: ô **Sample** (mặc định 200, `0` = tất cả) chỉ check N token đầu;
  token ngoài mẫu **đi thẳng vào batch** (không bị loại). Batch 6.000 token vì vậy vẫn bấm Run
  được ngay. Ô **Threads** (2–32, mặc định 16) chỉnh số request song song của bước check. Bật công tắc thì bấm **Run** sẽ lọc trước rồi mới chạy; lọc hết sạch thì báo lỗi và
  **không tạo job**.
* API: `POST /api/prefilter {tokens, workers, proxies}` → `{id}`; poll `GET /api/prefilter/{id}`
  (`checked/kept/dropped/counts/results[]` có `index` để map lại token); `POST /api/prefilter/{id}/stop`.
* CLI: `python3 web/prefilter.py --tokens-file tokens.txt --workers 8 [--proxy-file proxy.txt]`.

Test: `tests/test_prefilter.py` (19 case, không cần mạng) — classify payload, JWT email/hạn/
account_id, round-robin proxy, map fingerprint → token.

## Phân tích lịch sử: `analyze_history.py`

```bash
python3 analyze_history.py --days 7          # tỉ lệ theo ngày, phân bố lỗi, so trước/sau mốc sửa
python3 analyze_history.py --errors          # kèm top câu lỗi thô
```

Đọc `logs/web_<job>_results.json` (cùng nguồn desk dùng cho lịch sử) + `cs_state/link_watch.json`;
chỉ đọc, không sửa. Nhóm lỗi khớp `web/fe/src/lib/taskView.ts` để số liệu khớp giao diện. Dùng để
trả lời "lần sửa vừa rồi có cải thiện thật không?" thay vì đoán.

## Giao diện: Vite + Vue 3 + TS + shadcn-vue (chỉ còn một bản, ở `/`)

`/` phục vụ bản Vue (`web/fe` → `web/static/dist`), asset ở `/assets/*`. Giao diện vanilla cũ
(`static/index.html` + `app.js` + `style.css` + `notify.mp3`) và route `/classic` **đã xoá**;
`/next` chỉ còn redirect 308 về `/` cho bookmark cũ. Muốn xem lại bản cũ thì lấy từ git history
(commit trước `4bd621d`).

**Chọn batch (`JobPicker.vue`)** — thay `<select>` trần: mỗi dòng hiện `#id · trạng thái ·
✓success / total · ✗fail · HH:MM DD/MM`, màu `chart-2`/`destructive`; nút mở cũng hiện ngay số
success của batch đang xem. Danh sách 20 batch gần nhất, có scroll.

**Biểu đồ ngày (`DailyChart.vue`)** — panel "₹0.00 codes extracted per day" (bản vanilla cũ):
21 ngày gần nhất từ `GET /api/stats/global` (`days[]`), cột = số code ₹0.00/ngày, ngày cao nhất
tô `--chart-1`, các ngày khác `--chart-2`, hover ra tooltip (ngày / code / paid / unknown / jobs).
Màu **lấy nguyên token shadcn**, không pha opacity.

**Sidebar điều khiển job đã có ở `/`** (`web/fe/src/components/ControlPanel.vue`): dán access
token (có icon 🔑, đếm token ngay, nút **Copy** toàn bộ token đã trim — đổi sang ✓ 1,5 s sau khi
copy), Flow (CS/OAICS/Auto), Country, Workers (1–300), Runs/account (1–5),
Advanced settings (Promotion, proxy riêng, Mix pool order, Proxy quality filter, Stripe session,
Dump traffic — toàn bộ là `Switch`/`Select` của shadcn), nút **Run** (gọi `POST /api/run`) và
**Stop job** (`POST /api/stop/{job}`). Giá trị form lưu `localStorage['upi-run-form']` (riêng
token thì **không** lưu, tránh chạy nhầm batch cũ). Run bị disable khi chưa có token; lỗi từ
backend (vd chặn vì đĩa còn ít) hiện ngay dưới form. Bản vanilla đã xoá hẳn; nếu cần đối chiếu thì lấy lại từ git history.

Grid dày hơn: mặc định responsive **2 → 5 cột** (`sm/lg/xl/2xl`), kèm nút chọn **2–6 cột** trên
toolbar (nhớ trong `localStorage['upi-grid-cols']`); mỗi lần vẽ 180 card rồi "Load more".

## Chi tiết bản Vue (trước đây chạy thử ở `/next`)

Bản giao diện vanilla (`web/static/{index.html,app.js,style.css}`) vẫn ở `/` và **không bị thay
đổi**. Bản dựng bằng framework nằm ở `/next` để so sánh trước khi đổi hẳn.

```
web/fe/                 # mã nguồn: Vite 8 + Vue 3.5 + TS + Tailwind 4 + shadcn-vue (reka-ui)
web/fe/src/lib/api.ts       # kiểu dữ liệu + fetch /api/*, EventSource /api/stream
web/fe/src/composables/useDesk.ts  # store: job, Map task theo task_id, patch từ SSE
web/fe/src/lib/taskView.ts  # nhãn/màu chip trạng thái, lý do fail (giữ đúng ngữ nghĩa bản cũ)
web/fe/src/components/*     # StatsBar, TasksPanel, TaskCard, StatusChip
web/static/dist/        # KẾT QUẢ BUILD — commit để server chạy không cần Node
```

Lệnh:

```bash
pnpm -C web/fe install          # lần đầu
pnpm -C web/fe dev              # dev server :5174, proxy /api -> :8099 (HMR)
pnpm -C web/fe build            # build ra web/static/dist (base '/next/')
```

FastAPI mount sẵn: `/next` (và `/next/`) phục vụ `static/dist`, asset trỏ `/next/assets/…`; nếu
chưa build thì trả 404 kèm hướng dẫn. `app.py` chỉ thêm 2 dòng mount + 1 route, không đổi API.

Khác biệt có chủ đích so với bản vanilla:

* **Render giới hạn 120 card/lần + nút “Load more”** — job thật 6.000 task, vẽ hết DOM sẽ treo.
* **Ảnh QR lấy trực tiếp `artifact.qr_png`** (không qua `/api/artifact` — endpoint đó trả JSON
  trạng thái), và **ẩn QR khi link ở trạng thái cuối** hoặc khi Stripe đã xoá ảnh (HTTP 410),
  thay vì để khung ảnh vỡ.
* **Giữ job đang xem trên URL** (`/next?job=<id>`), mở được cả job cũ không nằm trong 20 job mới
  nhất; SSE `job_done` không làm mất lựa chọn.
* Chip trạng thái tách 3 mức như bản vanilla: `Payment successful` / `Awaiting payment` /
  `Not verified yet` (kèm tuổi lần dò), và `Plus Success` chỉ hiện khi thật sự lên Plus.

### Giao diện: dùng đúng shadcn-vue, **màu nguyên bản**

* Component do chính CLI sinh vào `src/components/ui/` (`card`, `badge`, `button`, `input`,
  `tabs`, `progress`, `separator`, `tooltip`, `scroll-area`) — dựa trên **reka-ui**, `cva`,
  `cn`, Tailwind 4; không tự viết lại.
* Bảng màu **lấy nguyên khối `:root` + `.dark` từ docs shadcn-vue** (neutral/oklch, `--radius:
  0.625rem`, đủ `--chart-1..5`, `--sidebar-*`) — không pha màu riêng. Chip trạng thái cũng chỉ
  dùng token có sẵn: `chart-2` (payment successful), `chart-3` (awaiting), `destructive`
  (failed), `muted-foreground` (chưa kiểm tra).
* **Theme switch** kiểu shadcn: class `dark` trên `<html>`, nút Mặt trời/Mặt trăng ở header,
  mặc định theo `prefers-color-scheme`, lưu vào `localStorage['upi-theme']` (đã kiểm: reload
  vẫn giữ). Bản light là giao diện sáng chuẩn của shadcn.

Đã kiểm bằng Chromium (Playwright): trang render, SSE `Live (SSE)`/`Finished` đúng, card Success
có QR decode được (với link còn hạn), Badge render đúng class của lib (`badgeVariants`), tooltip
mở khi hover, theme đổi + nhớ sau reload, `body` background đúng `oklch(1 0 0)` / `oklch(0.145 0 0)`,
không lỗi console; deep link `?job=` hoạt động.

## Vòng dò link: "Awaiting payment" không được đứng im

Chip "Awaiting payment" = trang instructions trả `intent_state=requires_action`, tức uỷ nhiệm
₹0 đã tạo nhưng khách **chưa quét/duyệt trong app UPI**. Đó là trạng thái đúng — vấn đề là
trước đây nó bị **đóng băng**:

* `MONITOR_MAX_IDLE_MIN = 30`: job xong quá 30 phút thì vòng dò nền **thôi không kiểm tra**
  link nữa. Đo trên job `3dcd91d4f58a`: 7 link, lần dò cuối cách đó **32–42 phút**, 2 link
  `waiting` sẽ không bao giờ đổi trạng thái nữa dù khách duyệt muộn hay uỷ nhiệm hết hạn.
* Khi trạng thái không đổi thì `_monitor_one` **không emit gì**, nên UI cũng không biết link
  còn được theo dõi hay không.

Nay:

| | Trước | Sau |
|---|---|---|
| Thời gian theo dõi | 30 phút sau khi job xong | **24 giờ** (`UPI_MONITOR_MAX_IDLE_MIN`, mặc định 1440) |
| Nhịp dò | 30s | 30s khi job còn nóng (≤ 5 phút sau khi xong), sau đó **5 phút/lần** (`UPI_MONITOR_COLD_SECS`) |
| Trạng thái không đổi | không báo gì | emit `task_link_checked` → chip hiện thêm tuổi lần dò: **“Awaiting payment · 4m”** |
| Hết hạn | chỉ khi trang trả `expires_at` đã qua | giữ nguyên, nhưng giờ vòng dò còn chạy nên bắt được |

Vì mỗi job chỉ có vài link sống (7 link / 5.859 acc trong job đo được), chi phí theo dõi 24h
là không đáng kể, và mọi request dò đều đi qua proxy của chính task đó.

### Sổ theo dõi bền vững (`cs_state/link_watch.json`)

Nguyên nhân sâu hơn: **job chỉ sống trong RAM**, restart desk là mất — mà vòng dò cũ chỉ nhìn
`_JOBS` trong RAM. Nên link của job cũ không ai dò nữa dù khách có duyệt sau đó.

* Mỗi link được ghi vào sổ (link, proxy của task, trạng thái, mốc dò, `first_seen/flip_at`,
  trạng thái Plus) — sổ nhỏ (16 entry cho ~30 job), được ghi lại sau mỗi lần dò.
* Vòng dò đọc **sổ** chứ không đọc RAM → job cũ vẫn được dò tiếp; nếu job còn trong RAM thì
  Task + SSE cũng được cập nhật như trước.
* `/api/state` trộn trạng thái link mới nhất từ sổ vào snapshot đọc từ đĩa
  (`engine.link_watch_overlay`) — không phải ghi lại file snapshot 18 MB mỗi lần dò.
* Lúc khởi động desk chạy `backfill_link_watch_from_disk()` (thread riêng) để nạp link của các
  job cũ vào sổ; mỗi lần mở một job cũ cũng nạp bù (`backfill_link_watch`).

Đo lại ngay sau khi bật: sổ có **16 link**, trong đó **8 link đã được khách duyệt
(`succeeded`)** — 6 trong số đó trước đây UI hiển thị nhầm là "Awaiting payment". 8 link còn
lại đã 28–33 giờ tuổi, quá cửa sổ 24h nên được bỏ theo dõi (phiên Stripe đã chết).

Giới hạn còn lại: snapshot **không lưu token** (cố ý — token là bí mật), nên link của job cũ
khi chuyển sang `succeeded` sẽ **không** tự chạy được bước dò Plus (badge "Plus Success" chỉ
hiện với job còn trong RAM).

## Lỗi `Cloudflare chặn tạo checkout (HTTP 403, cf-mitigated=challenge)`

Không phải Stripe, cũng không phải account bị khoá: **Cloudflare chặn ngay ở
`POST https://chatgpt.com/backend-api/payments/checkout`** trước khi flow chạm Stripe. Body trả
về là trang JS challenge (`Enable JavaScript and cookies to continue`), header
`cf-mitigated: challenge`, `cf-ray` với colo Ấn Độ (MAA/DEL/BOM/AMD…). Client của flow là HTTP
thuần (không chạy JS) nên không thể vượt; `cf_clearance` thì flow **không** có (cookie gửi lên chỉ
có `oai-did`).

Đo trên pool hiện tại (18/10) bằng chính endpoint `https://chatgpt.com/api/auth/csrf`:

| Kết quả | Số proxy |
|---|---|
| `200` (đi qua bình thường) | **9/24** |
| `403 + cf-mitigated: challenge` | **14/24** |
| lỗi TLS | 1/24 |

⇒ chặn theo **uy tín từng IP**, không phải chặn cả tài khoản. Nên cách xử lý là **chọn/đổi proxy**,
không phải bỏ account:

* `ippure.cf_challenge()` — canary mới: gọi `/api/auth/csrf` qua proxy trong lúc chấm điểm, gắn cờ
  `cf-challenge` (điểm tối đa 30 ⇒ grade tụt) khi gặp `403 + cf-mitigated: challenge`.
* Bộ lọc proxy (`filter_proxies`) **loại luôn** proxy mang cờ này (cùng nhóm với
  `unreachable`/`wrong-country`), nhờ vậy job không chết oan vì rơi vào IP bị chấm xấu.
* Trong flow: 403 ở bước tạo checkout **không còn `break`** — đo lại cho thấy 9/24 proxy đi qua
  được nên đổi proxy là có ích; proxy đó bị đánh dấu hỏng (`record_failure_by_stage`) và số lần
  thử vẫn bị chặn bởi `UPI_CHECKOUT_RETRY_MAX`.
* Nhãn lỗi trên UI đổi thành **“Cloudflare chặn tạo checkout (HTTP 403, cf-mitigated=challenge) —
  proxy IP bị chấm xấu, chưa hề chạm Stripe”** (trước đây ghi “OpenAI blocked checkout creation”,
  dễ hiểu sai là OpenAI khoá account). Vẫn đánh `no_retry` cho account vì chạy lại cùng acc với
  cùng vùng proxy gần như chắc chắn dính tiếp.

## Rà soát rò IP thật (leak) — mọi request ra ngoài phải qua proxy

Dump toàn bộ API (mục trên) cho phép soi lại từng request, và phát hiện các chỗ **gọi thẳng
ra Internet không qua proxy** — tức lộ IP thật của máy:

| Chỗ gọi | Hậu quả đo được (16/10) | Xử lý |
|---|---|---|
| `geobilling._reverse_geocode()` — Nominatim + BigDataCloud | BigDataCloud trả `402 … your ip address 2402:800:… is now banned` cho **5.580/5.580** lần; Nominatim **5.573 × 429** | Thêm tham số `proxy`, session dùng proxy của task; cache theo toạ độ (6h ok / 15m fail) + nhịp **1,1s/lần** cho Nominatim |
| `geobilling._reverse_bigdatacloud()` gọi `/data/reverse-geocode-client` | endpoint chỉ dành cho browser → 402 + ban IP | **Bỏ hẳn**; chỉ gọi `/data/reverse-geocode` khi có `UPI_BIGDATACLOUD_KEY`, không key thì không gọi gì |
| `geobilling._pin_record()` — `api.postalpincode.in` | đi thẳng bằng IP thật | Thêm tham số `proxy` (truyền từ `address_for_proxy` + 4 chỗ trong `extract_cs`) |
| `web/artifact_probe.probe()` — dò trang instructions | chạy **mỗi 20 giây/link** từ desk, đi thẳng ra `payments.stripe.com` | Thêm tham số `proxy`; engine truyền `task.proxy` (proxy của task được lưu lại trên `Task` vì lease đã trả về pool) |
| `cli.py` luồng `oaics_`: `POST /v1/confirmation_tokens` và confirm | đi thẳng ra `api.stripe.com` | Thêm `proxies=` |
| `extract_cs` lưu artifact (`UPI_SAVE_LINK_ARTIFACT`) ×2 chỗ `requests.get(redirect_url)` | đi thẳng | Dùng `fetch_via_proxy()` |

Đã kiểm và **vốn đã đúng** (có proxy): `upi_core.py` init, `at_check.py` ×2, `cli.py` các probe
egress/init, `extract_cs` tier-1 Nominatim, `ippure._http_get`.

Hai thứ thêm vào để lần sau tự soi được:

* **`extract_cs.dump_http`/recorder ghi cờ `proxied`** cho MỌI request trong dump — `false`
  nghĩa là request đó đi thẳng bằng IP của máy (`requests.get(...)` module-level tạo session
  rỗng nên `proxies` rỗng). Muốn rà lại: `python3 dumps_to_har.py dumps --summary` rồi soi
  hoặc `grep -c '"proxied": false' dumps/*/*.jsonl`.
* `restart_desk.sh` **từ chối restart khi không đọc được `/api/jobs`** mà service vẫn chạy
  (job lớn làm endpoint này chậm >60s — đo thật với 5.824 task). Trước đây ca đó bị coi là
  "không có job" nên restart sẽ giết batch đang chạy; nay phải `--force` mới đi tiếp.

Kiểm chứng lại sau khi sửa (một lần gọi `geobilling.address_for_proxy` với proxy thật):

```
GET  ip-api.com                   200 proxied=True
GET  nominatim.openstreetmap.org  200 proxied=True     ← trước đây 429, và đi thẳng
(không còn request nào tới api.bigdatacloud.net)
```

## Lỗi `400 Billing country must match request country`

OpenAI từ chối tạo checkout vì "nước billing phải khớp nước của request" (dù proxy exit là IN
theo ipwho). Đo trên 4.607 account từng dính: chỉ **22 (0,5%)** từng ra được mã ở job khác,
so với ~3% chung ⇒ đây là chuyện **account/region**, không phải proxy hỏng. Nên:

* trong flow: gặp lỗi này thì **break**, không thử proxy khác (trước đây thử thêm 2 lượt vô ích);
* ở engine: đánh `no_retry` (`account region does not match the IN billing country`).

Riêng số **proxy ứng viên cho chặng checkout** giờ **không buộc theo `retries`**: trước đây
`_env_values()` ghi đè `UPI_CHECKOUT_RETRY_MAX=3` bằng `retries` nên log hiện
`checkout-retry=1` — nay là `max(3, retries)`.

## Chính sách retry theo loại lỗi (audit)

Đo trên **3.965 task có ≥2 lượt chạy** trong log còn giữ: **0 lần retry nào ra được link**, và
các nhóm lỗi lặp lại y hệt sau khi chạy lại:

| Lỗi ở lượt trước | Số lần thử lại | Lượt sau OK | Lặp y hệt lỗi cũ |
|---|---|---|---|
| checkout 403 | 1.521 | 0 | 44,3% |
| lỗi vận chuyển proxy | 1.030 | 0 | 11,2% |
| timeout | 876 | 0 | 1,6% |
| no link at approve (chung) | 343 | 0 | **82,2%** |
| chỉ có hosted link (không phải QR) | 248 | 0 | 1,2% |
| user already paid | 8 | 0 | 62,5% |

Nên engine **bỏ retry** cho các nhóm chắc chắn là lỗi cấp account (đổi proxy cũng vô ích):
`risk decline` (theo cả cờ lẫn nội dung lỗi) · `promo not eligible` · `checkout 403` ·
`user already paid` · `chỉ có hosted link` · **`flow stopped at step 'approve'`** (bật lại bằng
`UPI_RETRY_APPROVE_STALL=1`).

Vẫn **giữ retry** cho lỗi tạm thời: lỗi vận chuyển proxy, timeout, không dựng được địa chỉ
(nhóm này có lượt chạy lại rẻ hoặc do chọn nhầm proxy). Lưu ý `retries=1` = không có lượt nào.

## Kiểm tra địa chỉ ngay lúc CHỌN PROXY (không đợi tới bước billing)

Proxy không dựng được địa chỉ hoá đơn (IP exit không định vị được, ipwho bị rate-limit,
PIN không tra được) thì bị **loại ngay ở tầng chọn proxy** như proxy chết —
`engine._address_usable()` gọi `extract_cs.billing_address_ok()` (có cache 15 phút theo proxy).
Tắt bằng `UPI_ADDRESS_PRECHECK=0`.

Vì sao không để tới bước điền billing: lúc đó task đã chạy hết checkout, và retry nghĩa là
chạy lại **cả flow** — đắt ngang dính risk decline. Kiểm ở tầng chọn proxy chỉ tốn ~1–3s
(đã cache) và worker đổi ngay sang proxy khác.

Nếu vẫn lọt tới bước billing (ví dụ geo chết đúng lúc đó), engine cho **thêm 1 lượt với proxy
mới** kể cả khi `retries = 1` (`_is_address_error`), và trong flow `billing_for_round()` cũng
thử các proxy khác của cùng round trước khi bỏ task.

## Địa chỉ hoá đơn: không dùng list cứng nữa

Mặc định `UPI_BILLING_FALLBACK=0`: nếu không dựng được địa chỉ từ IP exit (mức 2 = PIN
xác thực, mức 1 = ipwho + Nominatim) thì **task fail ngay ở đầu `run_once()`** — trước cả
bước tạo checkout — chứ không gửi một địa chỉ thành phố metro + PIN cố định không khớp IP.
Bật lại để so sánh bằng `UPI_BILLING_FALLBACK=1`.

Lý do: đo trong log thấy fallback xảy ra ~2,1% số lần dựng profile (14.279/683.350), phần
lớn vì proxy thoát ở nước khác (602 ca Philippines, 35 ca Myanmar) hoặc không định vị được
bang (5.810 ca) — địa chỉ Ấn Độ cứng + IP nước ngoài là tín hiệu xấu với Stripe/OpenAI.
Trong số đơn ra mã, nhóm dùng list cứng chiếm ~6,5% (bỏ ngày 10-04 là artifact của bản code
chưa có geo-IP).

### Suy bang khi API geo không trả `region`

Trong log có **34.908 ca `cannot canonicalise state '?'`** — kiểm lại thì **100% là `'?'`**,
tức API **không trả về region**, chứ không phải tên bang viết khác, nên bảng alias tên bang
không cứu được ca nào. Cách vá:

* `geobilling._CITY_STATES` — bảng **510 thành phố → bang** (mọi bang, gồm cả tên cũ
  Bangalore/Mysore/Belgaum và biến thể "Bengaluru Urban", "Mumbai Suburban District").
* `geobilling.state_from_city()` dùng làm mắt cuối trong chuỗi suy bang của
  `address_for_proxy()` (sau reverse geocode, `state_for_pin`, region của IP) và trong mức 1
  của `extract_cs` khi `state_code_in()` thất bại.
* Mức 1 cũng lấy **quận/huyện thật của PIN** (`pin_district()`) làm city khi reverse geocode
  không trả city.
* `canonical_state()` bỏ thêm tiền tố/hậu tố hay gặp: `State of Kerala`, `Kerala state`,
  `Bihar, India`.

### Kiểm tra địa chỉ trước khi gửi (`billing check`)

`extract_cs.verify_billing_address()` chạy ngay trong `upi_billing_profile()` (đầu mỗi round)
và trả về một verdict, ghi vào `billing_check` của task + hiện ở panel dòng **Address check**:

| verdict | Nghĩa | Có sửa gì không |
|---|---|---|
| `ok` | PIN có thật, city/state khớp PIN, cùng bang với IP exit | không |
| `fixed` | city/state lệch PIN | **sửa** city/state theo India Post |
| `fixed-pin` | PIN không tồn tại thật (hay gặp ở mức 1) | **thay** bằng PIN đã xác thực cùng bang + đổi city/locality theo PIN đó |
| `mismatch-exit` | PIN thuộc bang khác IP exit | không (phải giữ địa chỉ khớp IP — đây là cảnh báo proxy/geo) |
| `bad-pin` | không tra được PIN (và không vá được `_seed_pins`) | không |
| `unknown` | không có PIN | không |

Fallback mức 1 cũng không còn mượn tên đường của thành phố khác: khi Nominatim không trả
`road` thì lấy tên bưu cục thật của chính PIN đó, không thì chỉ `"<số nhà>, <thành phố>"`.

Kết quả mỗi task có thêm `billing_tier`: `tier2` (IP + PIN xác thực) · `tier1` (IP + reverse
geocode) · `hardcoded-list`, hiện trong panel ở dòng **Address source**.

## Lỗi vận chuyển proxy (`TLS connect error`) và cách chống

Triệu chứng trên UI: `no link — flow stopped at step 'approve' | last log: … promotion stage
failed: Failed to perform, curl: (35) TLS connect error …`. Đo trực tiếp trên pool hiện tại:

| Phép đo | Kết quả |
|---|---|
| 30 proxy, gọi **lần lượt** (1 kết nối/lúc) | **11/30 chết hẳn** (37%) — `ProxyError` |
| 19 proxy còn sống, 20 kết nối song song | 0% lỗi |
| 19 proxy còn sống, 60 kết nối song song | 0% lỗi |
| 19 proxy còn sống, **120 kết nối song song** | **4,2%** `TLS connect error` |

Tức có HAI nguồn: (1) proxy chết hẳn trong pool — lọc bằng **Proxy quality filter** hoặc
`region_pool.py --check`; (2) handshake rơi khi quá nhiều kết nối đồng thời.

Cách chống trong code: `extract_cs.with_transport_retry()` bọc các chặng dễ rơi (tạo checkout,
checkout/update của promotion) — gặp lỗi vận chuyển thì thử lại **3 lượt cách 1,5s**
(`UPI_TRANSPORT_RETRY`, `UPI_TRANSPORT_RETRY_DELAY`), còn lỗi nghiệp vụ (403 chặn checkout,
decline, promo không hợp lệ) thì **ném ra ngay**, không đốt proxy. Kết quả task cũng ghi rõ
nguyên nhân: `proxy transport error at 'promotion' stage (TLS/connection) — no link`.

## Tiến độ bước (step) trong card task

Tiến độ phải **đơn điệu**: bước nào đứng trước một bước đã xong thì chỉ có thể là
`done`/`skip`, không bao giờ còn `pending`. Lỗi cũ (chỉ ở chế độ **in-process** mà desk
đang dùng): luồng subprocess có `cli.make_step_advancer()` riêng để lấp các bước bị nhảy
qua, còn in-process thì không — nên card hiện ✓ ở giữa mà phía trên vẫn là ô trống, nhìn
như "step hiện tùm lùm, không theo kịp log".

Giờ `web/engine.py::_mk_hooks.step()` tự đánh dấu `skip` cho mọi bước **phía trước** bước
vừa tới mà còn `pending` (kèm event `step` để UI cập nhật ngay, không cần F5), và bỏ qua
bước đầu `runner` (là cả luồng, không phải bị bỏ). Retry vẫn reset toàn bộ về `pending`.

## Dọn log (`clean_logs.sh`)

`logs/` phình ~1 GB mỗi tuần nếu để nguyên. Dùng:

```bash
./clean_logs.sh                    # xem trước, giữ 3 ngày (DRY-RUN, không xoá gì)
./clean_logs.sh --days 7           # xem trước với mốc 7 ngày
./clean_logs.sh --apply            # xoá thật
```

* **Không bao giờ** xoá `logs/web_*_results.json` trừ khi bạn tự đặt `--results-days N > 0`
  — chúng là nguồn của bảng thống kê mã ₹0 và mọi phân tích lịch sử.
* Giữ nguyên cache tra cứu trong `cs_state/` (`in_address_ledger.json`,
  `in_pincode_store.json`, `in_state_seeds.json`).
* Từ chối chạy khi đang có job (`--force` để bỏ qua) — xoá state của task đang chạy là hỏng task.

## Trộn pool proxy (Mix pool order)

`_pick_proxy` chia proxy theo **thứ tự** trong pool (round-robin, để 100 task trên 100
proxy thì không trùng nhau). Pool dán tay thường được xếp theo từng khối vùng —
200 dòng `-st-Maharashtra`, rồi 200 dòng `-st-Bihar`… — nên nếu không trộn thì 200 task
đầu chỉ nhận đúng **một** vùng. Tick **"Mix pool order"** (mặc định BẬT) để server trộn
pool một lần cho mỗi job (`engine.mix_pool`), nhờ đó mỗi account rút từ một nhóm vùng khác
nhau. Bỏ tick = giữ nguyên thứ tự dán vào.

Trộn bằng RNG gieo theo `job_id`: mỗi job một thứ tự mới, cùng job thì tái lập được. Pool
sau khi trộn vẫn được ghi ra `cs_state/web_proxies_<job>.txt` (đúng thứ tự đã dùng) và
nhãn job hiện thêm `· mixed` để biết lần chạy đó có trộn hay không.

## Tắt bộ lọc proxy (Proxy quality filter)

Tick **"Proxy quality filter"** (mặc định BẬT): mỗi worker tự probe proxy của mình, loại con
không kết nối được / sai nước / tor / malicious rồi thử con kế tiếp; hết pool thì sinh thêm
1 session mới. Đây là đường `UPI_PROXY_QUALITY=lazy`.

Bỏ tick → `Job.filter_proxies = False`: dùng pool **nguyên trạng**, chia theo thứ tự (hoặc
thứ tự đã trộn nếu bật Mix pool order), **không probe, không loại, không sinh bù**. Nhãn job
hiện thêm `· no filter` để biết lần chạy đó có lọc hay không — dùng để so 2 chiều xem bộ lọc
có thật sự giúp gì.

Lưu ý: bộ lọc chỉ loại proxy *hỏng/sai nước*, **không** loại theo `risk`/verdict. Số liệu
74.276 lần thử cho thấy tỉ lệ ra mã ₹0 gần như không đổi theo chất lượng IP (clean 0,47% ·
medium 0,32% · dirty 0,37%), nên tắt lọc chủ yếu để tiết kiệm 2–5s probe mỗi task.

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
