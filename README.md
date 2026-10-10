# upi-checkout — luồng thanh toán UPI (IN) cho ChatGPT Plus

Toàn bộ logic UPI được gom về folder này. Chạy **trong** `upi-checkout/`:

```bash
cd upi-checkout
python3 cli.py auto accounts.txt --workers 4
```

**Có web UI** (dán nhiều AT, xem progress từng bước, biết đang chạy luồng OAICS hay CS):

```bash
cd upi-checkout/web && python3 app.py     # http://127.0.0.1:8099
```

→ chi tiết ở `web/README.md`.

Dashboard đã có hàng đợi dùng chung cho Run / Append / Retry, 1–200 worker mỗi job,
tối đa 1.000 task/batch, state riêng giữa các job và giao diện responsive với tìm
kiếm/chỉ số thời gian thực. Xem [kiểm thử offline](web/README.md#kiểm-thử-offline-không-tạo-giao-dịch)
để thử hiệu suất và UI mà không tạo giao dịch thật.

## Cấu trúc

```
upi-checkout/
├── cli.py                     ⭐ CLI chính: scan | oaics | cs | auto
├── cs_runner.py               wrapper luồng cs_ (payment_pages + approve) + sentinel
├── upi_cli.py                 CLI cũ (thử pipeline từng bước, đã bị cli.py thay thế)
├── upi_qr.py                  tạo link UPI + QR cho 1 account trong pool
├── run_oaics_flow.py          chạy hàng loạt luồng oaics_ (open_ai provider)
├── run_upi_cs.py              chạy luồng cs_ cho 1 account trong pool
├── run_upi_approve.py         fill-bill + approve cho 1 account (chạy bởi run_upi_batch)
├── run_upi_batch.py           chạy run_upi_approve cho nhiều account
├── scan_cs_upi.py             quét pool xem account nào ra link UPI
│
├── ideal_qr_extract.py        core: session/proxy/stripe/helper (bản sao của root)
├── extract_cs.py              luồng cs_ gốc (env-contract: UPI_TOKEN, PP_PROMO_MODE, ...)
├── check_upi_eligibility.py   probe account có hỗ trợ UPI + trial eligibility
├── sentinel_runner.js         node SDK → OpenAI-Sentinel-Token
│
├── proxy.txt                  proxy mẫu cho cli.py (vùng IN)
├── proxy_seeds*.txt           pool proxy (IN / VN)
├── proxy_state.json           state cooldown/fail của proxy
├── removed_proxies.jsonl      log proxy bị loại
├── upi_payload_*.json         payload checkout đã lưu
├── upi_qr_*.png / *.svg       QR đã tạo
├── upi_instructions_page.html trang hướng dẫn thanh toán UPI
├── cs_ctx_new1.json           context session cs_
├── upi_batch_results.json     kết quả run_upi_batch.py
├── cli_results.json           kết quả cli.py (sinh ra khi chạy)
├── dumps/                     dump HTTP thô (core ghi tự động)
├── logs/                      log (core ghi tự động)
└── _archive/                  file giữ lại, KHÔNG thuộc UPI — xem bên dưới
```

## Cách dùng

File account: mỗi dòng 1 JWT (hoặc `--token <jwt>` cho 1 account).

```bash
# Quét xem account nào có UPI (không thanh toán)
python3 cli.py scan accounts.txt --workers 4
#  → in has_upi, provider (stripe/open_ai), kind (cs/oaics), methods

# Tạo link UPI + QR cho account oaics_ (open_ai provider)
python3 cli.py oaics accounts.txt --workers 4

# Luồng cs_ (stripe provider)
python3 cli.py cs accounts.txt --workers 4 --promo off

# Tự dò provider rồi chạy đúng luồng (khuyên dùng)
python3 cli.py auto accounts.txt --workers 4
```

| Arg | Ý nghĩa | Mặc định |
|---|---|---|
| `--token` | 1 JWT (thay cho accounts) | - |
| `--proxy` | file proxy (`host:port:user:pass` hoặc `http://user:pass@host:port`) | `proxy.txt` trong folder |
| `--workers` | số account đồng thời | 4 |
| `--country` | billing country | `IN` |
| `--promo` | promo cho luồng cs/auto (off/campaign/trial/...) | `off` |

Kết quả mỗi lần chạy: `cli_results.json`.

## Quy ước đường dẫn

Script dùng 2 gốc:

- `ROOT` = folder `upi-checkout/` — nơi ghi output (payload, QR, results, dumps, logs).
- `BASE` = folder cha `Momo-Checkout/` — nơi đọc **tài nguyên dùng chung** với luồng MoMo:
  `tokens*.txt`, `tokens.json`, `proxy_in*.txt`, `pool_*.json`, `run_state/`.

Vì vậy có thể chạy script từ bất kỳ cwd nào.

## Phụ thuộc & lưu ý

- Python 3.10+, `requests`; `curl_cffi` không bắt buộc; `node` cần cho sentinel token.
- Luồng `cs_` (stripe) chạy tới bước approve nhưng Stripe có thể từ chối gắn PaymentMethod UPI
  (`generic_intent_invalid_payment_method_status_for_attachment`); luồng `oaics_` ra link+QR ổn định.
- `oaics`/`cs`/`auto` tạo **PaymentIntent thật** (không tự thanh toán UPI).

## `_archive/` — không phải UPI

- `ideal_qr_extract.ideal-flow.py` — thực chất là **luồng iDEAL** (Hà Lan), 3498 dòng,
  không liên quan UPI. Nó chỉ tồn tại duy nhất trong folder `upi_checkout/` cũ (đã xoá) nên
  được giữ lại đây để không mất code. Nếu bạn còn dùng iDEAL, chuyển nó sang repo riêng.
- `proxy_state.old.json` — bản proxy_state cũ hơn (có vài key `seed/ff2f797d2d` không còn
  trong `proxy_state.json` hiện tại); giữ lại phòng khi cần đối chiếu.

## Ghi chú refactor

Trước đây logic UPI nằm rải ở root + 3 folder trùng lặp (`upi/`, `upi_checkout/`, `upi_tool/`).
Các folder đó đã được gộp vào đây và xoá. Các file trùng đã verify md5/binary-diff trước khi bỏ:

- `upi_tool/upi_core.py` ≡ `ideal_qr_extract.py` (chỉ khác tên module trong docstring + dòng import)
- `upi_tool/check_upi_eligibility.py` ≡ `check_upi_eligibility.py` (chỉ khác dòng import)
- `upi_tool/extract_cs.py` ≡ `upi/upi_extract.py` → đặt tên `extract_cs.py` cho khớp `pix-checkout/`
- `sentinel_runner.js` giống hệt nhau ở mọi bản

`ideal_qr_extract.py` và `sentinel_runner.js` ở **root vẫn được giữ nguyên** vì luồng MoMo
(`check_momo_eligibility.py`, `try_promo_update.py`) cũng import. Bản trong folder này là bản sao
độc lập — sửa một bên không tự đồng bộ sang bên kia.

**Shim:** `../check_upi_eligibility.py` ở root là file tương thích, forward sang bản thật trong
folder này cho 6 script IN còn ở root (`probe_price_in.py`, `promo_matrix_in.py`, `run_scan_25.py`,
`scan_pool_parallel.py`, `scan_zero_capable.py`, `run_zero_geo.py`). Chuyển nốt 6 script đó vào đây
thì xoá được shim.

## Theo dõi link UPI sau khi tạo (web UI)

Thẻ "Thành công" hiển thị **số tiền**, **đồng hồ đếm ngược** và **trạng thái**, và
**tự kiểm tra lại mỗi 20 giây**.

Số tiền và `expires_at` **không có** trong artifact lưu xuống đĩa — `cs` flow không ghi
hai field đó (`artifact.amount_minor` và `artifact.intent` đều `null`, log cũng không có
chữ `expires`). Nên server phải mở lại chính trang instructions:

```
<server> GET https://payments.stripe.com/upi/instructions/...   (endpoint /api/artifact)
    -> <meta id="payload" data-message="<base64url>">
    -> {amount, expires_at, intent_state, mobile_auth_url}
```

| status | `intent_state` | Nghĩa |
|---|---|---|
| `waiting` | `requires_action` / `processing` | Đang chờ khách quét — còn dùng được, còn đếm ngược |
| `succeeded` | `succeeded` | **Khách đã uỷ nhiệm thành công** — kết quả tốt nhất |
| `failed` | `requires_payment_method` | Stripe từ chối, link chết |
| `canceled` | `canceled` | Đã huỷ |
| `expired` | — | Quá `expires_at` mà vẫn `waiting` |

> ⚠️ **Đừng lẫn với `verify.py` của `upi-zero-link`.** Bên đó là *kiểm tra trước khi
> giao*: `succeeded` nghĩa là "link đã bị dùng, đừng giao lại". Ở đây là *theo dõi*:
> `succeeded` là **điều mình muốn thấy**. Tôi từng bê nguyên判据 bên kia sang nên
> báo link thành công của bạn là "dead" — đã sửa.

Hai chiều độc lập nhau, không gộp:

- **status** — link đang ở giai đoạn nào (bảng trên)
- **kind** — loại link: `fam < am` ⇒ **₹0 委托** (`fam=1.00`, `am=1999.00`);
  `fam == am` ⇒ chuỗi thanh toán ₹1999. Ví dụ link thành công của bạn:
  `fam=1.00 < am=1999.00` ⇒ đúng là uỷ nhiệm ₹0.

Vòng 20 giây **tự tắt** khi chạm trạng thái kết thúc (`succeeded` / `failed` /
`canceled` / `expired`) — không hỏi lại những link đã ngã ngũ. Đồng hồ đếm ngược cũng
chỉ hiện khi còn `waiting`, vì `expires_at` là hạn của **trang hướng dẫn**, không phải
hạn của uỷ nhiệm (uỷ nhiệm có `validitystart`/`validityend` riêng trong URI, thường 10 năm) —
hiện "đã hết hạn" trên một link đã thành công là sai.

## Lưu ý về proxy theo chặng (luồng cs_)

`extract_cs` rewrite `region-XX` trong username proxy theo từng chặng:
checkout = `UPI_BOOTSTRAP_COUNTRY` (IN), promo = `UPI_PROMOTION_COUNTRY` (**IN**),
tax/PM/approve = `UPI_PROVIDER_COUNTRY` (IN). Promo từng để VN — đã đổi về IN cho khớp
billing country. Override bằng env nếu cần.

### `generic_intent_invalid_payment_method_status_for_attachment` (luồng cs_)

**Nguyên nhân:** checkout ở `mode=subscription` → Stripe phải lưu PM để charge off-session
các kỳ sau → PM UPI **bắt buộc có mandate**. Luồng `cs_` trước đây gửi `payment_pages/{cs}/confirm`
**không kèm** `setup_future_usage` / `mandate_data`, nên PM không ở trạng thái hợp lệ để gắn
vào PaymentIntent → Stripe trả mã lỗi trên, `payment_intent_status` thành `requires_payment_method`.

Luồng `oaics_` (`cli.py` `oaics_one`) vẫn luôn gửi 3 param này qua `confirmation_tokens` nên chạy được.

**Đã sửa:** `stripe_confirm_upi()` gửi thêm
`setup_future_usage=off_session` + `mandate_data[customer_acceptance][type]=online` +
`mandate_data[customer_acceptance][online][infer_from_client]=true`.

Tắt/rollback: `UPI_CONFIRM_MANDATE=0`.

> ⚠️ **2026-10-03 — chẩn đoán trên chưa đủ.** Run 100 task lúc 23:12–23:19 vẫn chết
> đúng mã lỗi này ở cả 6 task chạy xong. Xem mục dưới.
>
> Ngoài ra `extract_cs.py:1946` có ghi chú ngược lại: gửi `mandate_data` /
> `setup_future_usage` ở tầng top-level của `payment_pages/{cs}/confirm` bị trả
> `400 parameter_unknown` — tức phần «Đã sửa» ở trên đã từng bị thử và thất bại.
> Cần đối chiếu lại hai chỗ này.

### `upi[vpa]` — nguyên nhân thật khiến không ra link (đã sửa 2026-10-03)

**Triệu chứng:** `approve ok`, `submission_state=processing`, nhưng poll mãi không có
`hosted_instructions_url`; cuối cùng `generic_intent_invalid_payment_method_status_for_attachment`.

**Bằng chứng** — `dumps/*_poll_no_redirect.txt` (5/5 file) đều ghi:

```
next_action.type = upi_await_notification
```

**Cơ chế:** `stripe_create_upi_pm()` gửi kèm `upi[vpa]`, mà giá trị đó là
`billing["vpa"]` — vốn **luôn** được `normalize_vpa()` suy ra từ `default_in_phone()`,
một số điện thoại **ngẫu nhiên** (`9xxxxxxxxx@ybl`). Truyền VPA vào là Stripe hiểu
"gửi yêu cầu thu tiền tới đúng VPA này" → PM rơi vào trạng thái chờ thu tiền, không
gắn được vào intent, và **không bao giờ** trả `upi_handle_redirect_or_display_qr_code`
(hosted_instructions_url + QR). Đúng kết luận của `upi-zero-link` bước 4:
*«建 UPI payment method（不要传 `upi[vpa]`）»*.

**Đã sửa:** thêm `should_send_vpa(billing)` — mặc định **không** gửi `upi[vpa]`.
Chỉ gửi khi người dùng thật sự đặt `UPI_VPA` (lúc đó `vpa_explicit` được bật), hoặc
khi ép `UPI_SEND_VPA=1` để quay lại đường collect-request.

Hai chỗ đã bỏ dòng cứng: `stripe_create_upi_pm()` và `add_inline_upi_payment_method_data()`.

**Phân biệt với risk block** (dễ nhầm):

| Dấu hiệu | Nghĩa |
|---|---|
| `ChatGPT approve rejected: blocked` | **risk** — OpenAI chặn |
| `generic_decline` trong `setup_intent.last_setup_error` | **risk** — Stripe decline |
| `generic_intent_invalid_payment_method_status_for_attachment` | **không phải risk** — PM không hợp lệ để gắn (thiếu mandate, **hoặc do đã gửi `upi[vpa]`**) |
| `next_action.type = upi_await_notification` | **không phải risk** — đã gửi `upi[vpa]`, Stripe chuyển sang chờ thu tiền |

`is_account_risk_error()` trong `extract_cs.py` chỉ khớp 2 nhóm đầu; mã lỗi thứ 3 không khớp
chữ ký nào, và nhánh dump `poll_risk_blocked` cũng không chạy.
