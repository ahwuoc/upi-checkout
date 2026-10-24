# Audit: những gì đã sửa ở backend trong lúc làm frontend Vue

Phạm vi: từ commit `5908721` (trước khi bắt đầu làm FE, 23/10) đến `606795a` (24/10).
Lệnh kiểm tra lại:

```bash
git log --oneline 5908721..HEAD
git diff --stat 5908721..HEAD -- '*.py' ':(exclude)web/fe' ':(exclude)tests'
git diff 5908721..HEAD -- web/app.py
git diff 5908721..HEAD -- web/engine.py
```

## 1. File backend bị sửa (số dòng)

| file | + | − | loại |
|---|---|---|---|
| `web/app.py` | 81 | 7 | route + 3 endpoint + mount asset |
| `web/engine.py` | 160 | 45 | cờ prefilter, status `skipped`, checkpoint thread, snapshot perf |
| `web/prefilter.py` | 410 | 0 | **file mới** (bộ lọc token) |
| `analyze_history.py` | 225 | 0 | **file mới** (công cụ phân tích lịch sử) |
| `web/smoke_ui.py` | 106 | 311 | viết lại (UI cũ bị xoá nên test cũ vô dụng) |
| `web/static/index.html`, `app.js`, `style.css`, `notify.mp3`, `icons.svg` | — | xoá | giao diện vanilla |

## 2. File KHÔNG bị đụng (toàn bộ logic nghiệp vụ)

`extract_cs.py` · `cli.py` · `backend.py` · `ippure.py` · `geobilling.py` ·
`traffic_dump.py` · `dumps_to_har.py` · `recover_results.py` · `global_stats.py` ·
`artifact_probe.py`

→ Không có thay đổi nào ở luồng tạo checkout, Stripe, UPI, proxy, telemetry, dump traffic.

## 3. Từng thay đổi backend, kèm lý do

### 3.1 Bắt buộc để phục vụ giao diện mới
* `app.mount("/assets", ...)`, `app.mount("/sounds", ...)` — JS/CSS và file mp3 của bản build
  (trước đó chỉ mount `/static` của bản vanilla).
* `GET /` trả `web/static/dist/index.html` (bản Vue); thêm `GET /next` → redirect 308;
  `GET /classic` (thêm rồi xoá theo yêu cầu); bỏ `app.mount("/static")`.
* Xoá 5 file giao diện vanilla — theo yêu cầu **“xóa cái giao diện classic kia”**.

### 3.2 Làm theo yêu cầu trực tiếp (dù là backend)
* **“thêm bộ lọc token trước khi Run”** → `web/prefilter.py` (410 dòng) + 3 endpoint
  `POST /api/prefilter`, `GET /api/prefilter/{id}`, `POST /api/prefilter/{id}/stop`.
* **“sao không push vào luôn để mỗi worker tự kiểm tra”** → `engine._run_task` gọi
  `prefilter.probe_token` ngay đầu task; `_finish_skipped()`; status `skipped`;
  `Job.skipped` + headline key `skipped`; `job_progress` gửi thêm `running/queued`.
* **“xem phân tích lịch sử”** → `analyze_history.py` (công cụ CLI, không chạy trong server).
* **“sửa lại dùng tiếng anh”** → đổi vài chuỗi thông báo backend hiện trên UI (disk guard,
  write-results, nhãn verdict của prefilter).

### 3.3 Tự làm thêm khi điều tra hiệu năng (không ai yêu cầu, để trả lời câu hỏi “Running 288/162”)
* `Job.snapshot()`: copy nông thay `deepcopy` → 0,163 s → 0,048 s cho 6.000 task.
* `Job.write_results()`: serialize + ghi file **ngoài** `job._lock`.
* `Job.request_checkpoint()` + `Job.start_checkpoint_thread()`: worker không tự ghi
  `results.json` trong `finally` nữa.
* `Job.running` / `Job.queued` (property) + `job_progress` mang thêm 2 số này.

## 4. Ảnh hưởng hành vi (cần biết)
1. Mặc định bật **prefilter trong worker** → mỗi token tốn thêm **1 GET** trước khi chạy.
   Tắt bằng công tắc *Filter tokens before run* (gửi `prefilter: false`).
2. Task có thể ở trạng thái mới **`skipped`** (acc Plus / bị khoá / token chết) — không tính
   là `fail`, không retry.
3. `GET /api/jobs` headline và `/api/state` có thêm field `skipped`; `job_progress` có thêm
   `running`/`queued`; `Task` có `skipped_reason`.
4. Không thay đổi giao thức với Stripe/OpenAI, không đổi cách tạo checkout, không đổi proxy.

## 5. Nếu muốn quay lại “chỉ frontend”
Có thể revert riêng phần 3.3 (hiệu năng) mà không ảnh hưởng gì tới FE, hoặc tắt hẳn prefilter
bằng cách bỏ `prefilter: true` trong payload (FE đang gửi theo công tắc). Phần 3.1 là bắt buộc
cho bất kỳ giao diện mới nào.
