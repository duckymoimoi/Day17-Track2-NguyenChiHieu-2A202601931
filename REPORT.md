# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Chí Hiếu  **Lớp:** AICB-P2T2  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Output nguyên văn của ba lượt chạy</summary>

```text
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LAB 17 · make verify
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
run 1/3 … 94.9s
run 2/3 … 64.5s
run 3/3 … 63.3s

BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
──────────────────────────────────────────────────────────────────────────
gold_training_set     ✓ ok              12,480      12,480   ✓
gold_feature_daily    ✓ ok               9,100       9,100   ✓
gold_doc_chunks       ✓ ok              31,200      31,200   ✓
quarantine_tickets    ✓ ok                 312         312   ✓

CHECKSUM từng lượt
──────────────────────────────────────────────────────────────────────────
gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

KIỂM TRA KHÁC
──────────────────────────────────────────────────────────────────────────
dbt test                                    ✓ 11/11 pass
silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
  số file parquet                           ✓ 5,000 → 14
  kết quả truy vấn không đổi                ✓
DAG: catchup / max_active_runs              ✓ False / 1

TỔNG KẾT
──────────────────────────────────────────────────────────────────────────
✓  1 · gold_training_set idempotent & đúng số hàng
✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
✓  3 · contract + quarantine + dbt test
✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
──────────────────────────────────────────────────────────────────────────
4/4 tiêu chí đạt
```

</details>

Tổng kết: **4/4 tiêu chí đạt**.

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Retry cùng pipeline làm `gold_training_set` tăng lên 38.750 hàng và một `ticket_id` xuất hiện nhiều lần, dù `silver_tickets` chỉ giữ một trạng thái cho mỗi ticket. |
| **Nguyên nhân** | Model incremental không khai báo khóa nên dbt sinh câu `INSERT` thuần; retry hoặc backfill cùng dữ liệu CDC ghi thêm entity ticket thay vì ghi đè trạng thái cũ. |
| **Cách sửa** | Trong `dbt/models/gold/gold_training_set.sql`, dùng `unique_key='ticket_id'` và `incremental_strategy='merge'`; trong `dags/ai_training_pipeline.py`, đặt `catchup=False`, `max_active_runs=1`. |
| **Bằng chứng** | Trước: 38.750 hàng · sau: 12.480 hàng/12.480 ticket · checksum ba lượt đều `8dd7c98653`. |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` chỉ có 8.645 thay vì 9.100 cặp `(event_date, customer_id)`; 455 cặp ngày cũ chỉ có event tới muộn bị bỏ sót. |
| **Nguyên nhân** | Filter `event_date > max(event_date)` lấy watermark theo thời gian xảy ra sự kiện nên khi event ngày 12/08 tới kho vào 15/08, watermark đã đi qua ngày 12/08 và row đó không lọt vào lần chạy nào. |
| **Cách sửa** | Trong `gold_feature_daily.sql`, lùi watermark **3 ngày** theo `ceil(P99)` và merge theo khóa ghép `['event_date', 'customer_id']` để lần tính lại thay thế kết quả cũ. |
| **Bằng chứng** | P99 đo được: **2,723542 ngày** · trước: 8.645 hàng · sau: 9.100 hàng · checksum ba lượt đều `3db448685c`. |

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Pipeline không dừng sau khi source đổi schema ngày 10/08 nhưng classifier dự đoán kém; Silver có 6.606 hàng priority sai và quarantine vẫn rỗng. |
| **Nguyên nhân** | Source đổi cách biểu diễn từ số sang nhãn nhưng logic chỉ cast kiểu, khiến nhãn hợp lệ thành NULL và số ngoài miền vẫn được nhận trong khi contract và domain test chưa được bật. |
| **Cách sửa** | Trong `normalize_priority.sql`, giữ `1..4`, map `urgent/high/medium/low` về `1/2/3/4`, trả NULL cho giá trị lỗi; trong các model Silver, lọc lỗi trước khi xếp hạng, đưa chúng vào quarantine, bật contract và test miền giá trị. |
| **Bằng chứng** | Trước: 6.606 hàng priority sai, quarantine = 0 · sau: quarantine = 312, Silver vẫn đủ 12.480 ticket, priority sạch, `dbt test` 11/11 pass. |

---

## 4 · Bài mở rộng trong EXTRA.md

### Bài A — Tối ưu dashboard Parquet

| | |
|---|---|
| **Triệu chứng** | Dashboard phải mở 5.000 file Parquet và quét 5.000.000 đơn vị dữ liệu cho một truy vấn theo khách hàng/ngày. |
| **Nguyên nhân** | 5.000 file nhỏ không mang thông tin filter trong path buộc engine mở toàn bộ; `strftime(event_time, ...)` là predicate không sargable nên không dùng được partition pruning hay min/max statistics. |
| **Cách sửa** | Compact thành 14 partition theo `event_date`, sắp theo `event_date, customer_name, event_time`, row group 5.000; query dùng hive partitioning và `event_date = DATE '2026-08-09'`. |
| **Bằng chứng** | Rows scanned: 5.000.000 → 9.324, giảm 536,3× · file: 5.000 → 14 · rows on disk: 130.683 → 130.683 · result hash giữ nguyên `4379e4c5d9f3`. |

### Bài B — Consumer gặp sự cố giữa batch

| | |
|---|---|
| **Triệu chứng** | Consumer bị kill giữa batch làm offset đã dịch dù batch hiện tại chưa được ghi, nên restart làm mất message. |
| **Nguyên nhân** | Commit offset trước durable write tạo at-most-once nên crash giữa hai thao tác làm restart bỏ qua batch chưa ghi và mất dữ liệu. |
| **Cách sửa** | Ghi trước rồi commit offset; đặt `event_id` làm primary key; bulk upsert bằng `ON CONFLICT DO UPDATE` để replay cập nhật nội dung mới thay vì giữ bản cũ như `DO NOTHING`. |
| **Bằng chứng** | Chạy thẳng: 20.000/20.000 · crash batch 7: offset 3.000 · restart đọc lại batch: cuối cùng 20.000 hàng/20.000 event_id, không mất và không trùng · `make crash-test`: ĐẠT. |
