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
| **Nguyên nhân** | Model incremental không khai báo `unique_key` và strategy nên dbt sinh phép append. Vì grain là entity ticket và nguồn CDC có cả create/update ở nhiều ngày, retry hoặc backfill ghi thêm cùng entity thay vì thay thế trạng thái cũ. `catchup=True` và không giới hạn concurrent DAG run làm lỗi dễ bị kích hoạt đồng thời, nhưng không phải cơ chế tạo bản sao. |
| **Cách khắc phục** | Trong `dbt/models/gold/gold_training_set.sql`, dùng `unique_key='ticket_id'` và `incremental_strategy='merge'`. Trong `dags/ai_training_pipeline.py`, đặt `catchup=False`, `max_active_runs=1`. |
| **Bằng chứng** | Trước: 38.750 hàng · sau: 12.480 hàng/12.480 ticket · checksum ba lượt đều `8dd7c98653`. |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` chỉ có 8.645 thay vì 9.100 cặp `(event_date, customer_id)`; 455 cặp ngày cũ chỉ có event tới muộn bị bỏ sót. |
| **P99 độ trễ đo được** | **2,723542 ngày** (khoảng 65,365 giờ). P50 = 0,128576 ngày; P95 = 1,801146 ngày; max = 2,944688 ngày; tỷ lệ trễ trên một ngày = 5,0037%. |
| **Lookback đã chọn** | **3 ngày**, bằng cách làm tròn P99 lên số ngày nguyên. Window này cũng bao phủ max 2,944688 ngày của bộ dữ liệu hiện tại. |
| **Nguyên nhân** | Filter `event_date > max(event_date)` lấy watermark theo thời gian xảy ra sự kiện, không phải thời gian tới kho. Khi event ngày 12/08 tới vào 15/08, watermark đã đi qua ngày 12/08 nên row đó không lọt vào bất kỳ lần chạy sau nào. |
| **Cách khắc phục** | Lùi watermark ba ngày trong `gold_feature_daily.sql`, đồng thời merge theo composite key `['event_date', 'customer_id']` để các cặp được tính lại thay thế kết quả cũ thay vì cộng dồn. |
| **Bằng chứng** | Trước: 8.645 hàng · sau: 9.100 hàng · checksum ba lượt đều `3db448685c`. |

P99 đại diện cho SLO độ trễ thông thường và tránh để một outlier hiếm làm window tăng vô hạn; `max` nhạy với outlier và buộc mọi lượt chạy tương lai quét lại nhiều partition hơn. Mỗi ngày lookback bổ sung làm tăng chi phí đọc, group và merge ở mọi lượt chạy. Với dữ liệu này, `ceil(P99)=3` cũng bao phủ giá trị max; event vượt SLO trong tương lai cần cảnh báo và backfill có kiểm soát.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | `try_cast` biến các nhãn mới thành NULL nhưng lại chấp nhận `0`, `5`, `-1`; contract đang tắt nên pipeline vẫn chạy, trong khi 6.606 hàng Silver có priority sai và quarantine rỗng. |
| **Nguyên nhân** | Source đổi representation từ số sang nhãn nhưng logic chỉ cast kiểu, không xử lý schema evolution và cũng không kiểm tra miền 1..4. Vì contract bị tắt và thiếu domain test, lỗi semantic không làm dbt báo lỗi. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | `1..4`: giữ nguyên · `urgent/high/medium/low`: map thành `1/2/3/4` · `P1`, `P2`, `unknown`, `0`, `5`, `-1`, rỗng, NULL: loại khỏi Silver và đưa đúng bản ghi CDC vào quarantine. |
| **Cách khắc phục** | Chuẩn hóa bằng một macro dùng chung; lọc row lỗi trước khi xếp hạng CDC; quarantine khi macro trả NULL; bật contract; thêm `not_null` và `accepted_values [1,2,3,4]`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng · `silver_tickets` vẫn đủ 12.480 ticket · priority sạch · `dbt test` 11/11 pass. |

Bronze nên giữ nguyên dữ liệu nguồn để bảo toàn dấu vết điều tra và cho phép replay. Silver là nơi thực thi contract, chuẩn hóa schema evolution và định tuyến row lỗi. Không nên dừng cả DAG vì 312 bản ghi hỏng sẽ chặn hơn 130.000 event và 31.200 chunk hợp lệ; quarantine tạo hàng đợi xử lý riêng mà vẫn duy trì dịch vụ.

---

## 4 · Bài mở rộng trong EXTRA.md

### Bài A — Tối ưu dashboard Parquet

| | |
|---|---|
| **Bài đã làm** | A |
| **Nguyên nhân** | 5.000 file nhỏ không mang thông tin filter trong path buộc engine mở toàn bộ; `strftime(event_time, ...)` là predicate không sargable nên không dùng được partition pruning hay min/max statistics. |
| **Cách khắc phục** | Compact thành 14 partition theo `event_date`, sắp theo `event_date, customer_name, event_time`, row group 5.000; query dùng hive partitioning và `event_date = DATE '2026-08-09'`. |
| **Bằng chứng** | Rows scanned: 5.000.000 → 9.324, giảm 536,3× · file: 5.000 → 14 · rows on disk: 130.683 → 130.683 · result hash giữ nguyên `4379e4c5d9f3`. |

### Bài B — Consumer gặp sự cố giữa batch

| | |
|---|---|
| **Bài đã làm** | B |
| **Nguyên nhân** | Commit offset trước khi durable write tạo at-most-once: crash ở giữa hai thao tác làm restart bỏ qua batch chưa ghi và mất dữ liệu. Đổi write trước/commit sau tạo at-least-once nên batch có thể được replay. |
| **Cách khắc phục** | Ghi trước rồi commit offset; đặt `event_id` làm primary key; bulk upsert từng batch bằng `ON CONFLICT DO UPDATE`. Chọn `DO UPDATE` vì message replay có thể mang nội dung đã sửa và target cần phản ánh phiên bản mới; `DO NOTHING` sẽ giữ phiên bản cũ. |
| **Bằng chứng** | Chạy thẳng: 20.000/20.000 · crash batch 7: offset 3.000 · restart đọc lại batch: cuối cùng 20.000 hàng/20.000 event_id, không mất và không trùng · `make crash-test`: ĐẠT. |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Xác định grain, natural key và write strategy trước khi tin rằng retry là an toàn. |
| 2 | Đo chênh lệch event time/ingestion time và kiểm tra watermark có bỏ sót late data không. |
| 3 | So sánh phân bố raw với contract, tách schema evolution khỏi dữ liệu hỏng và giữ raw data để điều tra. |
