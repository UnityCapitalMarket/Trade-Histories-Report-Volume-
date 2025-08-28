# Trade Histories Exporter (MySQL → CSV/JSONL)

Công cụ dòng lệnh xuất dữ liệu **TradeHistories** từ MySQL sang **CSV** hoặc **JSON Lines** theo đúng layout đã mô tả (khớp với `TradeRecord` trong C#). Hỗ trợ **Raw SQL mode** (nhập nguyên câu `SELECT ...`) và **Filter mode** (không cần tự viết SQL).

---

## Tính năng

* Nhận **câu `SELECT` bất kỳ** (chỉ `SELECT`) qua `--sql`/`--sql-file` và **parse** kết quả theo layout chuẩn.
* Chuyển đổi thời gian **BigIntHumanReadable** (`yyyyMMddHHmmss` hoặc `yyyyMMddHHmmssSSS`, UTC) → ISO-8601 `Z`.
* Xuất **CSV** (UTF‑8) hoặc **JSONL** ra stdout.
* Filter mode: lọc theo `TradeAccountID`, `Ticket`, `Symbol`, khoảng `OpenTime`/`CloseTime`, `Comment`, `limit/offset`, `order`.
* Bảo vệ an toàn: chỉ cho phép **SELECT**, xác thực **đủ cột** yêu cầu.

---

## Cấu trúc thư mục đề xuất

```text
trade-histories-exporter/
├─ app/
│  ├─ export_trade_histories.py   # entry chính (CLI)
│  └─ __init__.py
├─ sql/
│  └─ sample_query.sql            # ví dụ câu SELECT đầy đủ cột
├─ examples/
│  ├─ sample_output.csv
│  └─ sample_output.jsonl
├─ scripts/
│  ├─ run_export.sh               # ví dụ shell script
│  └─ run_export.bat              # ví dụ batch Windows
├─ tests/
│  ├─ test_datetime_conversion.py # unit tests (đề xuất)
│  └─ test_sql_mode.py            # unit tests (đề xuất)
├─ .env.example                   # mẫu biến môi trường DB_*
├─ requirements.txt               # mysql-connector-python, pytest (nếu test)
├─ README.md                      # tài liệu này
├─ LICENSE                        # tuỳ chọn
└─ .gitignore
```

> Nếu trước đó bạn dùng `app/trade_fetcher.py`, hãy đổi tên thành `app/export_trade_histories.py` cho thống nhất.

---

## Yêu cầu hệ thống

* Python 3.9+
* MySQL 5.7+/8.0 (hoặc MariaDB tương thích)
* Thư viện: `mysql-connector-python`

Cài đặt:

```bash
python -m venv .venv
. .venv/bin/activate         # Windows: .venv\Scripts\activate
pip install -r requirements.txt
# hoặc
pip install mysql-connector-python
```

`requirements.txt` mẫu:

```
mysql-connector-python>=8.0
pytest>=8.0  # nếu chạy tests
```

---

## Chạy nhanh (Quick Start)

### Raw SQL mode (khuyến nghị)

```bash
python app/export_trade_histories.py \
  --host 127.0.0.1 --port 3306 --user root --password 123456 --database mt4_db \
  --sql-file sql/sample_query.sql \
  --csv-out trades.csv
```

Hoặc inline:

```bash
python app/export_trade_histories.py --host 127.0.0.1 --user root --password 123456 --database mt4_db \
  --sql "SELECT ID, TradeAccountID, Ticket, SymbolName, Digits, Type, Quantity, State, OpenTime, OpenPrice, OpenRate, CloseTime, ClosePrice, CloseRate, StopLoss, TakeProfit, Expiration, Commission, CommissionAgent, Swap, Profit, Tax, Magic, Comment, TimeStamp FROM TradeHistories WHERE TradeAccountID=111" \
  --csv-out trades.csv
```

> **Lưu ý PowerShell/Windows:** dùng dấu nháy kép `"` cho `--sql`, lưu ý escape nếu có ký tự đặc biệt. Với câu lệnh dài, nên dùng `--sql-file`.

### Filter mode (không cần tự viết SQL)

```bash
python app/export_trade_histories.py --host 127.0.0.1 --user root --password 123456 --database mt4_db \
  --account-id 111 --symbol EURUSD \
  --opened-from 2023-02-09T00:00:00Z --opened-to 2023-02-10T00:00:00Z \
  --limit 100 --order-by OpenTime --order-dir ASC \
  --csv-out trades.csv
```

Xuất JSONL thay vì CSV:

```bash
python app/export_trade_histories.py ... --jsonl
```

---

## Biến môi trường (tuỳ chọn)

Có thể cấu hình qua ENV thay vì arguments:

```
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=123456
DB_NAME=mt4_db
```

File `.env.example` minh hoạ các biến trên.

---

## Layout bắt buộc (cột phải có trong kết quả SELECT)

```
ID, TradeAccountID, Ticket, SymbolName, Digits, Type, Quantity, State,
OpenTime, OpenPrice, OpenRate, CloseTime, ClosePrice, CloseRate,
StopLoss, TakeProfit, Expiration, Commission, CommissionAgent, Swap,
Profit, Tax, Magic, Comment, TimeStamp
```

* `Type`, `State`: dạng số nguyên (enum như C#).
* `OpenTime`, `CloseTime`, `Expiration`, `TimeStamp`: **BigIntHumanReadable**.
* `IsClosed` được tính trong app: `CloseTime >= OpenTime`.

Ví dụ dòng dữ liệu mẫu:

```
'9209','111','3599795','EURUSD','5','0','0','5','20230209084334000','1.07351','1.07351','20230209090257000','1.07351','0','0','0','19700101000000000','0','0','0','0','0','3599793','close hedge by #3599791','20230209090257000'
```

---

## Quy tắc thời gian (BigIntHumanReadable)

* Chấp nhận độ dài **14** (`yyyyMMddHHmmss`) hoặc **17** (`yyyyMMddHHmmssSSS`).
* Múi giờ: **UTC**. Xuất ra ISO‑8601 dạng `YYYY-MM-DDTHH:MM:SS(.mmm)Z`.
* Sentinel/không set: `0` hoặc `19700101000000000` → `null` trong JSON / rỗng trong CSV.

---

## Tuỳ chọn CLI

```
--host, --port, --user, --password, --database
--sql, --sql-file                 # Raw SQL mode (chỉ SELECT)
--csv-out PATH                    # Xuất CSV; nếu không set → in JSONL ra stdout
--jsonl                           # In JSONL ra stdout

# Filter mode (khi không dùng --sql/--sql-file)
--account-id INT
--ticket INT
--symbol TEXT
--opened-from ISO8601Z
--opened-to ISO8601Z
--closed-from ISO8601Z
--closed-to ISO8601Z
--comment-like TEXT
--limit INT (1..10000, mặc định 100)
--offset INT (>=0)
--order-by [ID|OpenTime|CloseTime|TimeStamp|Ticket]
--order-dir [ASC|DESC]
```

---

## Bảo mật & Thực hành tốt

* Tránh ghi password trong lịch sử shell; có thể dùng biến môi trường hoặc file `.env` (không commit).
* `--sql/--sql-file` chỉ chấp nhận **SELECT**; app từ chối `INSERT/UPDATE/DELETE`.
* Với câu `SELECT *`, **không nên** dùng – hãy liệt kê đủ cột theo layout.

---

## Xử lý sự cố (Troubleshooting)

* **SyntaxError: unterminated string literal** khi chạy Windows: do dòng `"\n"` trong code bị xuống dòng. Đã sửa ở hàm `_strip_leading_comments`. Hãy pull bản mới nhất.
* **Thiếu cột bắt buộc**: app sẽ báo tên các cột còn thiếu; sửa lại câu `SELECT`.
* **Sai định dạng thời gian**: kiểm tra cột thời gian là 14 hoặc 17 ký tự số, ví dụ `20230209084334000`.
* **PowerShell escape**: ưu tiên `--sql-file` để tránh lỗi trích dẫn.

---

## Phát triển

* Chạy kiểm thử (đề xuất dùng `pytest`):

```bash
pytest -q
```

* Style: PEP8, type hints. Mục tiêu mã rõ ràng, dễ bảo trì.

---

## Giấy phép

Chọn giấy phép phù hợp (MIT/GPL/Apache-2.0). Thêm file `LICENSE` ở gốc repo.

---

## Góp ý

Mở issue/PR trên GitHub. Mọi phản hồi đều được hoan nghênh!
