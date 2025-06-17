# HƯỚNG DẪN SỬ DỤNG NHANH

## Bước 1: Chuẩn bị danh sách URL
- Mở file `urls.txt`
- Thêm các URL masothue.com cần crawl (mỗi URL một dòng)
- Ví dụ:
```
https://masothue.com/4601034603-cong-ty-co-phan-do-dac-va-ban-do-thai-an
https://masothue.com/url-khac-1
https://masothue.com/url-khac-2
```

## Bước 2: Chạy chương trình
Có 3 cách:

### Cách 1: Double-click file batch
- Double-click file `run_crawler.bat`

### Cách 2: Chạy từ Command Prompt
```cmd
cd /d "e:\Work\crawl"
python masothue_crawler_advanced.py
```

## Bước 3: Xem kết quả
- File Excel sẽ được tạo với tên `masothue_companies_YYYYMMDD_HHMMSS.xlsx`
- Mở file để xem thông tin đã crawl

## Lưu ý quan trọng
- Đảm bảo có kết nối internet
- Không crawl quá nhiều URL cùng lúc để tránh bị block
- Chương trình tự động có delay 2 giây giữa các request
