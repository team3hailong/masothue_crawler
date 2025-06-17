import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from urllib.parse import urljoin
import os

class MasothueCrawler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.results = []
    
    def extract_company_info(self, url):
        """Trích xuất thông tin công ty từ một URL masothue.com"""
        try:
            print(f"Đang crawl: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Tìm bảng chứa thông tin công ty
            tbody = soup.find('tbody')
            if not tbody:
                print(f"Không tìm thấy bảng thông tin cho URL: {url}")
                return None
            
            # Dictionary để lưu thông tin
            company_info = {
                'URL': url,
                'Mã số thuế': '',
                'Địa chỉ': '',
                'Người đại diện': '',
                'Điện thoại': '',
                'Ngày hoạt động': '',
                'Quản lý bởi': '',
                'Loại hình DN': '',
                'Tình trạng': '',
                'Ngành nghề chính': ''
            }
            
            # Lấy thông tin từ các hàng trong bảng
            rows = tbody.find_all('tr')
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 2:
                    # Lấy text của cột đầu tiên (label)
                    label_cell = cells[0]
                    value_cell = cells[1]
                    
                    # Loại bỏ icon và lấy text
                    label_text = label_cell.get_text(strip=True)
                    
                    # Xử lý từng loại thông tin
                    if 'Mã số thuế' in label_text:
                        span = value_cell.find('span', class_='copy')
                        if span:
                            company_info['Mã số thuế'] = span.get_text(strip=True)
                    
                    elif 'Địa chỉ' in label_text:
                        span = value_cell.find('span', class_='copy')
                        if span:
                            company_info['Địa chỉ'] = span.get_text(strip=True)
                    
                    elif 'Người đại diện' in label_text:
                        # Tìm tên trong thẻ span hoặc a
                        name_element = value_cell.find('span', itemprop='name')
                        if name_element:
                            a_tag = name_element.find('a')
                            if a_tag:
                                company_info['Người đại diện'] = a_tag.get_text(strip=True)
                            else:
                                company_info['Người đại diện'] = name_element.get_text(strip=True)
                    
                    elif 'Điện thoại' in label_text:
                        span = value_cell.find('span', class_='copy')
                        if span:
                            company_info['Điện thoại'] = span.get_text(strip=True)
                    
                    elif 'Ngày hoạt động' in label_text:
                        span = value_cell.find('span', class_='copy')
                        if span:
                            company_info['Ngày hoạt động'] = span.get_text(strip=True)
                    
                    elif 'Quản lý bởi' in label_text:
                        span = value_cell.find('span', class_='copy')
                        if span:
                            company_info['Quản lý bởi'] = span.get_text(strip=True)
                    
                    elif 'Loại hình DN' in label_text:
                        a_tag = value_cell.find('a')
                        if a_tag:
                            company_info['Loại hình DN'] = a_tag.get_text(strip=True)
                    
                    elif 'Tình trạng' in label_text:
                        a_tag = value_cell.find('a')
                        if a_tag:
                            company_info['Tình trạng'] = a_tag.get_text(strip=True)
                    
                    elif 'Ngành nghề chính' in label_text:
                        # Lấy toàn bộ text, bao gồm cả mô tả trong ngoặc
                        text_content = value_cell.get_text(separator=' ', strip=True)
                        # Loại bỏ các khoảng trắng thừa
                        company_info['Ngành nghề chính'] = re.sub(r'\s+', ' ', text_content)
            
            print(f"✓ Crawl thành công: {company_info['Mã số thuế']}")
            return company_info
            
        except Exception as e:
            print(f"✗ Lỗi khi crawl {url}: {str(e)}")
            return None
    
    def load_urls_from_file(self, filename='urls.txt'):
        """Đọc danh sách URL từ file"""
        urls = []
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    # Bỏ qua dòng trống và dòng comment
                    if line and not line.startswith('#'):
                        if line.startswith('https://masothue.com'):
                            urls.append(line)
                        else:
                            print(f"⚠ Bỏ qua URL không hợp lệ: {line}")
        except FileNotFoundError:
            print(f"✗ Không tìm thấy file {filename}")
            return []
        except Exception as e:
            print(f"✗ Lỗi khi đọc file {filename}: {str(e)}")
            return []
        
        return urls
    
    def crawl_multiple_urls(self, urls):
        """Crawl nhiều URL và lưu kết quả"""
        for i, url in enumerate(urls, 1):
            print(f"\n[{i}/{len(urls)}] Đang xử lý...")
            
            info = self.extract_company_info(url)
            if info:
                self.results.append(info)
            
            # Thêm delay để tránh bị block
            time.sleep(2)
        
        print(f"\n✓ Hoàn thành! Đã crawl thành công {len(self.results)}/{len(urls)} công ty.")
    
    def save_to_excel(self, filename='masothue_data.xlsx'):
        """Lưu kết quả ra file Excel"""
        if not self.results:
            print("Không có dữ liệu để lưu!")
            return
        
        df = pd.DataFrame(self.results)
        
        # Sắp xếp các cột theo thứ tự mong muốn
        column_order = [
            'Mã số thuế', 'Địa chỉ', 'Người đại diện', 'Điện thoại', 
            'Ngày hoạt động', 'Quản lý bởi', 'Loại hình DN', 
            'Tình trạng', 'Ngành nghề chính', 'URL'
        ]
        
        df = df[column_order]
        
        # Lưu ra file Excel
        df.to_excel(filename, index=False, engine='openpyxl')
        print(f"✓ Đã lưu dữ liệu ra file: {filename}")
        print(f"Tổng số công ty: {len(self.results)}")


def main():
    print("=== MASOTHUE.COM CRAWLER ===")
    
    # Tạo crawler
    crawler = MasothueCrawler()
    
    # Đọc URL từ file
    urls = crawler.load_urls_from_file('urls.txt')
    
    if not urls:
        print("Không có URL nào để crawl!")
        print("Vui lòng thêm URL vào file 'urls.txt'")
        return
    
    print(f"Số lượng URL cần crawl: {len(urls)}")
    
    # Hiển thị danh sách URL
    print("\nDanh sách URL sẽ được crawl:")
    for i, url in enumerate(urls, 1):
        print(f"  {i}. {url}")
    
    # Xác nhận trước khi bắt đầu
    response = input(f"\nBạn có muốn tiếp tục crawl {len(urls)} URL? (y/n): ").strip().lower()
    if response not in ['y', 'yes', 'có']:
        print("Đã hủy!")
        return
    
    # Bắt đầu crawl
    crawler.crawl_multiple_urls(urls)
    
    # Lưu kết quả ra Excel
    if crawler.results:
        # Tạo tên file với timestamp
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"masothue_companies_{timestamp}.xlsx"
        crawler.save_to_excel(filename)
    
    print("\n=== HOÀN THÀNH ===")


if __name__ == "__main__":
    main()
