# TheGuardianProject

**End-to-end Data Engineering Pipeline for The Guardian**  
Automates ingestion, transformation, and analysis of news data, with a demo chatbot for semantic retrieval.

## 🌟 Giới thiệu

TheGuardianProject là một dự án data engineering hoàn chỉnh được xây dựng dựa trên 3 công nghệ chính: **Airflow**, **dbt**, và **Postgres**. Dự án cho phép tự động hóa các bước thu thập, xử lý và phân tích dữ liệu tin tức từ The Guardian. Ngoài ra, hệ thống còn cung cấp một chatbot demo giúp truy vấn dữ liệu ngữ nghĩa (semantic retrieval).

## 🚀 Tính năng chính

- **Ingestion (Thu thập dữ liệu):**  
  Tự động tải dữ liệu tin tức từ API của The Guardian hoặc nguồn lưu trữ mẫu.

- **ETL & Transformation:**  
  Sử dụng Airflow để điều phối luồng dữ liệu và dbt để chuyển đổi/chuyển hóa dữ liệu thành dạng phân tích thuận tiện hơn.

- **Data Analytics:**  
  Lưu trữ dữ liệu trên Postgres, có thể dùng notebook hoặc công cụ BI để phân tích dữ liệu.

- **Chatbot Semantic Retrieval:**  
  Một chatbot demo dùng NLP để truy vấn các thông tin ngữ nghĩa từ kho dữ liệu báo chí đã xử lý.

## 🛠️ Công nghệ sử dụng

- **Apache Airflow** – Điều phối ETL pipeline.
- **dbt (data build tool)** – Quản lý và chuyển đổi dữ liệu.
- **Postgres** – Hệ quản trị cơ sở dữ liệu mở lưu trữ dữ liệu đã xử lý.
- 
## 📁 Cấu trúc repo 

```
.
├── airflow/            # DAG và cấu hình liên quan đến Airflow
├── dbt/                # Dự án dbt và models chuyển đổi dữ liệu
├── docker/             # Set up image postgres
├── src/                # Script và các job của pipeline
├── requirements.txt    # Các thư viện Python cần thiết
├── README.md           # Tài liệu dự án
```

## ⚡ Hướng dẫn cài đặt & chạy thử

### 1. Clone repo:

```bash
git clone https://github.com/DangVanVy23521825/TheGuardianProject.git
cd TheGuardianProject
```

### 2. Cài đặt Python & các package yêu cầu

```bash
pip install -r requirements.txt
```

### 3. Khởi động các thành phần:

- **Postgres:**  
  Cài đặt hoặc khởi tạo PostgreSQL, tạo database cho dự án này.

- **Airflow:**  
  Khởi tạo các dịch vụ Airflow, cấu hình kết nối với Postgres.

```bash
# Ví dụ (chạy bằng docker-compose hay trực tiếp, tuỳ cấu hình trong repo)
airflow initdb
airflow webserver &
airflow scheduler &
```

- **dbt:**  
  Thiết lập kết nối với Postgres và chạy các lệnh chuyển đổi.

```bash
dbt run
```

- **Notebook & Chatbot:**  
  Vào thư mục notebook và mở file trên Jupyter để trải nghiệm truy vấn dữ liệu hoặc chatbot.

## 📣 Đóng góp

Mọi đóng góp mở rộng chức năng hoặc cải thiện pipeline đều được hoan nghênh! Hãy tạo issue hoặc gửi pull request qua GitHub.

**Tác giả:** [DangVanVy23521825](https://github.com/DangVanVy23521825)
