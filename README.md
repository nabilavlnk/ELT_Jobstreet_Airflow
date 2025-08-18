# ELT Jobstreet Airflow

## 📌 Deskripsi Project
Project ini dibuat untuk mengolah data lowongan pekerjaan dari Jobstreet menggunakan ELT (Extract, Load, Transform). Data yang sudah diambil kemudian dimasukkan ke **database Impala** lalu divisualisasikan dengan Streamlit supaya lebih mudah dianalisis.

Workflow ini dijalankan dan diatur menggunakan **Apache Airflow** dalam container Docker.

---

## 📂 Alur Proses

### 1. **Extract**
Proses scraping data dari halaman Jobstreet menggunakan selenium.
- Informasi yang diambil:
  - **Job Title**
  - **Dates**
  - **Location**
  - **Type Work**
  - **Classification**
  - **Company**
  - **Salary** (Jika tersedia)

### 2. **Load**
Data hasil scraping disimpan sementara dalam format CSV, kemudian dimuat ke Impala agar bisa di-query untuk kebutuhan query analitik.

### 3. **Transform**
Tahap ini data diproses agar lebih rapi dan siap pakai, dengan langkah langkah:
- Standarisasi kolom.
- Mengambil rata rata salary.
- Menambahkan geolokasi (latitude & longitude) berdasarkan provinsi.
- Menghapus duplicates data by job_titles and company.

### 4. **Visualization**
Data yang sudah bersih kemudian divisualisasikan di Streamlit. Hasilnya berupa grafik dan chart interaktif untuk memahami tren lowongan pekerjaan di Indonesia.

---

📈 Sumber Data
- Website Jobstreet
- Geolokasi dari dataset provinsi Indonesia
