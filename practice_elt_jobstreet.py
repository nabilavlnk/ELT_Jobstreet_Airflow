from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta
from impala.dbapi import connect
import pandas as pd
import logging
import time
import shutil
import uuid
import os


# -------------------------- Environment Setup -------------------------- #
shutil.rmtree("/tmp/chrome-user-data", ignore_errors=True)

dotenv_path = Path(__file__).parent / "config.env"

if not dotenv_path.exists():
    print("⚠️ config.env NOT FOUND at:", dotenv_path)
else:
    print("✅ config.env FOUND at:", dotenv_path)

load_dotenv(dotenv_path)

# -------------------------- Function Code -------------------------- #
def fetch_html():
        filter = Variable.get("job_filter", default_var="")
        base_url = f'https://id.jobstreet.com/id/{filter}-jobs' if filter else 'https://id.jobstreet.com/id/jobs'
        path = "/usr/bin/chromedriver"
        service = Service(path)

        chrome_options = Options()
        user_data_path = f"/tmp/chrome-user-data-{uuid.uuid4()}"
        chrome_options.add_argument(f"--user-data-dir={user_data_path}")
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--remote-debugging-port=9222")

        os.system("pkill chrome || true")
        os.system("pkill chromedriver || true")

        time.sleep(3)
    
        job_titles = []
        location = []
        type_work = []
        classification = []
        company = []
        salary = []
        dates = []

        
        website = f'{base_url}'
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.get(website)
     
        for i in range(20):
           job_buttons = driver.find_elements(By.XPATH, '//article[@data-testid="job-card"]')

           if i >= len(job_buttons):
               print(f"Index {i} out of range, only {len(job_buttons)} jobs found.")
               break

           try:
               job_link = job_buttons[i].find_element(By.TAG_NAME, "a").get_attribute("href")

               # Buka tab baru
               driver.execute_script("window.open(arguments[0]);", job_link)
               driver.switch_to.window(driver.window_handles[1])

               wait = WebDriverWait(driver, 15)

               #title
               title_element = wait.until(
                   EC.presence_of_element_located((By.XPATH, '//h1[@data-automation="job-detail-title"]'))
               )

               time.sleep(1)
               
               #dates
               wait.until(EC.text_to_be_present_in_element((By.XPATH, "//span[contains(text(), 'Posted')]"), "Posted"))
               date_element = driver.find_element(By.XPATH, "//span[contains(text(), 'Posted')]")

               time.sleep(1)
               
               #type_work
               type_element = wait.until(
                   EC.presence_of_element_located((By.XPATH, '//span[@data-automation="job-detail-work-type"]'))
               )

               #location
               location_element = wait.until(
                   EC.presence_of_element_located((By.XPATH, '//span[@data-automation="job-detail-location"]'))
               )

               #classification
               classification_element = wait.until(
                   EC.presence_of_element_located((By.XPATH, '//span[@data-automation="job-detail-classifications"]'))
               )

               #company
               company_element = wait.until(
                   EC.presence_of_element_located((By.XPATH, '//span[@data-automation="advertiser-name"]'))
               )

               try:
                   salary_element = wait.until(
                   EC.presence_of_element_located((By.XPATH, '//span[@data-automation="job-detail-salary"]'))
                   )
                   salary_text = salary_element.text

               except:
                   salary_text = None
               
               time.sleep(2)

               title_text = title_element.text
               job_titles.append(title_text)

               date_text = date_element.text
               dates.append(date_text)

               location_text = location_element.text
               location.append(location_text)

               type_text = type_element.text
               type_work.append(type_text)

               classification_text = classification_element.text
               classification.append(classification_text)

               company_text = company_element.text
               company.append(company_text)

               salary.append(salary_text)

               print(f"{i+1}. {date_text}")

               driver.close()  
               driver.switch_to.window(driver.window_handles[0]) 
               time.sleep(2)

           except Exception as e:
               print(f"Error on job {i+1}: {e}")
               if len(driver.window_handles) > 1:
                   driver.close()
                   driver.switch_to.window(driver.window_handles[0])
            
        all_data = pd.DataFrame({
             'job_titles':job_titles
             ,'dates':dates
             ,'location':location
             ,'type_work':type_work
             ,'classification':classification
             ,'company':company
             ,'salary':salary
        })

        print(all_data.head())
        output = "/opt/airflow/output/scraped_data.csv"
        all_data.to_csv(output, index=False, mode='w')
        print("Data berhasil disimpan")

def impala_connection():
    return connect(
          host=os.getenv("HOST"), 
          port=21096, 
          use_ssl=True,
          database=os.getenv("DATABASE"),
          kerberos_service_name=os.getenv("KERBEROS_SERVICE"), 
          auth_mechanism = 'GSSAPI', 
          ca_cert=os.getenv("CA_CERT_PATH")
     )

def insert_to_impala():

    conn = impala_connection()
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO nabila.tmp_result_scrape_jobstreet PARTITION (load_date='{}')
        SELECT %s, %s, %s, %s, %s, %s, %s
        """.format(pd.Timestamp.now().strftime('%Y-%m-%d'))

    all_data = pd.read_csv('/opt/airflow/output/scraped_data.csv')
    all_data = all_data.astype(object).where(pd.notnull(all_data), None)
    cursor.executemany(insert_query, list(all_data.itertuples(index=False, name=None)))

    cursor.close()
    conn.close()

def transform_data():
     
     conn = impala_connection()
     cursor = conn.cursor()
     
     transform_query = """
                        WITH 
                        tmp_1 AS(
                        SELECT
                            job_titles
                            ,dates
                            ,CAST(REGEXP_EXTRACT(dates, 'Posted (\\\\d+)', 1) AS INT) AS time_value --angka
                            ,LOWER(REGEXP_EXTRACT(dates, 'Posted \\\\d+\\\\s+(\\\\w+)', 1)) AS time_unit --satuan
                            ,type_work
                            ,SPLIT_PART(`location`, ',', 1) AS city
                            ,SPLIT_PART(`location`, ',', 2) AS prov
                            ,REGEXP_EXTRACT(classification, '\\\\((.*?)\\\\)', 1) AS classification
                            ,company
                            ,salary
                            ,CASE 
                                WHEN salary IS NULL THEN 0
                                ELSE CAST(REGEXP_EXTRACT(REGEXP_REPLACE(SPLIT_PART(salary, '–', 1),'[^0-9]', ''),'([0-9]+)', 1) AS INT) 
                            END AS min_salary
                            ,CASE 
                                WHEN salary IS NULL THEN 0
                                ELSE CAST(REGEXP_EXTRACT(REGEXP_REPLACE(SPLIT_PART(salary, '–', 2),'[^0-9]', ''),'([0-9]+)', 1) AS INT) 
                                END AS max_salary
                        FROM nabila.tmp_result_scrape_jobstreet
                        ) 
                        ,tmp_2 AS(
                        SELECT
                            job_titles
                            ,dates
                            ,CASE
                                WHEN time_unit = 'detik' THEN NOW() -- anggap tetap hari ini
                                WHEN time_unit = 'menit' THEN NOW()
                                WHEN time_unit = 'jam' THEN NOW()
                                WHEN time_unit = 'hari' THEN DATE_SUB(NOW(), time_value)
                            ELSE DATE_SUB(NOW(), 31)
                            END AS posting_date
                            ,type_work
                            ,city
                            ,prov
                            ,classification
                            ,company
                            ,min_salary
                            ,max_salary
                            ,CAST((min_salary+max_salary)/2 as INT) AS avg_salary
                        FROM tmp_1
                        ) 
                        ,tmp_3 AS(
                        SELECT
                            job_titles
                            ,CAST(posting_date as date) as posting_date
                            ,type_work
                            ,city
                            ,UPPER(TRIM(prov)) AS prov
                            ,classification
                            ,company
                            ,min_salary
                            ,max_salary
                            ,avg_salary
                            ,ROW_NUMBER() OVER (PARTITION BY job_titles, company ORDER BY posting_date DESC) AS rn
                        FROM tmp_2
                        ) 
                        ,tmp_4 AS(
                        SELECT
                            job_titles
                            ,posting_date
                            ,type_work
                            ,city
                            ,CASE
                            WHEN TRIM(city) = 'Jakarta Raya' THEN 'DKI Jakarta'
                            WHEN TRIM(prov) = 'JAKARTA RAYA' THEN 'DKI Jakarta'
                            ELSE prov
                            END AS prov
                            ,classification
                            ,company
                            ,min_salary
                            ,max_salary
                            ,avg_salary
                        FROM tmp_3
                        WHERE rn = 1
                        ) 
                        ,tmp_5 AS(
                        SELECT
                            a.*
                            ,b.lat as lat
                            ,b.long as long
                        FROM tmp_4 a
                        LEFT JOIN nabila.long_lat_prov_ind b
                        ON LOWER(TRIM(a.prov)) = LOWER(TRIM(b.name))
                        )
                        INSERT OVERWRITE TABLE nabila.result_scrape_jobstreet
                        SELECT 
                            job_titles
                            ,posting_date
                            ,type_work
                            ,city
                            ,prov
                            ,CAST(lat AS FLOAT) lat
                            ,CAST(long AS FLOAT) long
                            ,classification
                            ,company
                            ,min_salary
                            ,max_salary
                            ,avg_salary
                        FROM tmp_5
                        """
     
     cursor.execute(transform_query)
     cursor.close()
     conn.close()

def save_data_csv():
     conn = impala_connection()
     cursor = conn.cursor()
     cursor.execute("""
                    SELECT * FROM nabila.result_scrape_jobstreet
                    """)
     
     rows = cursor.fetchall()
     columns = [col[0] for col in cursor.description]
     df = pd.DataFrame(rows, columns=columns)

     output = "/opt/airflow/output/data_jobstreet.csv"
     df.to_csv(output, index=False, mode='w')
     print("Data berhasil disimpan")
    
# -------------------------- Airflow DAG Ssetup -------------------------- #
default_args = {
    'owner':'nabila',
    'retries':3,
    'retry_delay':timedelta(minutes=5)
}

with DAG(dag_id='practice_elt_jobstreet',
         start_date=datetime(2025, 7, 1),
         schedule_interval="0 11 * * 1-5",
         catchup=False) as dag:
        
        fetch_html = PythonOperator(
        task_id='fetch_html',
        python_callable=fetch_html
        )

        insert_to_impala = PythonOperator(
        task_id='insert_to_impala',
        python_callable=insert_to_impala
        )

        transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
        )

        save_data_csv = PythonOperator(
        task_id='save_data_csv',
        python_callable=save_data_csv
        )

fetch_html >> insert_to_impala >> transform_data >> save_data_csv