import os
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.decorators import apply_defaults

# ── Environment ───────────────────────────────────────────────────────────
gcp_project_id = os.environ.get("GCP_PROJECT_ID", "boston-rideshare-analytics")
gcs_bucket     = os.environ.get("GCS_BUCKET", "boston-rideshare")
bq_dataset     = os.environ.get("BQ_DATASET", "rideshare_dw")
bq_table       = os.environ.get("BQ_EXTERNAL_TABLE", "external_rideshare_events")
kafka_servers  = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
kafka_topic    = os.environ.get("KAFKA_TOPIC", "rideshare_events")

DBT_DIR      = "/opt/airflow/dbt"
DBT_BIN      = "/home/airflow/.local/bin/dbt"
# SỬA DÒNG NÀY:
DBT_ENV      = {**os.environ, "DBT_KEYFILE": "/opt/gcp/service-account.json", "GCP_LOCATION": "us-east1"}
DBT_BASE_CMD = f"{DBT_BIN}"
DBT_FLAGS    = f"--project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"
GCS_KEY_PATH = "/opt/gcp/service-account.json"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "boston_rideshare_pipeline",
    default_args=default_args,
    description="ELT: Extract (Kafka) → Load (Spark→GCS) → Staging (dbt stg) → Transform (dbt core/marts + DQ)",
    schedule_interval="@hourly",
    catchup=False,
    tags=["rideshare", "extract", "load", "staging", "transform", "data-quality"],
)


# ══════════════════════════════════════════════════════════════════════════════
# CUSTOM SENSOR: KafkaTopicSensor
# Dùng confluent_kafka (đã có trong image vì producer dùng nó).
# Poke mỗi 60s, timeout sau 10 phút.
# ══════════════════════════════════════════════════════════════════════════════
class KafkaTopicSensor(BaseSensorOperator):
    """
    Sensor chờ Kafka topic có ít nhất `min_messages` messages.
    Dùng confluent_kafka.Consumer để lấy watermark offsets.
    """
    @apply_defaults
    def __init__(self, bootstrap_servers, topic, min_messages=100, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.min_messages = min_messages

    def poke(self, context):
        from kafka import KafkaConsumer
        from kafka.errors import KafkaError

        try:
            # Kết nối tới Kafka bằng kafka-python-ng
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id='airflow-extract-sensor',
                enable_auto_commit=False
            )
            
            # Kiểm tra xem topic đã tồn tại chưa
            topics = consumer.topics()
            if self.topic not in topics:
                self.log.info(f"[Extract] Topic '{self.topic}' chưa tồn tại. Đợi...")
                consumer.close()
                return False

            # Lấy thông tin các partition của topic
            partitions = consumer.partitions_for_topic(self.topic)
            if not partitions:
                consumer.close()
                return False

            from kafka import TopicPartition
            total = 0
            
            # Tính số lượng message dựa trên đầu và cuối offset của từng partition
            for pid in partitions:
                tp = TopicPartition(self.topic, pid)
                # Lấy offset thấp nhất (earliest) và cao nhất (latest)
                beginning_offsets = consumer.beginning_offsets([tp])
                end_offsets = consumer.end_offsets([tp])
                
                low = beginning_offsets.get(tp, 0)
                high = end_offsets.get(tp, 0)
                total += (high - low)
                
            consumer.close()

            self.log.info(f"[Extract] Topic '{self.topic}' có {total} messages (cần {self.min_messages}).")
            return total >= self.min_messages

        except Exception as e:
            self.log.warning(f"[Extract] Kafka chưa sẵn sàng hoặc lỗi: {e}. Thử lại...")
            return False

# ══════════════════════════════════════════════════════════════════════════════
# CUSTOM SENSOR: GCSBronzeSensor
# Chờ Spark ghi ít nhất 1 file Parquet vào GCS Bronze trong giờ hiện tại.
# ══════════════════════════════════════════════════════════════════════════════
class GCSBronzeSensor(BaseSensorOperator):
    """
    Sensor thông minh: Chỉ Pass nếu có file Parquet mới tinh 
    được tạo ra trong vòng X phút đổ lại đây (ví dụ: 15 phút).
    """
    @apply_defaults
    def __init__(self, bucket, prefix, key_path, within_minutes=15, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.key_path = key_path
        self.within_minutes = within_minutes 

    def poke(self, context):
        from google.cloud import storage
        from google.oauth2 import service_account
        from datetime import datetime, timezone, timedelta

        try:
            creds = service_account.Credentials.from_service_account_file(self.key_path)
            client = storage.Client(credentials=creds)
            bucket = client.bucket(self.bucket)

            # Lấy danh sách các file trong thư mục
            blobs = bucket.list_blobs(prefix=self.prefix)
            
            now = datetime.now(timezone.utc)
            boundary_time = now - timedelta(minutes=self.within_minutes)

            # Quét xem có file nào được tạo sau mốc thời gian boundary_time không
            for blob in blobs:
                # Bỏ qua nếu là folder ảo hoặc không phải file parquet
                if blob.name.endswith('/') or not blob.name.endswith('.parquet'):
                    continue
                
                if blob.updated >= boundary_time:
                    self.log.info(f"[Load][OK] Phát hiện dữ liệu MỚI TIẾP CẬN! File: {blob.name} (Cập nhật lúc: {blob.updated})")
                    return True

            self.log.info(f"[Load][WAIT] Không có file Parquet nào mới trong {self.within_minutes} phút qua. Đợi Spark Streaming ghi thêm...")
            return False

        except Exception as e:
            self.log.warning(f"[Load] Lỗi khi check GCS: {e}. Thử lại...")
            return False


# ══════════════════════════════════════════════════════════════════════════════
# GROUP 1 – EXTRACT
# Sensor chờ Kafka Producer đã publish data vào topic.
# Producer chạy liên tục trong container riêng (boston_producer).
# ══════════════════════════════════════════════════════════════════════════════
with TaskGroup("extract", tooltip="Sensor: chờ Kafka Producer publish data vào topic", dag=dag) as tg_extract:

    wait_for_kafka_data = KafkaTopicSensor(
        task_id="wait_for_kafka_data",
        bootstrap_servers=kafka_servers,
        topic=kafka_topic,
        min_messages=100,          # cần ít nhất 100 messages mới tiến hành Load
        mode="poke",
        poke_interval=60,          # check mỗi 60 giây
        timeout=600,               # timeout sau 10 phút
        dag=dag,
    )


# ══════════════════════════════════════════════════════════════════════════════
# GROUP 2 – LOAD
# Sensor chờ Spark Streaming đã ghi Parquet lên GCS Bronze.
# Spark chạy liên tục trong container boston_spark_master.
# ══════════════════════════════════════════════════════════════════════════════
with TaskGroup("load", tooltip="Sensor: chờ Spark ghi Parquet lên GCS Bronze", dag=dag) as tg_load:

    wait_for_gcs_bronze = GCSBronzeSensor(
        task_id="wait_for_gcs_bronze",
        bucket=gcs_bucket,
        prefix="bronze/rideshare_events/",
        key_path=GCS_KEY_PATH,
        within_minutes=15,         # <--- THÊM DÒNG NÀY: Chỉ nhận file trong 15 phút qua
        mode="poke",
        poke_interval=60,
        timeout=1800,              
        dag=dag,
    )

    # Sau khi có file trong GCS, refresh BigQuery External Table metadata
    # để BQ nhận ra các Parquet file mới (không cần load data vào BQ table)
    def refresh_bq_external_table_fn(**context):
        from google.cloud import bigquery
        from google.oauth2 import service_account

        table_id = f"{gcp_project_id}.{bq_dataset}.{bq_table}"
        print(f"[Load] Refreshing BigQuery External Table: {table_id}")

        creds = service_account.Credentials.from_service_account_file(GCS_KEY_PATH)
        client = bigquery.Client(project=gcp_project_id, credentials=creds)

        # Query nhỏ để BQ rescan external table partition metadata
        query = f"SELECT COUNT(*) AS total_rows FROM `{table_id}`"
        result = list(client.query(query).result())
        total = result[0].total_rows
        print(f"[Load][OK] External table sẵn sàng. Tổng rows hiện tại: {total:,}")

    refresh_bq_table = PythonOperator(
        task_id="refresh_bq_external_table",
        python_callable=refresh_bq_external_table_fn,
        provide_context=True,
        dag=dag,
    )

    wait_for_gcs_bronze >> refresh_bq_table


# ══════════════════════════════════════════════════════════════════════════════
# GROUP 3 – STAGING
# dbt chạy RIÊNG staging model: stg_rideshare_events
# Đây là bước biến raw JSON (Bronze) thành bảng có cột/type rõ ràng (Silver).
# ══════════════════════════════════════════════════════════════════════════════
with TaskGroup("staging", tooltip="dbt staging: raw JSON → bảng có cấu trúc (stg_rideshare_events)", dag=dag) as tg_staging:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT_BASE_CMD} deps {DBT_FLAGS}",
        env=DBT_ENV,
        dag=dag,
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT_BASE_CMD} run --select staging {DBT_FLAGS}", 
        env=DBT_ENV,
        dag=dag,
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"{DBT_BASE_CMD} test --select staging,test_type:generic {DBT_FLAGS}",
        env=DBT_ENV,
        dag=dag,
    )

    dbt_deps >> dbt_run_staging >> dbt_test_staging


# ══════════════════════════════════════════════════════════════════════════════
# GROUP 4 – TRANSFORM
# dbt chạy core (fact/dim) + marts + DQ tests đầy đủ.
# ══════════════════════════════════════════════════════════════════════════════
with TaskGroup("transform", tooltip="dbt core/marts + Data Quality tests", dag=dag) as tg_transform:

    dbt_run_core = BashOperator(
        task_id="dbt_run_core",
        bash_command=f"{DBT_BASE_CMD} run --select core {DBT_FLAGS}",
        env=DBT_ENV,
        dag=dag,
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"{DBT_BASE_CMD} run --select marts {DBT_FLAGS}", 
        env=DBT_ENV,
        dag=dag,
    )

    dbt_test_all = BashOperator(
        task_id="dbt_test_all",
        bash_command=f"{DBT_BASE_CMD} test --select core marts {DBT_FLAGS}",
        env=DBT_ENV,
        dag=dag,
    )

    # Phân loại kết quả test: WARN → tiếp tục | ERROR → dừng pipeline
    def evaluate_dq_results(**context):
        results_path = os.path.join(DBT_DIR, "target", "run_results.json")

        if not os.path.exists(results_path):
            print("[DQ] run_results.json không tìm thấy. Bỏ qua.")
            return

        with open(results_path) as f:
            run_results = json.load(f)

        error_failures, warn_failures = [], []

        for result in run_results.get("results", []):
            if result.get("status") in ("fail", "error"):
                node_name = result.get("unique_id", "unknown")
                severity = (
                    result.get("node", {})
                          .get("config", {})
                          .get("severity", "error")
                          .lower()
                )
                failures = result.get("failures", 0)
                if severity == "error":
                    error_failures.append({"test": node_name, "failures": failures})
                else:
                    warn_failures.append({"test": node_name, "failures": failures})

        if warn_failures:
            print("[DQ][WARNING] Các WARN test phát hiện vấn đề:")
            for item in warn_failures:
                print(f"  ⚠  {item['test']}: {item['failures']} dòng lỗi")
            print("[DQ][WARNING] Pipeline tiếp tục. Kiểm tra dq_failures trong BigQuery.")

        if error_failures:
            msg = "[DQ][CRITICAL] ERROR test FAILED – dừng pipeline!\n"
            for item in error_failures:
                msg += f"  ✖  {item['test']}: {item['failures']} dòng lỗi\n"
            msg += "Kiểm tra dataset 'dq_failures' trong BigQuery."
            raise ValueError(msg)

        if not warn_failures and not error_failures:
            print("[DQ][OK] Tất cả data quality checks đều pass.")

    dq_alert = PythonOperator(
        task_id="dq_alert",
        python_callable=evaluate_dq_results,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag,
    )

    # Refresh DQ metrics mart (datasource cho Grafana dashboard)
    dbt_run_dq_mart = BashOperator(
        task_id="dbt_run_dq_mart",
        bash_command=f"{DBT_BASE_CMD} run --select mart_data_quality_metrics {DBT_FLAGS}", 
        env=DBT_ENV,
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag,
    )

    # Chain bên trong Transform
    dbt_run_core >> dbt_run_marts >> dbt_test_all >> dq_alert >> dbt_run_dq_mart


# ══════════════════════════════════════════════════════════════════════════════
# CHAIN CHÍNH: Extract → Load → Staging → Transform
# ══════════════════════════════════════════════════════════════════════════════
tg_extract >> tg_load >> tg_staging >> tg_transform