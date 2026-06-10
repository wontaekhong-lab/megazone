# Databricks notebook source
# MAGIC %md
# MAGIC ## 클래스룸 설정
# MAGIC
# MAGIC 다음 셀을 실행하여 이 코스에 필요한 작업 환경을 구성합니다.

# COMMAND ----------

####################################################################################
# 카탈로그, 스키마, 볼륨 이름에 대한 Python 변수 설정 (필요한 경우 변경)
catalog_name = "magazon"
schema_name = "ingestion_lab"
volume_name = "myfiles"
####################################################################################

####################################################################################
# 카탈로그, 스키마, 볼륨이 존재하지 않으면 생성
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}")
####################################################################################

####################################################################################
# 지정된 catalog.schema.volume에 employees.csv라는 파일 생성
import pandas as pd
data = [
    ["1111", "Kristi", "USA", "Manager"],
    ["2222", "Sophia", "Greece", "Developer"],
    ["3333", "Peter", "USA", "Developer"],
    ["4444", "Zebi", "Pakistan", "Administrator"]
]
columns = ["ID", "Firstname", "Country", "Role"] 
df = pd.DataFrame(data, columns=columns)
file_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/employees.csv"
df.to_csv(file_path, index=False)
################################################################################

####################################################################################
# 지정된 catalog.schema.volume에 taxi_files라는 볼륨 생성
spark.sql(f'CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.taxi_files')
output_volume = f'/Volumes/{catalog_name}/{schema_name}/taxi_files'

sdf = spark.table("samples.nyctaxi.trips")

(sdf
    .write
    .mode("overwrite")
    .csv(output_volume, header=True)
    )
####################################################################################

# COMMAND ----------

# MAGIC %md
# MAGIC ## 실습 시작

# COMMAND ----------

# MAGIC %md
# MAGIC 1. 현재 카탈로그를 **dbacademy**로, 스키마를 **ingesting_data**로 설정합니다.
# MAGIC
# MAGIC     **힌트**:
# MAGIC     - 카탈로그: `USE CATALOG`
# MAGIC     - 스키마: `USE SCHEMA`

# COMMAND ----------

# DBTITLE 1,Cell 5
# MAGIC %sql
# MAGIC USE CATALOG magazon;
# MAGIC USE SCHEMA ingestion_lab;

# COMMAND ----------

# MAGIC %md
# MAGIC 2. 현재 카탈로그와 스키마를 보려면 쿼리를 실행합니다. 결과가 **dbacademy** 카탈로그와 **ingestion_lab** 스키마를 표시하는지 확인합니다.

# COMMAND ----------

# DBTITLE 1,Cell 7
# MAGIC %sql
# MAGIC SELECT current_catalog() AS catalog, current_schema() AS schema;

# COMMAND ----------

# MAGIC %md
# MAGIC 3. 스키마에서 사용 가능한 볼륨을 확인하고 **taxi_files** 볼륨이 목록에 있는지 확인합니다.

# COMMAND ----------

# DBTITLE 1,Cell 9
# MAGIC %sql
# MAGIC SHOW VOLUMES;

# COMMAND ----------

# MAGIC %md
# MAGIC 4. **taxi_files** 볼륨의 파일을 나열하고 **name** 열을 확인하여 볼륨에 저장된 파일 유형을 확인합니다. 밑줄(_)로 시작하는 추가 파일은 무시합니다.
# MAGIC
# MAGIC **힌트**: 볼륨에 액세스하려면 다음 경로 형식을 사용합니다: */Volumes/catalog_name/schema_name/volume_name/*.

# COMMAND ----------

# DBTITLE 1,Cell 11
# MAGIC %sql
# MAGIC LIST '/Volumes/magazon/ingestion_lab/taxi_files';

# COMMAND ----------

# MAGIC %md
# MAGIC 5. 볼륨 경로를 직접 쿼리하고 적절한 파일 형식을 사용하여 파일의 데이터를 미리 보기합니다. 볼륨 경로를 백틱(`)으로 묶어야 합니다.
# MAGIC
# MAGIC **힌트**: SELECT * FROM \<file-format\>.\`\<path-to-volume-taxi_files\>\`

# COMMAND ----------

# DBTITLE 1,Cell 13
# MAGIC %sql
# MAGIC SELECT * FROM csv.`/Volumes/magazon/ingestion_lab/taxi_files`;

# COMMAND ----------

# MAGIC %md
# MAGIC 6. 스키마에 다음 열을 포함하는 **taxitrips_bronze**라는 테이블을 생성합니다:
# MAGIC | 필드 이름 | 필드 타입 |
# MAGIC | --- | --- |
# MAGIC | tpep_pickup_datetime | TIMESTAMP |
# MAGIC | tpep_dropoff_datetime | TIMESTAMP |
# MAGIC | trip_distance | DOUBLE |
# MAGIC | fare_amount | DOUBLE |
# MAGIC | pickup_zip | INT |
# MAGIC | dropoff_zip | INT |
# MAGIC
# MAGIC **참고:** DROP TABLE 문은 오류를 방지하기 위해 테이블이 이미 존재하는 경우 삭제합니다.

# COMMAND ----------

# DBTITLE 1,Cell 15
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS taxitrips_bronze;
# MAGIC
# MAGIC CREATE TABLE taxitrips_bronze (
# MAGIC   tpep_pickup_datetime TIMESTAMP,
# MAGIC   tpep_dropoff_datetime TIMESTAMP,
# MAGIC   trip_distance DOUBLE,
# MAGIC   fare_amount DOUBLE,
# MAGIC   pickup_zip INT,
# MAGIC   dropoff_zip INT
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC 7. [COPY INTO](https://docs.databricks.com/en/sql/language-manual/delta-copy-into.html) 문을 사용하여 **taxi_files** 볼륨의 파일을 **taxitrips_bronze** 테이블에 로드합니다. 다음 옵션을 포함합니다:
# MAGIC     - FROM `path-to-taxi_files`
# MAGIC     - FILEFORMAT = '\<file-format\>'
# MAGIC     - FORMAT_OPTIONS
# MAGIC       - 'header' = 'true'
# MAGIC       - 'inferSchema' = 'true'
# MAGIC
# MAGIC     21,932개의 행이 삽입되었는지 확인합니다.

# COMMAND ----------

# DBTITLE 1,Cell 17
# MAGIC %sql
# MAGIC COPY INTO taxitrips_bronze
# MAGIC FROM '/Volumes/magazon/ingestion_lab/taxi_files'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC 8. **taxitrips_bronze** 테이블의 행 수를 세어봅니다. 테이블에 21,932개의 행이 있는지 확인합니다.

# COMMAND ----------

# DBTITLE 1,Cell 19
# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM taxitrips_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC 9. **taxitrips_bronze** 테이블의 히스토리를 확인합니다. 버전 0과 버전 1이 사용 가능한지 확인합니다.

# COMMAND ----------

# DBTITLE 1,Cell 21
# MAGIC %sql
# MAGIC DESCRIBE HISTORY taxitrips_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC 10. 다음 스크립트를 실행하여 **trip_distance**가 *1*보다 작은 모든 행을 삭제합니다. *5,387*개의 행이 삭제되었는지 확인합니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM taxitrips_bronze
# MAGIC   WHERE trip_distance < 1;

# COMMAND ----------

# MAGIC %md
# MAGIC 11. **taxitrips_bronze** 테이블의 히스토리를 확인합니다. **operation** 열을 확인합니다. *DELETE* 작업이 발생한 버전을 확인합니다.

# COMMAND ----------

# DBTITLE 1,Cell 25
# MAGIC %sql
# MAGIC DESCRIBE HISTORY taxitrips_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC 12. 쿼리를 실행하여 **taxitrips_bronze** 테이블의 현재 버전에 있는 총 행 수를 세어봅니다. 현재 테이블에 *16,545*개의 행이 있는지 확인합니다.
# MAGIC
# MAGIC **힌트:** 기본적으로 가장 최근 버전이 사용됩니다.

# COMMAND ----------

# DBTITLE 1,Cell 27
# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM taxitrips_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC 13. 테이블의 원래 버전을 쿼리하여 처음 생성되었을 때의 행 수를 세어봅니다. 원래 테이블에 *21,932*개의 행이 있는지 확인합니다.
# MAGIC
# MAGIC **힌트:** FROM \<table> VERSION AS OF \<n>

# COMMAND ----------

# DBTITLE 1,Cell 29
# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM taxitrips_bronze VERSION AS OF 1;

# COMMAND ----------

# MAGIC %md
# MAGIC 14. 아차! 실수로 이전에 행을 삭제하면 안 되는 것이었습니다. [RESTORE](https://docs.databricks.com/en/sql/language-manual/delta-restore.html) 문을 사용하여 Delta 테이블을 *DELETE* 작업 이전의 원래 상태로 복원합니다.

# COMMAND ----------

# DBTITLE 1,Cell 32
# MAGIC %sql
# MAGIC RESTORE TABLE taxitrips_bronze TO VERSION AS OF 1;

# COMMAND ----------

# MAGIC %md
# MAGIC 15. **taxitrips_bronze** 테이블의 히스토리를 확인합니다. 가장 최근 버전에 **operation** *RESTORE*가 포함되어 있는지 확인합니다.

# COMMAND ----------

# DBTITLE 1,Cell 34
# MAGIC %sql
# MAGIC DESCRIBE HISTORY taxitrips_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC 16. 현재 **taxitrips_bronze** 테이블의 총 행 수를 세어봅니다. 테이블의 가장 최근 버전에 *21,932*개의 행이 있는지 확인합니다.

# COMMAND ----------

# DBTITLE 1,Cell 36
# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM taxitrips_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC 17. **ingestion_lab** 스키마를 삭제합니다.

# COMMAND ----------

# DBTITLE 1,Cell 38
# MAGIC %sql
# MAGIC DROP SCHEMA ingestion_lab CASCADE;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 요약
# MAGIC 이 실습을 완료하면 여러분은 이제 다음을 편안하게 수행할 수 있을 것입니다:
# MAGIC * 표준 Delta Lake 테이블 생성 및 데이터 조작 명령 완료
# MAGIC * 테이블 히스토리를 포함한 테이블 메타데이터 검토
# MAGIC * 스냅샷 쿼리 및 롤백을 위한 Delta Lake 버전 관리 활용
