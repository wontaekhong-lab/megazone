# 파이프라인 소스 코드

# 임포트
# Python 사용 시 pipelines와 functions 모듈을 임포트합니다
from pyspark import pipelines as dp
from pyspark.sql.functions import *

# 파이프라인 설정 가져오기
# 파이프라인 생성 시 설정한 값들입니다
# 아래 코드는 이러한 설정을 Python 변수로 캡처합니다
catalog_name = spark.conf.get("catalog_name")
schema_name = spark.conf.get("schema_name")

# 설정을 사용하여 소스 경로 정의
source_path = f'/Volumes/{catalog_name}/{schema_name}/myfiles/'

# 브론즈 테이블
# Auto Loader를 사용하여 소스 위치에서 CSV 데이터를 읽어 브론즈 레벨 테이블로 저장
@dp.table
def bronze_table():
    return (
        spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("header", "true") # CSV 파일에 헤더가 있는 경우 이 옵션을 사용하세요
        .load(source_path)
    )

# 변환 로직과 데이터 품질 검증을 포함한 실버 레벨 테이블
@dp.table
# 첫 번째 검증은 'Country' 컬럼에 USA, India, Pakistan 이외의 값이 있으면 경고를 표시합니다
# 두 번째 검증은 'Role' 컬럼이 null이거나 예상 값이 아닌 레코드를 삭제합니다
# 세 번째 검증은 'ID' 컬럼이 null인 경우 파이프라인을 실패 처리합니다
@dp.expect("valid_country", "Country IN ('USA', 'India', 'Pakistan')")
@dp.expect_or_drop("valid_role", "Role IS NOT NULL AND Role IN ('INSTRUCTOR', 'MANAGER', 'DEVELOPER')")
@dp.expect_or_fail("valid_id", "ID IS NOT NULL")
def silver_table():
    return (
        spark.readStream.table("bronze_table") \
        .withColumn("id", col("id").cast("int")) \
        .withColumn("Role", upper(col("Role"))) \
        .select("ID", "Firstname", "Country", "Role")
    )

@dp.table
def gold_mv():
    return (
        spark.read.table("silver_table") \
        .groupBy("Role") \
        .agg(countDistinct("ID").alias("DistinctUsers"))
    )
