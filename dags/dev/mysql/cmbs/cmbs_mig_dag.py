import MySQLdb
import MySQLdb.cursors
import subprocess
import base64
import os
import sys
from typing import List, Dict, Any

# ===== 설정부 =====
JAR_PATH = "/path/to/your/decryptor.jar" # JAR 절대경로 지정 필수
JAVA_BIN = "java"

SRC = {
    "host": "10.10.1.10", "port": 3306, "user": "root", "passwd": "src_mysql_pass123!", "db": "member_db",
    "cursorclass": MySQLdb.cursors.DictCursor
}

TGT = {
    "host": "10.10.1.20", "port": 3306, "user": "root", "passwd": "tgt_mysql_pass456@", "db": "member_db",
}

# 테이블별 마이그레이션 설정
TABLE_CONFIG = {
    "mbr_base": {
        "columns": ["mbr_no", "mbr_id", "mbr_nm", "enc_email"],
        "decrypt_cols": ["enc_email"],
        "range_col": "mbr_no",  # 범위 기준 컬럼
        "start_id": 1,          # 시작 ID
        "end_id": 50000         # 종료 ID
    }
}

# ===== [추가] Base64 함수 =====
def handle_base64(data: str, mode: str = "decode") -> str:
    """Base64 인코딩/디코딩 수행"""
    if not data: return data
    try:
        if mode == "decode":
            return base64.b64decode(data.encode('utf-8')).decode('utf-8')
        else:
            return base64.b64encode(data.encode('utf-8')).decode('utf-8')
    except Exception as e:
        print(f"Base64 {mode} Error: {e}")
        return data

# ===== [문제 1] Java 암호화 호출 (개선) =====
def call_java_decrypt(encrypted_value: str) -> str:
    if not encrypted_value: return encrypted_value
    try:
        # JAR 경로를 포함한 실행
        result = subprocess.run(
            [JAVA_BIN, "-jar", JAR_PATH, encrypted_value],
            capture_output=True, text=True, check=True
        )
        return result.stdout.strip()
    except Exception as e:
        # 에러 발생 시 로그를 남기고 원본 반환 (변조 방지 체크용)
        print(f"Java Decrypt Fail: {e}")
        return f"ERR_DECRYPT_{encrypted_value}"

# ===== 데이터 가공 (Transform) =====
def transform_row(row: Dict[str, Any], decrypt_cols: List[str]) -> tuple:
    for col in decrypt_cols:
        if col in row and row[col]:
            # 1. 먼저 Base64 디코딩 (필요한 경우)
            # decoded_val = handle_base64(row[col], "decode")
            
            # 2. Java 복호화 수행
            row[col] = call_java_decrypt(row[col])
            
            # 3. 추가 변조/검증 로직 (예: 공백 제거)
            row[col] = row[col].strip() if row[col] else row[col]

    return tuple(row.values())

def _etl_process(table_name: str, config: Dict):
    src_conn = None
    tgt_conn = None
    
    try:
        src_conn = MySQLdb.connect(**SRC)
        tgt_conn = MySQLdb.connect(**TGT)
        src_cur = src_conn.cursor()
        tgt_cur = tgt_conn.cursor()

        cols_str = ", ".join(config["columns"])
        placeholders = ", ".join(["%s"] * len(config["columns"]))
        insert_sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})"

        # [문제 해결] 값의 범위(Range) 설정 쿼리
        range_query = f"""
            SELECT {cols_str} FROM {table_name} 
            WHERE {config['range_col']} BETWEEN %s AND %s
        """
        
        print(f"   > Processing range: {config['start_id']} ~ {config['end_id']}")
        src_cur.execute(range_query, (config['start_id'], config['end_id']))

        processed_count = 0
        while True:
            rows = src_cur.fetchmany(1000) # CHUNK_SIZE
            if not rows: break
            
            transformed_rows = [transform_row(r, config["decrypt_cols"]) for r in rows]

            try:
                tgt_cur.executemany(insert_sql, transformed_rows)
                tgt_conn.commit()
            except Exception as e:
                tgt_conn.rollback()
                print(f"   ! Error during batch insert: {e}")
                raise
            
            processed_count += len(transformed_rows)
            print(f"   - {table_name}: {processed_count} rows migrated...", end='\r')

    finally:
        if src_conn: src_conn.close()
        if tgt_conn: tgt_conn.close()

def main():
    for table_name, config in TABLE_CONFIG.items():
        print(f"\n[START] {table_name}")
        try:
            _etl_process(table_name, config)
            print(f"\n[FINISH] {table_name} - Success")
        except Exception as e:
            print(f"\n[ABORT] {table_name} - Error: {e}")

if __name__ == "__main__":
    main()