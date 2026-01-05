#!/bin/bash

# 사용법: ./upload_folder_to_s3.sh <source_folder> <s3_bucket> <s3_prefix>
# 예시: ./upload_folder_to_s3.sh ./my_data mhsong-criu-s3-data--usw2-az1--x-s3 backups/2024

# 파라미터 확인
if [ $# -ne 3 ]; then
    echo "사용법: $0 <source_folder> <s3_bucket> <s3_prefix>"
    echo "예시: $0 ./my_data mhsong-criu-s3-data--usw2-az1--x-s3 backups/2024"
    exit 1
fi

# 파라미터 설정
SOURCE_FOLDER="$1"
S3_BUCKET="$2"
S3_PREFIX="$3"

# S3 prefix 끝에 슬래시 제거 (있을 경우)
S3_PREFIX="${S3_PREFIX%/}"

# 소스 폴더 존재 확인
if [ ! -d "$SOURCE_FOLDER" ]; then
    echo "오류: '$SOURCE_FOLDER' 폴더가 존재하지 않습니다."
    exit 1
fi

echo "========================================="
echo "Express One Zone S3 업로드 시작"
echo "========================================="
echo "소스 폴더: $SOURCE_FOLDER"
echo "대상 버킷: $S3_BUCKET"
echo "S3 Prefix: $S3_PREFIX"
echo ""

# 업로드할 파일 개수 확인
FILE_COUNT=$(find "$SOURCE_FOLDER" -type f | wc -l)
echo "업로드할 파일 개수: $FILE_COUNT개"

# 전체 크기 계산
TOTAL_SIZE=$(du -sh "$SOURCE_FOLDER" | cut -f1)
echo "전체 크기: $TOTAL_SIZE"
echo ""

# 업로드 시작 시간 기록
upload_start_time=$(date +%s)
echo "업로드 시작 시간: $(date)"
echo "-------------------------------------"

# S3에 폴더 전체 업로드 (recursive)
# Express One Zone은 단일 AZ 스토리지 클래스
aws s3 cp "$SOURCE_FOLDER" "s3://${S3_BUCKET}/${S3_PREFIX}/" \
    --recursive

# 업로드 결과 확인
if [ $? -eq 0 ]; then
    upload_end_time=$(date +%s)
    upload_duration=$((upload_end_time - upload_start_time))
    
    echo ""
    echo "✓ 업로드 성공!"
    echo "  소요시간: ${upload_duration}초"
    echo "  S3 경로: s3://${S3_BUCKET}/${S3_PREFIX}/"
    
    # 업로드된 내용 확인
    echo ""
    echo "업로드된 내용 확인:"
    echo "-------------------------------------"
    aws s3 ls "s3://${S3_BUCKET}/${S3_PREFIX}/" --recursive | head -20
    
    # 20개 이상인 경우 메시지 표시
    if [ $FILE_COUNT -gt 20 ]; then
        echo "... (총 $FILE_COUNT개 파일)"
    fi
    
    # 업로드된 전체 크기 확인
    echo ""
    echo "S3 버킷 내 업로드된 크기 확인:"
    aws s3 ls "s3://${S3_BUCKET}/${S3_PREFIX}/" --recursive --summarize | grep "Total Size"
    
else
    echo ""
    echo "✗ 업로드 실패!"
    echo "AWS CLI 설정 및 버킷 권한을 확인해주세요."
    exit 1
fi

echo ""
echo "========================================="
echo "업로드 스크립트 실행 완료"
echo "========================================="