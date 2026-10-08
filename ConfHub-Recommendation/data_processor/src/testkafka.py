from kafka import KafkaConsumer
import json
import time
import re  # 👈 QUAN TRỌNG: Thêm thư viện Regular Expression

# 1. CẤU HÌNH
KAFKA_SERVER = 'localhost:9092' 

print(f"--- Đang kết nối tới Kafka: {KAFKA_SERVER} ---")
print(f"--- Chế độ: NGHE TOÀN BỘ CÁC TOPIC (Catch-all) ---")

try:
    # 2. KHỞI TẠO CONSUMER (Không điền tên topic ở đây nữa)
    consumer = KafkaConsumer(
        bootstrap_servers=[KAFKA_SERVER],
        auto_offset_reset='earliest', 
        enable_auto_commit=True,
        group_id='debug-group-all-topics',
        # 👇 Fix lỗi version nếu server Kafka quá mới so với thư viện python
        api_version=(0, 10, 1), 
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) 
    )

    # 3. ĐĂNG KÝ NGHE BẰNG REGEX (Biểu thức chính quy)
    # pattern='^.*$' nghĩa là: Nghe tất cả mọi thứ
    # pattern='^user.*$' nghĩa là: Chỉ nghe các topic bắt đầu bằng chữ "user"
    consumer.subscribe(pattern='^.*$') 

    print("✅ Kết nối thành công! Đang rình mò trên TẤT CẢ các topic...")
    
    # 4. VÒNG LẶP ĐỌC TIN NHẮN
    for message in consumer:
        # Bỏ qua các topic nội bộ của Kafka (như __consumer_offsets) cho đỡ rối
        if message.topic.startswith("__"):
            continue

        print("\n" + "="*50)
        # 👇 In ra tên Topic để bạn biết tin nhắn này đến từ đâu
        print(f"📢 TOPIC: {message.topic} | OFFSET: {message.offset}")
        print("-" * 50)
        print(json.dumps(message.value, indent=4, ensure_ascii=False))

except Exception as e:
    print(f"❌ Lỗi kết nối: {e}")
    print("Gợi ý: Kiểm tra xem Docker Kafka có đang chạy không?")