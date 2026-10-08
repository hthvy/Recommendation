# Cài thêm 
pip install pymongo kafka-python
npm install winston-daily-rotate-file (ở BE)
python -m pip install elasticsearch
# Cách chạy lần đầu
1. pip install -r requirements.txt
1.1. docker compose up --build
# Hãy xem kỹ các containers trong docker có bật lên hết chưa -> phải bật hết
2. Các lần sau chỉ cần chạy: docker compose up

# recommendation system
Cài thêm: python -m pip install pandas scikit-learn redis pymongo python-dotenv apscheduler
pip install elasticsearch==8.12.0
pip install requests
pip install apscheduler //cài đồng hồ lên lịch
pip install python-dotenv
# Cách chạy
# đặt lịch (không được tắt máy) - 2am mỗi ngày
python recommendation_engine/main_job.py 
# load dữ liệu conf vào elasticsearch
python -m core.sync_es
