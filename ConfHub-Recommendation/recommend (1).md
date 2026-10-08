\[CHATBOT]

kết nối localhost redis cổng 6380

Key : recommend\_user:{user\_id}

* Kiểu dữ liệu: Chuỗi JSON (chứa một mảng các ID hội nghị, ví dụ: \["id1", "id2", ...]).  
* Nguồn gốc:BE->src/modules/recommend/services/recommend.service.ts
* Được tạo ra từ hàm run\_personalized\_recommendation dành cho trang chủ (Home/For You).
* Trường hợp key trống có thể lấy topic quan tâm ở PosttgreSQL tìm trong bảng "TopicUserInteresteds" những dòng có chứa ID của người dùng (userId), sau đó đối chiếu (JOIN/include) sang bảng gốc "Topics" để lấy ra cái cột tên (name)
* Trường hợp cuối cùng nếu các trường hợp trên trống, fallback top trending" ở hàm get\_upcoming\_candidates\_by\_topics(ở file user\_based\_cf.py)



\[ADMIN]

Các thông số Admin có thể quản lý:

**File chính: src/modules/recommend/services/recommend.service.ts**

* Limit hiển thị Top Trending (take: 36): Số lượng hội nghị phổ biến tối đa được trả về khi hệ thống rơi vào Kịch bản 4 (User không có lịch sử).
* Limit quét hội nghị liên quan Detail (take: 50): Khi xem 1 hội nghị, Backend sẽ quét bao nhiêu hội nghị cùng Topic để mang đi chấm điểm CF.
* Limit hiển thị Detail (slice(0, 10)): Số lượng hội nghị liên quan thực tế hiện ra ở trang chi tiết hội nghị(đang fix cứng là 10).
* Điểm Boost Search (SEARCH\_INTENT\_BOOST = 100): Trọng số cộng thêm cực mạnh khi hội nghị khớp với từ khóa người dùng gõ vào thanh tìm kiếm. Admin có thể giảm số này nếu thấy search bị "ưu tiên quá đà".
* Thời gian Cache phân trang (TTL = 600): Thời gian lưu Cache kết quả tìm kiếm trên Redis (đang là 600 giây = 10 phút). Admin có thể chỉnh ngắn lại nếu muốn dữ liệu update realtime hơn, hoặc dài ra nếu server bị yếu.
* Pagination Per Page (perPage = 12): Số lượng hội nghị load ra trong 1 trang.



**File chính: recommendation\_engine/algorithms/user\_based\_cf.py**

* Bảng trọng số hành vi (SHORT\_TERM\_EVENT\_WEIGHTS):

  * CLICK\_FOLLOW\_STAR: 3.0 điểm
  * CLICK\_ADD\_CALENDAR: 2.8 điểm
  * SEARCH\_CONFERENCE: 3.5 điểm
  * TIME\_ON\_PAGE: 1.2 điểm...
* Lấy bao nhiêu tracking để huấn luyện mỗi lần: ví dụ lấy 30 tracking gần nhất 

ursor = collection.find({

"user\_id": user\_id,

&#x20;"event\_type": {"$in": list(SHORT\_TERM\_EVENT\_WEIGHTS.keys())}

&#x20; }).sort("timestamp", -1).limit(30)

* Số lượng "hàng xóm" CF (top\_k\_neighbors = 10): Thuật toán sẽ học lỏm sở thích từ bao nhiêu người giống bạn nhất.
* Candidate Limit (limit = 1000 / limit = 300): Ở Kịch bản 3, ES sẽ nhặt ra bao nhiêu ứng viên (1000) để đưa vào tính toán, và lọc lại giữ bao nhiêu cái (300). Admin có thể chỉnh giới hạn 300 này.
* Trọng số lai (Hybrid Weights):
* Nhân đôi điểm CF (cf\_score \* 2.5).
* Trừ điểm cực mạnh nếu đã tương tác (score -= 5.0).
* Trừ điểm nếu nằm trong negative items/unfollow (score -= 0.5).



**File chính: recommendation\_engine/algorithms/search\_and\_recommend.py**

* Trọng số Topic Search (SEARCH\_INTENT\_WEIGHT = 0.35): Mức độ ảnh hưởng của từ khóa tìm kiếm lên sở thích topic lâu dài.
* Hệ số Boost Search (boost\_score = score \* 0.25): Sự bù trừ điểm.
* Khoảng thời gian quét lịch sử gõ Keyword (days = 2): Chỉ lấy những keyword user gõ trong 2 ngày gần nhất.



**recommendation\_engine/main\_job.py (File này điều phối thời gian)**

* Cronjob Interval: Bao lâu thì AI chạy lại một lần? (Ví dụ: Chạy mỗi 1 giờ, hoặc chạy vào lúc 2h sáng). Admin có thể setting chu kỳ này.
* Admin bấm reset lại là chạy lại
* Chọn module thuật toán để chạy

