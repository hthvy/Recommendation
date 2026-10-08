# file: filters/manager.py
from datetime import datetime
from .strategies import (
    BotFilterStrategy, TestUserFilterStrategy, MandatoryFieldStrategy,
    DebounceStrategy, BurstRateLimitStrategy, StateToggleStrategy,
    TimeRangeStrategy, ScrollThrottleStrategy, DuplicateViewStrategy,
    SearchContentStrategy, FeedbackQualityStrategy, BaseStrategy
)

# Tạo thêm một class nhỏ để check linh hoạt (conference_id HOẶC page_id)
class IDCheckStrategy(BaseStrategy):
    def should_filter(self, user_id, event_type, metadata, current_ts):
        # Nếu không có cả conference_id lẫn page_id -> CHẶN
        if not metadata.get('conference_id') and not metadata.get('page_id'):
            return True
        return False

class FilterManager:
    def __init__(self):
        # --- 1. BỘ LỌC TOÀN CỤC ---
        self.global_filters = [
            BotFilterStrategy(),
            TestUserFilterStrategy()
        ]

        # --- 2. BỘ LỌC THEO EVENT ---
        self.strategies = {
            'CLICK_FOLLOW_STAR': [
                StateToggleStrategy(interval=2.0),
                BurstRateLimitStrategy(max_count=5, window=10.0),
                MandatoryFieldStrategy(['conference_id'])
            ],
            'CLICK_ADD_CALENDAR': [
                DebounceStrategy(interval=1.0),
                BurstRateLimitStrategy(max_count=5, window=10.0),
                MandatoryFieldStrategy(['conference_id'])
            ],
            'CLICK_EXTERNAL_WEBSITE': [
                DebounceStrategy(interval=1.0),
                BurstRateLimitStrategy(max_count=5, window=10.0),
                MandatoryFieldStrategy(['conference_id'])
            ],
            'CLICK_CONFERENCE_TITLE': [
                DebounceStrategy(interval=1.0),
                MandatoryFieldStrategy(['conference_id'])
            ],
            'CLICK_RELATED_DETAIL': [
                DebounceStrategy(interval=1.0),
                MandatoryFieldStrategy(['conference_id'])
            ],
            'SEARCH_CONFERENCE': [
                DebounceStrategy(interval=2.0), 
                SearchContentStrategy()         
            ],
            'SUBMIT_FEEDBACK': [
                DebounceStrategy(interval=5.0), 
                FeedbackQualityStrategy(),
                MandatoryFieldStrategy(['conference_id'])
            ],
            
            'TIME_ON_PAGE': [
                TimeRangeStrategy(min_seconds=4.0, max_seconds=1800.0),
                # Thay MandatoryFieldStrategy cũ bằng cái check linh hoạt này
                IDCheckStrategy() 
            ],

            'SCROLL_DEPTH': [
                ScrollThrottleStrategy(interval=3.0),
                IDCheckStrategy() # Scroll cũng nên check linh hoạt
            ],
            'VIEW_PAGE': [
                MandatoryFieldStrategy(['url']),
                DuplicateViewStrategy(window=60.0)
            ]
        }
        self.default_filters = [] 

    def parse_timestamp(self, ts_str):
        try:
            return datetime.fromisoformat(str(ts_str).replace('Z', '+00:00')).timestamp()
        except:
            import time
            return time.time()

    def is_noise(self, log_data):
        event_type = log_data.get('event_type')
        user_id = log_data.get('user_id')
        
        if not event_type: return "Missing Event Type"
        if not user_id: return "Missing User ID"

        metadata = log_data.get('metadata', {})
        timestamp = self.parse_timestamp(log_data.get('timestamp'))

        # 1. Global Checks
        for strategy in self.global_filters:
            if strategy.should_filter(user_id, event_type, metadata, timestamp):
                return strategy.__class__.__name__

        # 2. Event Specific Checks
        strategies_list = self.strategies.get(event_type, self.default_filters)
        
        if not isinstance(strategies_list, list):
            strategies_list = [strategies_list]

        for strategy in strategies_list:
            if strategy.should_filter(user_id, event_type, metadata, timestamp):
                return strategy.__class__.__name__
                        
        return None 
