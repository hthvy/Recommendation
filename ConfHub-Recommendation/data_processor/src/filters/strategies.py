# file: filters/strategies.py
from abc import ABC, abstractmethod
from collections import deque

class BaseStrategy(ABC):
    @abstractmethod
    def should_filter(self, user_id, event_type, metadata, current_ts):
        pass

# --- SECURITY ---
class BotFilterStrategy(BaseStrategy):
    def __init__(self):
        self.bots = ['bot', 'crawl', 'spider', 'slurp', 'postman', 'headless', 'curl', 'wget', 'test', 'admin']
    def should_filter(self, user_id, event_type, metadata, current_ts):
        ua = metadata.get('userAgent', '').lower()
        return any(b in ua for b in self.bots) if ua else False

class TestUserFilterStrategy(BaseStrategy):
    def __init__(self):
        self.prefixes = ['test_ignore_', 'admin_debug_'] 
    def should_filter(self, user_id, event_type, metadata, current_ts):
        u = str(user_id).lower()
        return len(u) < 2 or any(u.startswith(p) for p in self.prefixes)

# --- QUALITY ---
class MandatoryFieldStrategy(BaseStrategy):
    def __init__(self, fields): self.fields = fields
    def should_filter(self, user_id, event_type, metadata, current_ts):
        return any(not metadata.get(f) for f in self.fields)

# --- INTERACTION ---
class DebounceStrategy(BaseStrategy):
    def __init__(self, interval=0.5):
        self.interval = interval; self.last_seen = {}
    def should_filter(self, user_id, event_type, metadata, current_ts):
        target = metadata.get('conference_id') or metadata.get('page_id') or metadata.get('url') or 'global'
        key = f"{user_id}:{event_type}:{target}"
        if current_ts - self.last_seen.get(key, 0) < self.interval: return True
        self.last_seen[key] = current_ts; return False

class BurstRateLimitStrategy(BaseStrategy):
    def __init__(self, max_count=5, window=10.0):
        self.max_count = max_count; self.window = window; self.history = {}
    def should_filter(self, user_id, event_type, metadata, current_ts):
        key = f"{user_id}:{event_type}"
        if key not in self.history: self.history[key] = deque()
        q = self.history[key]
        while q and q[0] < current_ts - self.window: q.popleft()
        if len(q) >= self.max_count: return True
        q.append(current_ts); return False

class StateToggleStrategy(BaseStrategy):
    def __init__(self, interval=2.0):
        self.interval = interval; self.last = {}
    def should_filter(self, user_id, event_type, metadata, current_ts):
        action = metadata.get('action'); target = metadata.get('conference_id')
        if not action or not target: return False
        key = f"{user_id}:{event_type}:{target}"
        prev = self.last.get(key)
        self.last[key] = {'a': action, 't': current_ts}
        return prev and prev['a'] != action and (current_ts - prev['t'] < self.interval)

# --- TIME LOGIC 
class TimeRangeStrategy(BaseStrategy):
    def __init__(self, min_seconds=4.0, max_seconds=1800.0):
        self.min_s = min_seconds; self.max_s = max_seconds

    def should_filter(self, user_id, event_type, metadata, current_ts):
        try:
            # Ưu tiên lấy seconds có sẵn (log của bạn là 7)
            s = float(metadata.get('duration_seconds') or 0)
            m = float(metadata.get('duration_ms') or 0)
            
            # Nếu s > 0 lấy s (7), nếu không lấy m/1000
            val = s if s > 0 else (m / 1000.0)

            if val <= 0: return False # Không có time -> Cho qua
            
            if val < self.min_s: return True 
            if val > self.max_s: return True
        except: return False
        return False

class ScrollThrottleStrategy(BaseStrategy):
    def __init__(self, interval=3.0):
        self.interval = interval; self.last = {}
    def should_filter(self, user_id, event_type, metadata, current_ts):
        key = f"{user_id}:{metadata.get('url')}"
        if current_ts - self.last.get(key, 0) < self.interval: return True
        self.last[key] = current_ts; return False

class DuplicateViewStrategy(BaseStrategy):
    def __init__(self, window=60.0):
        self.window = window; self.last = {}
    def should_filter(self, user_id, event_type, metadata, current_ts):
        pid = metadata.get('conference_id') or metadata.get('page_id') or metadata.get('url')
        if not pid: return False
        key = f"{user_id}:{event_type}:{pid}"
        if current_ts - self.last.get(key, 0) < self.window: return True
        self.last[key] = current_ts; return False

class SearchContentStrategy(BaseStrategy):
    def _has_meaningful_value(self, value):
        if value is None:
            return False

        if isinstance(value, str):
            return bool(value.strip())

        if isinstance(value, dict):
            return any(self._has_meaningful_value(v) for v in value.values())

        if isinstance(value, (list, tuple, set)):
            if not value:
                return False
            if list(value) == [0, 100]:
                return False
            return any(self._has_meaningful_value(v) for v in value)

        if isinstance(value, bool):
            return value

        return True

    def should_filter(self, user_id, event_type, metadata, current_ts):
        kw = str(metadata.get('keyword', '')).strip().lower()
        metadata['keyword'] = kw
        query = str(metadata.get('query', '')).strip().lower()
        filters = metadata.get('filters', {})

        has_keyword_or_query = bool(kw or query)
        has_filters = isinstance(filters, dict) and self._has_meaningful_value(filters)

        return not has_keyword_or_query and not has_filters

class FeedbackQualityStrategy(BaseStrategy):
    def should_filter(self, user_id, event_type, metadata, current_ts):
        try:
            if not (1 <= int(metadata.get('rating_score', 0)) <= 5): return True
        except: return True
        c = str(metadata.get('feedback_content', '')).strip()
        return (int(metadata['rating_score']) in [1, 5] and len(c) < 5)
