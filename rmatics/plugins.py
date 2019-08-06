from rmatics.utils.cacher import FlaskCacher
from rmatics.utils.cacher.cache_invalidators import MonitorCacheInvalidator

invalidator = MonitorCacheInvalidator(autocommit=False)

monitor_cacher = FlaskCacher(prefix='monitor', cache_invalidator=invalidator,
                             allowed_kwargs=['problem_id', 'user_ids',
                                             'time_after', 'time_before'
                                             'statement_id', 'context_source', 'show_hidden'])
