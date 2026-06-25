from .queue import SubmitQueue


submit_queue = SubmitQueue()
queue_submit = submit_queue.submit


get_last_get_id = submit_queue.get_last_get_id
