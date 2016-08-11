MAX_EXCEPTIONS_HANDLING_TRY = 3


class Finalization:
    def __init__(self, info, task, process_manager):
        self.task = task
        self.info = info
        self.item = {'info': self.info, 'task': self.task}
        self.process_manager = process_manager

    def __call__(self, value):
        self.process_manager.running_tasks_count -= 1
        if value.get("exception"):

            failed_item = value.get("item")
            failed_item.get('task')['try'] = failed_item.get('task').get('try', 0) + 1
            if failed_item.get('task').get('try') <= MAX_EXCEPTIONS_HANDLING_TRY:
                self.process_manager.q.append(failed_item)
                self.process_manager.do_tasks()
            return

        if self.process_manager.saver:
            self.process_manager.saver(self.info, self.task, value.get("value"))

        if len(self.process_manager.q) > 0:
            self.process_manager.do_tasks()

        if self.process_manager.running_tasks_count == 0 and len(self.process_manager.not_ready_tasks) == 0:
            self.process_manager.terminate()
