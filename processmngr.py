import copy
from asyncfactory import AsyncFactory 
from collections import deque as Queue
from finalization import Finalization

MAX_CONCURRENT_RUNNING_TASKS = 10


def exception_handler(func, qitem, *args, **kwargs):
    try:
        return {"exception": False, 'value': func(*args, **kwargs)}
    except Exception as e:
        return {"exception": True, 'item': qitem, "message": e}


class ProcessManager:
    def __init__(self, saver=None, dependency_checkup=None):
        self.q = Queue()
        self.max_concurrency = MAX_CONCURRENT_RUNNING_TASKS
        self.running_tasks_count = 0
        self.saver = saver
        self.dependency_checkup = dependency_checkup
        self.asyncs = AsyncFactory()

    @classmethod
    def instance(cls, *args, **kwargs):
        try:
            return cls._instance
        except AttributeError:
            cls._instance = cls(*args, **kwargs)
            return cls._instance

    def add_task(self, info, task):
        item = {'info': info, 'task': task}
        self.q.append(item)

    def add_do_task_list(self, item_list):
        for item in item_list:
            self.add_task(**item)
        self.do_tasks()

    def do_tasks(self):
        not_ready_tasks = []
        while len(self.q) > 0 and self.max_concurrency > self.running_tasks_count:
            item = self.q.popleft()
            deps = self._check_dependencies(item.get('info'),
                                            item.get('task').get('dependencies'))
            if not deps:
                not_ready_tasks.append(item)
            else:
                self.running_tasks_count += 1
                kwargs = copy.deepcopy(item.get('info'))
                if isinstance(deps, dict):
                    kwargs.update(deps)
                kwargs.update({'func': item.get('task').get('method'), 'qitem': item})
                finish_task = Finalization(item.get('info'), item.get('task'), self)

                self.asyncs.call(exception_handler, finish_task, kwargs)

        for item in copy.deepcopy(not_ready_tasks):
            self.add_task(item.get('info'), item.get('task'))

    def _check_dependencies(self, info, dependencies):
        if not self.dependency_checkup or not dependencies:
            return True
        return self.dependency_checkup(info, dependencies)




