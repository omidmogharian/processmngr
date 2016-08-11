from multiprocessing import Pool


class AsyncFactory(object):
    def __init__(self):
        self.pool = Pool()
        self.result = []

    def call(self, func, cb_func, kwargs):
        r = self.pool.apply_async(func, kwds=kwargs, callback=cb_func)
        self.result.append(r)
