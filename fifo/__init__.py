import time
import uuid
import logging
import traceback
import cPickle as pickle
from importlib import import_module
from redis import Redis
from rabbit_sender import Sender
from rabbit_worker import Worker

logging.basicConfig(format=('%(asctime)s %(levelname)s pid-%(process)d '
                            '%(message)s'),
                    level=logging.DEBUG)

logger = logging.getLogger('fifo')

loads = pickle.loads
dumps = pickle.dumps


COMPLETED = 'completed'
TIMEOUT = 'timeout'
ERROR = 'error'
EXPIRED = 'expired'


class FifoClient(object):
    def __init__(self, broker):
        self.redis = Redis.from_url(broker)
        self.rabbit_sender = Sender(broker)

    def queue_task(self, name, task_args, max_wait, result_timeout=60):
        return self.queue_tasks(name, [task_args], max_wait, result_timeout)[0]

    def queue_tasks(self, name, tasks_args, max_wait, result_timeout=60):
        queue, function = name.rsplit('.', 1)
        tasks = [{
            'id': uuid.uuid4().hex,
            'function': function,
            'args': args or (),
            'time': time.time(),
            'max_wait': max_wait,
            'result_timeout': result_timeout,
        } for args in tasks_args]
        self.rabbit_sender.push_to_queue(queue, tasks)
        return [task['id'] for task in tasks]

    def wait(self, task_id, timeout=None):
        result = self.redis.brpop([task_id], timeout)
        if result:
            task_id, result = result
            return loads(result)
        else:
            raise TimeoutException()

    def wait_for_group(self, task_ids, timeout=None):
        deadline = time.time() + timeout
        task_ids = set(task_ids)
        results = {}
        for task_id in task_ids:
            results[task_id] = {'status': TIMEOUT, 'body': None}
        while task_ids and time.time() < deadline:
            timeout = int(deadline - time.time())
            if timeout > 0:
                result = self.redis.brpop(task_ids, timeout)
                if result:
                    task_id, task_result = result
                    results[task_id] = loads(task_result)
                    task_ids.remove(task_id)
        return results


class FifoWorker(object):
    def __init__(self, broker, module_name, name=None):
        self.broker = broker
        self.redis = Redis.from_url(broker)
        self.queue = module_name
        self.tasks_module = import_module(module_name)
        self.name = name
        self.rabbit_worker = Worker()

    def process_one(self):
        self.rabbit_worker.brpop(self.queue, self.process_message)

    def process_message(self, value):
        task = value
        logger.info("Task %s (%s) received ",
                    task['id'], task['function'])
        q_time = time.time() - task['time']
        if q_time > task['max_wait']:
            # Task has waited in the queue for more than the specified max
            logger.info('Task %s (%s) expired (waited %0.2fs) ',
                        task['id'], task['function'], q_time)
        else:
            task_function = getattr(self.tasks_module, task['function'])
            try:
                result = task_function(*task['args'])
                task_result = {'status': COMPLETED, 'body': result}
                logger.info('Task %s (%s) completed (%0.2fs)',
                            task['id'], task['function'], q_time)
            except Exception:
                logger.exception('Task %s (%s) raised an exception: ',
                                 task['id'], task['function'])
                tb = traceback.format_exc()
                task_result = {'status': ERROR, 'body': str(tb)}
            # if result_timeout <= 0 the client isn't waiting for a result
            if task['result_timeout'] > 0:
                self.redis.lpush(task['id'], dumps(task_result))
                self.redis.expire(task['id'], task['result_timeout'])

    def run(self):
        logger.info("Broker: %s", self.broker)
        logger.info("Queue: %s", self.queue)
        logger.info("Tasks module: %s", self.tasks_module.__file__)
        while True:
            try:
                self.process_one()
            except Exception, e:
                logger.exception(e)


class TimeoutException(Exception):
    pass
