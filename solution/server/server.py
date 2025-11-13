import json
import os
import threading
import time
from typing import List, Optional
from collections import deque
import pika
import pika.exceptions
from flask import Flask, request

class Server:
    def __init__(self, mq_host, mq_port, data_dir):
        self.mq_host = mq_host
        self.mq_port = mq_port
        self.data_dir = data_dir
        self.processed_images = set()
        self.lock = threading.Lock()
        self.buffer = deque()
        self.buffer_lock = threading.Lock()
        self.buffer_event = threading.Event()

        while True:
            try:
                conn = pika.BlockingConnection(pika.ConnectionParameters(
                    self.mq_host, self.mq_port, heartbeat=5))
                ch = conn.channel()

                try:
                    ch.queue_delete(queue='task_queue')
                except:
                    pass
                ch.queue_declare(queue='task_queue', durable=True)

                conn.close()
                break
            except:
                time.sleep(1)

        threading.Thread(target=self._callback_loop, daemon=True).start()
        threading.Thread(target=self._buffer_publisher_loop, daemon=True).start()

    def _buffer_publisher_loop(self):
        while True:
            self.buffer_event.wait(timeout=1.0)
            self.buffer_event.clear()

            try:
                conn = pika.BlockingConnection(pika.ConnectionParameters(
                    self.mq_host, self.mq_port, heartbeat=5))
                ch = conn.channel()
                ch.confirm_delivery()
                ch.queue_declare(queue='task_queue', durable=True)

                while True:
                    with self.buffer_lock:
                        if not self.buffer:
                            break
                        message = self.buffer.popleft()

                    try:
                        ch.basic_publish(
                            exchange='',
                            routing_key='task_queue',
                            body=message,
                            properties=pika.BasicProperties(delivery_mode=2),
                        )
                    except (pika.exceptions.UnroutableError, pika.exceptions.NackError):
                        with self.buffer_lock:
                            self.buffer.appendleft(message)
                        break

                conn.close()
            except:
                time.sleep(1)

    def _callback_loop(self):
        while True:
            try:
                conn = pika.BlockingConnection(pika.ConnectionParameters(
                    self.mq_host, self.mq_port, heartbeat=5))
                ch = conn.channel()
                ch.queue_declare(queue='callback_queue', durable=True)

                def callback(c, m, p, b):
                    try:
                        img_id = str(json.loads(b).get('image_id'))
                        with self.lock:
                            self.processed_images.add(img_id)
                        c.basic_ack(delivery_tag=m.delivery_tag)
                    except:
                        c.basic_nack(delivery_tag=m.delivery_tag, requeue=True)

                ch.basic_consume(queue='callback_queue', on_message_callback=callback)
                ch.start_consuming()
            except:
                time.sleep(1)

    def _try_publish_immediately(self, message, timeout=0.8):
        result = [False]

        def publish():
            try:
                conn = pika.BlockingConnection(pika.ConnectionParameters(
                    self.mq_host, self.mq_port, heartbeat=5))
                ch = conn.channel()
                ch.confirm_delivery()
                ch.queue_declare(queue='task_queue', durable=True)

                try:
                    ch.basic_publish(
                        exchange='',
                        routing_key='task_queue',
                        body=message,
                        properties=pika.BasicProperties(delivery_mode=2),
                    )
                    result[0] = True
                except (pika.exceptions.UnroutableError, pika.exceptions.NackError):
                    result[0] = False
                finally:
                    conn.close()
            except:
                result[0] = False

        thread = threading.Thread(target=publish, daemon=True)
        thread.start()
        thread.join(timeout=timeout)

        return result[0]

    def add_image(self, image_url: str) -> str:
        image_id = str(abs(hash(image_url + str(time.time()))) % (10 ** 8))
        message = json.dumps({'image_id': image_id, 'image_url': image_url})

        if self._try_publish_immediately(message, timeout=0.8):
            return image_id

        with self.buffer_lock:
            self.buffer.append(message)

        self.buffer_event.set()

        return image_id

    def get_processed_images(self) -> List[str]:
        with self.lock:
            return list(self.processed_images)

    def get_image_caption(self, image_id: str) -> Optional[str]:
        path = os.path.join(self.data_dir, f"{image_id}.txt")
        if os.path.exists(path):
            with open(path, 'r') as f:
                return f.read().strip()

def create_app() -> Flask:
    app = Flask(__name__)
    server = Server('rabbitmq', 5672, '/data')

    @app.route('/api/v1.0/images', methods=['POST'])
    def add_image():
        body = request.get_json(force=True)
        if not body or 'image_url' not in body:
            return "Bad request", 400
        return {"image_id": server.add_image(body['image_url'])}

    @app.route('/api/v1.0/images', methods=['GET'])
    def get_processed_images():
        image_ids = server.get_processed_images()
        return {"image_ids": image_ids}

    @app.route('/api/v1.0/images/<string:image_id>', methods=['GET'])
    def get_image_caption(image_id):
        result = server.get_image_caption(image_id)
        if result is None:
            return "Image not found.", 404
        else:
            return {'caption': result}

    return app

if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=5000)
