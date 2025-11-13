
import json
import os
import time
import pika

class Worker:
    def __init__(self, mq_host, mq_port, data_dir):
        self.mq_host = mq_host
        self.mq_port = mq_port
        self.data_dir = data_dir
        while True:
            try:
                conn = pika.BlockingConnection(pika.ConnectionParameters(
                    self.mq_host, self.mq_port, heartbeat=5))
                conn.close()
                break
            except:
                time.sleep(1)

    def produce_image_caption(self, image_url):
        return str(abs(hash(image_url)) % (10 ** 8))

    def send_callback(self, image_id):
        while True:
            try:
                conn = pika.BlockingConnection(pika.ConnectionParameters(
                    self.mq_host, self.mq_port, heartbeat=5))
                ch = conn.channel()
                ch.queue_declare(queue='callback_queue', durable=True)
                ch.basic_publish(
                    exchange='',
                    routing_key='callback_queue',
                    body=json.dumps({'image_id': image_id}),
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                conn.close()
                break
            except:
                time.sleep(1)

    def process_message(self, ch, method, properties, body):
        try:
            data = json.loads(body)
            image_id = data['image_id']
            image_url = data['image_url']

            caption = self.produce_image_caption(image_url)
            with open(os.path.join(self.data_dir, f"{image_id}.txt"), 'w') as f:
                f.write(caption)

            self.send_callback(image_id)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def run(self):
        while True:
            try:
                conn = pika.BlockingConnection(pika.ConnectionParameters(
                    self.mq_host, self.mq_port, heartbeat=5))
                ch = conn.channel()
                ch.queue_declare(queue='task_queue', durable=True)
                ch.basic_qos(prefetch_count=1)
                ch.basic_consume(queue='task_queue', on_message_callback=self.process_message)
                ch.start_consuming()
            except:
                time.sleep(1)

if __name__ == '__main__':
    worker = Worker('rabbitmq', 5672, '/data')
    worker.run()
