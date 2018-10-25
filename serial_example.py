import asyncio
import botocore.session
import subprocess


def create_queue():
    print("Creating Queue -- Starting")
    session = botocore.session.get_session()
    client = session.create_client('sqs', region_name='us-west-1',endpoint_url='http://localhost:5000',)
    client.create_queue(QueueName='test_queue1')
    print("Creating Queue -- Finished")


def write_message(client, queue_url, i):
    print(f"Writing Message {i} -- Starting")
    client.send_message(
            QueueUrl=queue_url,
            MessageBody=f'{i}',
            MessageDeduplicationId=f'{i}',
        )
    print(f"Writing Message {i} -- Finished")


def write_10_messages():
    print("Writing to Queue -- Starting")
    session = botocore.session.get_session()
    client = session.create_client('sqs', region_name='us-west-1',endpoint_url='http://localhost:5000',)
    resp = client.get_queue_url(QueueName='test_queue1')
    queue_url = resp['QueueUrl']
    for i in range(10):
        write_message(client, queue_url, i)

    print("Writing to Queue -- Finished")


def handle_read_response(client, queue_url, response):
    if 'Messages' in response:
        for msg in response['Messages']:
            # Need to remove msg from queue or else it'll reappear
            print(f"Handling response {msg['Body']} -- Starting")
            client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=msg['ReceiptHandle']
            )
            print(f"Handling response {msg['Body']} -- Finished")


def read_10_messages():
    print("Reading from Queue -- Starting")
    session = botocore.session.get_session()
    client = session.create_client('sqs', region_name='us-west-1',endpoint_url='http://localhost:5000',)
    client.create_queue(QueueName='test_queue1')

    resp = client.get_queue_url(QueueName='test_queue1')
    queue_url = resp['QueueUrl']

    for i in range(10):
        response = client.receive_message(
            QueueUrl=queue_url,
            WaitTimeSeconds=2,
        )
        handle_read_response(client, queue_url, response)


    print("Reading from Queue -- Finished")


def main_serial():
    create_queue()
    write_10_messages()
    read_10_messages()


def main():
    #p = subprocess.Popen('moto_server sqs', shell=True)
    import time
    start = time.time()
    for i in range(20):
        main_serial()
    print(time.time() - start)
    #p.kill()


if __name__ == '__main__':
    main()
