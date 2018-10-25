import asyncio
import aiobotocore
import subprocess


async def create_queue():
    print("Creating Queue -- Starting")
    session = aiobotocore.get_session()
    client = session.create_client('sqs', region_name='us-west-1',endpoint_url='http://localhost:5000',)
    await client.create_queue(QueueName='test_queue1')
    print("Creating Queue -- Finished")
    await client.close()


async def write_message(client, queue_url, i):
    print(f"Writing Message {i} -- Starting")
    await client.send_message(
            QueueUrl=queue_url,
            MessageBody=f'string{i}',
            MessageDeduplicationId=f'{i}',
        )
    print(f"Writing Message {i} -- Finished")


async def write_10_messages():
    print("Writing to Queue -- Starting")
    session = aiobotocore.get_session()
    client = session.create_client('sqs', region_name='us-west-1',endpoint_url='http://localhost:5000',)
    resp = await client.get_queue_url(QueueName='test_queue1')
    queue_url = resp['QueueUrl']
    await asyncio.gather(*[asyncio.ensure_future(write_message(client, queue_url, i)) for i in range(10)])
    print("Writing to Queue -- Finished")
    await client.close()


async def handle_read_response(client, queue_url, response_fut):
    response = await response_fut
    if 'Messages' in response:
        for msg in response['Messages']:
            # Need to remove msg from queue or else it'll reappear
            print(f"Received response {msg['Body']} {msg}")
            await client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=msg['ReceiptHandle']
            )


async def read_10_messages():
    print("Reading from Queue -- Starting")
    session = aiobotocore.get_session()
    client = session.create_client('sqs', region_name='us-west-1',endpoint_url='http://localhost:5000',)
    resp = await client.get_queue_url(QueueName='test_queue1')
    queue_url = resp['QueueUrl']
    response_coros = [client.receive_message(
            QueueUrl=queue_url,
            WaitTimeSeconds=2,
        ) for i in range(10)]
    await asyncio.gather(
        *[asyncio.ensure_future(handle_read_response(client, queue_url, response_fut)) for response_fut in response_coros])
    print("Reading from Queue -- Finished")
    await client.close()


async def main_coro():
    await create_queue()
    await asyncio.gather(write_10_messages(), read_10_messages())


def main():
    #p = subprocess.Popen('moto_server sqs', shell=True)
    import time
    start = time.time()
    for i in range(20):
        print("="*50)
        asyncio.get_event_loop().run_until_complete(main_coro())
    print(time.time() - start)
    #p.kill()


if __name__ == '__main__':
    main()
