import asyncio
from concurrent.futures import ProcessPoolExecutor
from datetime import timedelta
from signal import signal, SIGINT, SIG_IGN

from aiokafka import AIOKafkaConsumer, ConsumerRecord, TopicPartition
from aiokafka import AIOKafkaProducer

from config import KafkaBindingConfig


class KafkaBindingExecutor:
    def __init__(self, config: KafkaBindingConfig):
        super().__init__()
        self._config = config

        self._running_tasks = set()

    async def on_startup(self):
        self.__exec = ProcessPoolExecutor(self._config.incoming.partitions)
        for partition in range(self._config.incoming.partitions):
            self.__exec.submit(self.__process_event_loop, partition)

    def __process_event_loop(
            self,
            partition: int,
    ):
        print("starting process event loop")
        # logger.info("starting process event loop")
        # ingore Ctrl + C so we could stop consumer inside a coroutine
        signal(SIGINT, SIG_IGN)
        asyncio.run(self.__listen(partition))

    async def __listen(self, partition: int):
        print("listening")
        self._consumer = AIOKafkaConsumer(
            bootstrap_servers=self._config.bootstrap.servers,
            group_id=self._config.incoming.group.id,
            enable_auto_commit=False,
        )
        self._consumer.assign([TopicPartition(self._config.incoming.topic, partition)])
        await self._consumer.start()
        print("consumer started")

        while True:
            # if shutdown_event.is_set():
            #     await asyncio.gather(*self._running_tasks)
            #     await self._consumer.stop()
            #     print("consumer stopped")
            #     break

            try:
                # only `getmany` supports pausing, not `getone` nor `__aiter__`
                msgs = await self._consumer.getmany(
                    timeout_ms=int(
                        self._config.incoming.poll.timeout / timedelta(milliseconds=1)
                    )
                )

                for tp, messages in msgs.items():
                    for msg in messages:
                        task = asyncio.create_task(self.__on_event(msg))
                        self._running_tasks.add(task)
                        task.add_done_callback(lambda t: self._running_tasks.remove(t))
            except KeyboardInterrupt as ex:
                print("keyboard interrupt")
                break

    async def __on_event(self, record: ConsumerRecord):
        try:
            await self.__dummy_handler(record)
            await self._ack(record)
        except Exception as ex:
            await self._dlq(record, ex)

    async def __dummy_handler(self, record: ConsumerRecord):
        print(record)

    async def on_shutdown(self):
        print("on_shutdown")
        self.__exec.shutdown()

    async def _ack(self, record: ConsumerRecord):
        task = asyncio.create_task(
            self._consumer.commit(
                {TopicPartition(record.topic, record.partition): record.offset}
            )
        )
        self._running_tasks.add(task)
        task.add_done_callback(lambda t: (self._running_tasks.remove(t)))

    async def _dlq(self, record: ConsumerRecord, ex: Exception):
        async with AIOKafkaProducer(
                bootstrap_servers=self._config.bootstrap.servers,
        ) as producer:
            await producer.send_and_wait(
                "dead-letter-topic-" + self._config.incoming.topic,
                key=record.key,
                value=record.value,
                headers=list(record.headers),
            )
