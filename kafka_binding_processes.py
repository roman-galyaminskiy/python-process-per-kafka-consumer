import asyncio
import os
from datetime import timedelta
from multiprocessing import Process, Event
from signal import signal, SIGINT, SIG_IGN

from aiokafka import AIOKafkaConsumer, ConsumerRecord, TopicPartition
from aiokafka import AIOKafkaProducer

from config import KafkaBindingConfig


class KafkaBindingProcesses:
    def __init__(self, config: KafkaBindingConfig):
        super().__init__()
        self.__config = config

        # keeping references for started tasks: https://github.com/python/cpython/issues/88831
        self.__running_tasks = set()
        self.__shutdown_event = Event()

    async def on_startup(self):
        self.processes = [
            Process(
                target=self._process_event_loop,
                args=(
                    partition,
                    self.__shutdown_event,
                ),
            )
            for partition in range(self.__config.incoming.partitions)
        ]
        for process in self.processes:
            process.start()

    def _process_event_loop(
            self,
            partition: int,
            shutdown_event: Event,
    ):
        print(f"{os.getpid()} starting process event loop")
        # ingore Ctrl + C so we could stop consumer inside a coroutine
        signal(SIGINT, SIG_IGN)
        asyncio.run(self._listen(partition, shutdown_event))

    async def _listen(self, partition: int, shutdown_event: Event):
        print(f"{os.getpid()} listening")
        self._consumer = AIOKafkaConsumer(
            bootstrap_servers=self.__config.bootstrap.servers,
            group_id=self.__config.incoming.group.id,
            enable_auto_commit=False,
        )
        self._consumer.assign([TopicPartition(self.__config.incoming.topic, partition)])
        await self._consumer.start()
        print(f"{os.getpid()} consumer started")

        while True:
            if shutdown_event.is_set():
                await asyncio.gather(*self.__running_tasks)
                await self._consumer.stop()
                print(f"{os.getpid()} consumer stopped")
                break

            # only `getmany` supports pausing, not `getone` nor `__aiter__`
            # code pausing event is omitted for simplicity
            msgs = await self._consumer.getmany(
                timeout_ms=int(
                    self.__config.incoming.poll.timeout / timedelta(milliseconds=1)
                )
            )

            for tp, messages in msgs.items():
                for msg in messages:
                    task = asyncio.create_task(self._on_event(msg))
                    self.__running_tasks.add(task)
                    task.add_done_callback(lambda t: self.__running_tasks.remove(t))

    async def _on_event(self, record: ConsumerRecord):
        try:
            await self._dummy_handler(record)
            await self._ack(record)
        except Exception as ex:
            await self._dlq(record, ex)

    async def _dummy_handler(self, record: ConsumerRecord):
        print(f"{os.getpid()} dummy_handler", record)

    async def on_shutdown(self):
        self.__shutdown_event.set()
        for process in self.processes:
            process.join()

    async def _ack(self, record: ConsumerRecord):
        task = asyncio.create_task(
            self._consumer.commit(
                {TopicPartition(record.topic, record.partition): record.offset}
            )
        )
        self.__running_tasks.add(task)
        task.add_done_callback(lambda t: (self.__running_tasks.remove(t)))

    async def _dlq(self, record: ConsumerRecord, ex: Exception):
        async with AIOKafkaProducer(
                bootstrap_servers=self.__config.bootstrap.servers,
        ) as producer:
            await producer.send_and_wait(
                "dead-letter-topic-" + self.__config.incoming.topic,
                key=record.key,
                value=record.value,
                headers=list(record.headers),
            )
