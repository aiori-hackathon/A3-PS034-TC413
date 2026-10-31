"""
stream_queue.py — Redis Streams Message Queue with In-Memory Consumer Group Fallback.
Provides decoupled, ordered streaming with consumer groups, acking, backpressure,
and pending message inspection/recovery.
"""
import asyncio
import json
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger("stream_queue")


class StreamQueue:
    """
    Redis Streams Producer and Consumer with Consumer Group semantics.
    Falls back gracefully to an in-memory queue if Redis is unreachable.
    """

    def __init__(self, stream_name: str = "telemetry:stream", group_name: str = "analytics_group", redis_url: Optional[str] = "redis://localhost:6379"):
        self.stream_name = stream_name
        self.group_name = group_name
        self.redis_url = redis_url
        self.use_redis = False
        self._redis_client = None

        # In-memory stream storage for standalone/fallback mode
        self._inmem_stream: List[Tuple[str, Dict[str, Any]]] = []
        self._inmem_id_counter = 0
        self._inmem_group_offsets: Dict[str, int] = {group_name: 0}
        self._inmem_pending: Dict[str, Tuple[str, Dict[str, Any]]] = {}  # msg_id -> (consumer_name, payload)
        self._inmem_event = asyncio.Event()

    async def initialize(self):
        if not self.redis_url:
            self.use_redis = False
            logger.info("No Redis URL specified; using built-in in-memory consumer-group stream.")
            return

        try:
            import redis.asyncio as aioredis
            client = aioredis.from_url(self.redis_url, decode_responses=True, socket_timeout=2.0)
            await client.ping()
            self._redis_client = client
            self.use_redis = True
            logger.info(f"Connected to Redis Streams at {self.redis_url}")

            # Create consumer group if not exists
            try:
                await client.xgroup_create(self.stream_name, self.group_name, id="0", mkstream=True)
            except Exception as e:
                if "BUSYGROUP" not in str(e):
                    logger.warning(f"Error creating Redis stream group: {e}")
        except Exception as e:
            self.use_redis = False
            self._redis_client = None
            logger.info(f"Redis not available ({e}); using built-in in-memory consumer-group stream.")

    async def produce(self, payload: Dict[str, Any]) -> str:
        """
        Produce a record onto the stream. Enforces maxlen for buffer bound.
        """
        data_str = json.dumps(payload)
        if self.use_redis and self._redis_client:
            try:
                msg_id = await self._redis_client.xadd(
                    self.stream_name,
                    {"data": data_str},
                    maxlen=10000,
                    approximate=True,
                )
                return msg_id
            except Exception as e:
                logger.error(f"Redis xadd error: {e}, transitioning to in-memory fallback")
                self.use_redis = False
                self._redis_client = None

        # In-memory produce
        self._inmem_id_counter += 1
        msg_id = f"{int(time.time() * 1000)}-{self._inmem_id_counter}"
        self._inmem_stream.append((msg_id, payload))
        if len(self._inmem_stream) > 10000:
            self._inmem_stream.pop(0)
            if self._inmem_group_offsets.get(self.group_name, 0) > 0:
                self._inmem_group_offsets[self.group_name] -= 1

        self._inmem_event.set()
        return msg_id

    async def consume_group(self, consumer_name: str, count: int = 10, block_ms: int = 2000) -> List[Tuple[str, Dict[str, Any]]]:
        """
        Consume messages for consumer_name in consumer group.
        """
        if self.use_redis and self._redis_client:
            try:
                res = await self._redis_client.xreadgroup(
                    groupname=self.group_name,
                    consumername=consumer_name,
                    streams={self.stream_name: ">"},
                    count=count,
                    block=block_ms,
                )
                messages = []
                if res:
                    for _, entries in res:
                        for m_id, fields in entries:
                            data_obj = json.loads(fields.get("data", "{}"))
                            messages.append((m_id, data_obj))
                return messages
            except Exception as e:
                logger.error(f"Redis xreadgroup error: {e}, falling back to in-memory")
                self.use_redis = False
                self._redis_client = None

        # In-memory consume logic
        offset = self._inmem_group_offsets.get(self.group_name, 0)
        if offset >= len(self._inmem_stream):
            # Wait for event or timeout
            try:
                self._inmem_event.clear()
                await asyncio.wait_for(self._inmem_event.wait(), timeout=block_ms / 1000.0)
            except asyncio.TimeoutError:
                return []

        offset = self._inmem_group_offsets.get(self.group_name, 0)
        slice_end = min(offset + count, len(self._inmem_stream))
        batch = self._inmem_stream[offset:slice_end]
        self._inmem_group_offsets[self.group_name] = slice_end

        for m_id, item in batch:
            self._inmem_pending[m_id] = (consumer_name, item)

        return batch

    async def ack(self, msg_id: str):
        """
        Acknowledge message processing.
        """
        if self.use_redis and self._redis_client:
            try:
                await self._redis_client.xack(self.stream_name, self.group_name, msg_id)
            except Exception as e:
                logger.error(f"Redis xack error: {e}")
        else:
            self._inmem_pending.pop(msg_id, None)

    async def inspect_pending(self) -> Dict[str, Any]:
        """
        Inspect unacknowledged pending messages in the Pending Entries List (PEL).
        """
        if self.use_redis and self._redis_client:
            try:
                pending_info = await self._redis_client.xpending(self.stream_name, self.group_name)
                return {
                    "backend": "redis_streams",
                    "pending_count": pending_info.get("pending", 0) if isinstance(pending_info, dict) else len(pending_info),
                    "raw": pending_info,
                }
            except Exception as e:
                return {"backend": "redis_streams", "error": str(e)}

        return {
            "backend": "in_memory_streams_fallback",
            "pending_count": len(self._inmem_pending),
            "pending_ids": list(self._inmem_pending.keys()),
        }

    async def recover_pending_messages(self, consumer_name: str, count: int = 50) -> List[Tuple[str, Dict[str, Any]]]:
        """
        Recovers unacknowledged messages assigned to a crashed consumer.
        """
        if self.use_redis and self._redis_client:
            try:
                # Read pending messages for consumer starting from ID '0'
                res = await self._redis_client.xreadgroup(
                    groupname=self.group_name,
                    consumername=consumer_name,
                    streams={self.stream_name: "0"},
                    count=count,
                )
                recovered = []
                if res:
                    for _, entries in res:
                        for m_id, fields in entries:
                            data_obj = json.loads(fields.get("data", "{}"))
                            recovered.append((m_id, data_obj))
                return recovered
            except Exception as e:
                logger.error(f"Redis recover_pending error: {e}")
                return []

        # In-memory pending recovery
        recovered = []
        for m_id, (cname, item) in list(self._inmem_pending.items()):
            recovered.append((m_id, item))
            if len(recovered) >= count:
                break
        return recovered

    async def get_stream_stats(self) -> Dict[str, Any]:
        if self.use_redis and self._redis_client:
            try:
                info = await self._redis_client.xinfo_stream(self.stream_name)
                groups = await self._redis_client.xinfo_groups(self.stream_name)
                return {
                    "backend": "redis_streams",
                    "length": info.get("length", 0),
                    "groups": groups,
                }
            except Exception:
                pass

        return {
            "backend": "in_memory_streams_fallback",
            "total_stream_entries": len(self._inmem_stream),
            "consumer_offset": self._inmem_group_offsets.get(self.group_name, 0),
            "pending_acks": len(self._inmem_pending),
        }

    async def close(self):
        if self._redis_client:
            try:
                await self._redis_client.close()
            except Exception:
                pass
            self._redis_client = None
