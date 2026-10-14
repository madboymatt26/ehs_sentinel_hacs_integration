"""Serialized NASA requests with bounded, source-correlated readback evidence."""
import asyncio
import math

from .nasa_message import NASAMessage
from .nasa_packet import NASAPacket, AddressClassEnum, PacketType, DataType


class MessageProducer:
    _CHUNKSIZE = 10
    CONFIRM_TIMEOUT = 4
    READ_ATTEMPTS = 3

    def __init__(self, hass, coordinator):
        self.hass = hass
        self.coordinator = coordinator
        self._logger = coordinator._logger
        self.writer = None
        self._transaction_lock = asyncio.Lock()

    def set_writer(self, writer):
        if writer is not self.writer:
            self.writer = writer
            self.coordinator.set_transport(writer is not None)

    def _ready(self):
        return bool(self.writer and not self.writer.is_closing()
                    and self.coordinator.source_evidence.connected
                    and self.coordinator.indoor_address and self.coordinator.outdoor_address)

    def _transport_failed(self):
        try:
            if self.writer is not None:
                self.writer.close()  # Unblock the reader so the existing loop reconnects.
        finally:
            self.set_writer(None)

    async def _wait_confirmations(self, events):
        # A timed-out task is done, but is not a successful acknowledgement.
        tasks = [asyncio.create_task(event.wait()) for event in events]
        try:
            results = await asyncio.wait_for(asyncio.gather(*tasks), self.CONFIRM_TIMEOUT)
            return bool(events) and all(results) and all(event.is_set() for event in events)
        except TimeoutError:
            return False
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def read_request(self, list_of_messages: list, retry_mode=False):
        async with self._transaction_lock:
            return await self._read_request(list_of_messages, retry_mode)

    async def _read_request(self, names, retry_mode=False):
        if not self._ready() or not names or len(set(names)) != len(names):
            return False
        sources = dict.fromkeys(self.coordinator.expected_source(name) for name in names)
        epoch = self.coordinator.source_evidence.epoch
        for source in sources:
            group = [name for name in names if self.coordinator.expected_source(name) == source]
            for start in range(0, len(group), self._CHUNKSIZE):
                chunk = group[start:start + self._CHUNKSIZE]
                packet = self._build_default_read_packet()
                packet.set_packet_messages([self._build_message(name) for name in chunk])
                if source == (self.coordinator.outdoor_address['class'],
                              self.coordinator.outdoor_address['channel'], self.coordinator.outdoor_address['address']):
                    packet.set_packet_dest_address_class(AddressClassEnum(source[0]))
                    packet.set_packet_dest_channel(source[1])
                    packet.set_packet_dest_address(source[2])
                for attempt in range(self.READ_ATTEMPTS if retry_mode else 1):
                    if not self._ready() or epoch != self.coordinator.source_evidence.epoch:
                        return False
                    await asyncio.sleep(0.5)
                    events = [self.coordinator.create_read_confirmation(name) for name in chunk] if retry_mode else []
                    try:
                        await self._write_packet_to_serial(packet)
                        if not retry_mode or await self._wait_confirmations(events):
                            break
                    except (OSError, ConnectionError):
                        self._transport_failed()
                        return False
                    finally:
                        for name in chunk:
                            self.coordinator._read_confirmations.pop(name, None)
                else:
                    self._logger.error('No fresh read confirmation for %s', chunk)
                    return False
        return self._ready() and epoch == self.coordinator.source_evidence.epoch

    async def write_request(self, message, value, read_request_after=False,
                            source_address_class=None, source_channel=None, source_address=None,
                            dest_address_class=None, dest_channel=None, dest_address=None,
                            packet_type=None, data_type=None):
        async with self._transaction_lock:
            names = [message] if not isinstance(message, list) else message
            values = [value] if not isinstance(value, list) else value
            if (not self.coordinator.writemode or not read_request_after or not self._ready()
                    or not names or len(names) != len(values)
                    or any(not isinstance(name, str) for name in names)):
                return False
            names = [name.strip() for name in names]
            if len(names) != len(set(names)):
                return False
            try:
                if any(not self.coordinator.nasa_repo[name]['hass_opts']['writable']
                       or not self.coordinator.is_message_fresh(name) for name in names):
                    return False
                sources = {self.coordinator.expected_source(name) for name in names}
                if len(sources) != 1 or len(names) > self._CHUNKSIZE:
                    return False
                source = sources.pop()
                decoded = [self._decode_value(name, val) for name, val in zip(names, values, strict=True)]
                messages = [self._build_message(name, val) for name, val in zip(names, decoded, strict=True)]
                packet = self._build_default_request_packet()
                packet.set_packet_messages(messages)
                packet.set_packet_dest_address_class(AddressClassEnum(source[0]))
                packet.set_packet_dest_channel(source[1])
                packet.set_packet_dest_address(source[2])
                # A different destination or ACK/NACK must not count as convergence.
                if dest_address_class is not None and AddressClassEnum[dest_address_class].value != source[0]:
                    return False
                if dest_channel is not None and dest_channel != source[1]:
                    return False
                if dest_address is not None and dest_address != source[2]:
                    return False
                if source_address_class is not None:
                    packet.set_packet_source_address_class(AddressClassEnum[source_address_class])
                if source_channel is not None:
                    packet.set_packet_source_channel(source_channel)
                if source_address is not None:
                    packet.set_packet_source_address(source_address)
                if packet_type is not None:
                    packet.set_packet_type(PacketType[packet_type])
                if data_type is not None:
                    if DataType[data_type] not in (DataType.Request, DataType.Write):
                        return False
                    packet.set_packet_data_type(DataType[data_type])
                expected = [await self.coordinator.determine_value(msg.packet_payload, name, msg.packet_message_type)
                            for msg, name in zip(messages, names, strict=True)]
                if any(val is None for val in expected):
                    return False
                packet.to_raw()
            except (KeyError, ValueError, TypeError, OverflowError, StopIteration):
                return False

            epoch = self.coordinator.source_evidence.epoch
            events = [self.coordinator.create_write_confirmation(name, val, source)
                      for name, val in zip(names, expected, strict=True)]
            try:
                if not self._ready() or epoch != self.coordinator.source_evidence.epoch:
                    return False
                await self._write_packet_to_serial(packet)
                # Command ONCE; retry observations, not uncertain physical actions.
                for _ in range(self.READ_ATTEMPTS):
                    if not await self._read_request(names):
                        return False
                    if await self._wait_confirmations(events):
                        return self._ready() and epoch == self.coordinator.source_evidence.epoch
                self._logger.error('Write not confirmed for %s; device outcome is unknown', names)
                return False
            except (OSError, ConnectionError):
                self._transport_failed()
                return False
            finally:
                for name in names:
                    self.coordinator._write_confirmations.pop(name, None)

    def _decode_value(self, message, value):
        meta = self.coordinator.nasa_repo[message]
        if meta.get('type') == 'ENUM':
            return int(next(key for key, label in meta['enum'].items() if label == value))
        numeric = float(value)
        if not math.isfinite(numeric):
            raise ValueError('Non-finite NASA value')
        bounds = meta['hass_opts'].get('platform', {})
        if numeric < bounds.get('min', -math.inf) or numeric > bounds.get('max', math.inf):
            raise ValueError('NASA value outside device bounds')
        if 'step' in bounds:
            steps = (numeric - bounds.get('min', 0)) / bounds['step']
            if not math.isclose(steps, round(steps), abs_tol=1e-6):
                raise ValueError('NASA value not on a supported device step')
        value = numeric
        return int(eval(meta.get('reverse-arithmetic', 'value'), {'__builtins__': {}}, {'value': value}))

    def _build_message(self, message, value=0):
        msg = NASAMessage()
        msg.set_packet_message(int(self.coordinator.nasa_repo[message]['address'], 16))
        sizes = {0: 1, 1: 2, 2: 4, 3: 1}
        msg.set_packet_payload_raw(value.to_bytes(sizes[msg.packet_message_type], byteorder='big', signed=True))
        return msg

    def _build_default_read_packet(self):
        packet = self._build_default_request_packet()
        packet.set_packet_source_channel(255)
        packet.set_packet_source_address(0)
        packet.set_packet_dest_address_class(AddressClassEnum.BroadcastSetLayer)
        packet.set_packet_dest_channel(self.coordinator.indoor_address['channel'])
        packet.set_packet_dest_address(32)
        packet.set_packet_data_type(DataType.Read)
        return packet

    def _build_default_request_packet(self):
        packet = NASAPacket()
        packet.set_packet_source_address_class(AddressClassEnum.JIGTester)
        packet.set_packet_source_channel(0)
        packet.set_packet_source_address(255)
        packet.set_packet_dest_address_class(AddressClassEnum(self.coordinator.indoor_address['class']))
        packet.set_packet_dest_channel(self.coordinator.indoor_address['channel'])
        packet.set_packet_dest_address(self.coordinator.indoor_address['address'])
        packet.set_packet_information(True)
        packet.set_packet_version(2)
        packet.set_packet_retry_count(0)
        packet.set_packet_type(PacketType.Normal)
        packet.set_packet_data_type(DataType.Request)
        packet.set_packet_number(166)
        return packet

    async def _write_packet_to_serial(self, packet):
        if not self._ready():
            raise ConnectionError('EHS transport unavailable')
        self.writer.write(packet.to_raw())
        await asyncio.wait_for(self.writer.drain(), timeout=5)
