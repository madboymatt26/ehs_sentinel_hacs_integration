"""Real EHS producer/coordinator, fake transport: no sockets or devices."""
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import yaml
from homeassistant.core import State
from homeassistant.exceptions import HomeAssistantError, ServiceValidationError
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.ehs_sentinel import async_send_signal_service, async_import_fsv_file_service
from custom_components.ehs_sentinel.coordinator import EHSSentinelCoordinator
from custom_components.ehs_sentinel.freshness import SourceEvidence
from custom_components.ehs_sentinel.nasa_packet import DataType, AddressClassEnum
from custom_components.ehs_sentinel.sensor import EHSSentinelSensor
from custom_components.ehs_sentinel.number import EHSSentinelNumber
from custom_components.ehs_sentinel.select import EHSSentinelSelect
from custom_components.ehs_sentinel.switch import EHSSentinelSwitch
from custom_components.ehs_sentinel.binary_sensor import EHSSentinelBinarySensor

ROOT = Path(__file__).resolve().parents[3]
CURVE = 'VAR_IN_TEMP_WATER_LAW_TARGET_F'
DHW = 'NASA_INDOOR_DHW_SET_TEMP'


@pytest.fixture
def co(hass):
    repo = yaml.safe_load((ROOT / 'custom_components/ehs_sentinel/data/nasa_repository.yml').read_text())
    config = dict(ip='127.0.0.1', port=1234, name='Test', write_mode=True, polling=True,
                  extended_logging=False, diagnostic_logs=False, force_refresh=False,
                  polling_yaml='fetch_interval: []\ngroups: {}')
    co = EHSSentinelCoordinator(hass, MockConfigEntry(domain='ehs_sentinel', title='Test'), config, repo)
    co.indoor_address = {'class': 32, 'channel': 0, 'address': 1}
    co.outdoor_address = {'class': 16, 'channel': 0, 'address': 0}
    writer = MagicMock()
    writer.is_closing.return_value = False
    writer.drain = AsyncMock()
    co.producer.set_writer(writer)
    co.producer.CONFIRM_TIMEOUT = 0.001
    for name in (CURVE, DHW, 'NASA_DHW_OPMODE', 'NASA_DHW_POWER'):
        clock, stamp = SourceEvidence.clock()
        co.source_evidence.record(name, clock, co.source_evidence.epoch, stamp)
    return co


def receipt(co, name=CURVE, **overrides):
    clock, stamp = SourceEvidence.clock()
    return dict(received=clock, timestamp=stamp, epoch=co.source_evidence.epoch,
                source=co.expected_source(name), data_type=DataType.Resposne) | overrides


@pytest.fixture
def no_pacing():
    original = asyncio.sleep
    async def yield_only(_):
        await original(0)
    with patch('custom_components.ehs_sentinel.message_producer.asyncio.sleep', yield_only):
        yield


async def test_zero_ack_is_failure_without_resending_command(co, no_pacing):
    assert not await co.producer.write_request(CURVE, '1', read_request_after=True)
    assert co.producer.writer.write.call_count == 4  # one command, three readbacks
    assert not co._write_confirmations
    assert not co._read_confirmations


async def test_read_timeout_retries_and_returns_false(co, no_pacing):
    assert not await co.producer.read_request([CURVE], retry_mode=True)
    assert co.producer.writer.write.call_count == 3
    assert not co._read_confirmations


async def test_matching_post_command_readback_is_success(co, no_pacing):
    async def confirm():
        co.confirm_write(CURVE, 1, receipt(co))
        co.confirm_read(CURVE, receipt(co))
    co.producer.writer.drain.side_effect = confirm
    assert await co.producer.write_request(CURVE, '1', read_request_after=True)
    assert not co._write_confirmations


@pytest.mark.parametrize('overrides', [
    {'received': -1}, {'epoch': -1}, {'source': (16, 0, 0)},
    {'data_type': DataType.Nack}, {'data_type': DataType.Ack}, {'data_type': DataType.Request},
])
async def test_old_wrong_source_and_non_state_packets_do_not_ack(co, overrides):
    event = co.create_write_confirmation(CURVE, 1)
    co.confirm_write(CURVE, 1, receipt(co, **overrides))
    assert not event.is_set()
    co.confirm_write(CURVE, 2, receipt(co))
    assert not event.is_set()


async def test_partial_batch_ack_is_not_success(co, no_pacing):
    async def confirm():
        co.confirm_write(CURVE, 1, receipt(co))
    co.producer.writer.drain.side_effect = confirm
    assert not await co.producer.write_request([CURVE, DHW], ['1', '50'], read_request_after=True)
    assert not co._write_confirmations


@pytest.mark.parametrize('messages,values', [
    ([CURVE, DHW], ['1']), ([CURVE, CURVE], ['1', '2']),
    ([CURVE], ['nan']), ([CURVE], ['inf']), ([CURVE], ['6']),
    ([CURVE], ['1.2']),
    (['missing'], ['1']), (['NASA_DHW_OPMODE'], ['not-a-mode']),
])
async def test_invalid_command_sends_nothing(co, messages, values):
    assert not await co.producer.write_request(messages, values, read_request_after=True)
    co.producer.writer.write.assert_not_called()


async def test_disabled_write_mode_and_unconfirmed_path_send_nothing(co):
    co.writemode = False
    assert not await co.producer.write_request(CURVE, '1', read_request_after=True)
    co.writemode = True
    assert not await co.producer.write_request(CURVE, '1', read_request_after=False)
    co.producer.writer.write.assert_not_called()


async def test_cancellation_cleans_waiters_and_lock(co, no_pacing):
    co.producer.CONFIRM_TIMEOUT = 10
    task = asyncio.create_task(co.producer.write_request(CURVE, '1', read_request_after=True))
    for _ in range(10):
        await asyncio.sleep(0)
        if co._write_confirmations:
            break
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not co._write_confirmations
    assert not co.producer._transaction_lock.locked()


async def test_transport_failure_invalidates_sources(co, no_pacing):
    co.producer.writer.drain.side_effect = BrokenPipeError()
    assert not await co.producer.write_request(CURVE, '1', read_request_after=True)
    assert not co.is_message_fresh(CURVE)
    assert not co.source_evidence.connected


async def test_reconnect_rejects_previous_epoch_and_cached_values(co):
    old = receipt(co)
    co.set_transport(False)
    co.set_transport(True)
    assert not co.is_message_fresh(CURVE)
    event = co.create_write_confirmation(CURVE, 1)
    co.confirm_write(CURVE, 1, old)
    assert not event.is_set()
    assert not co.source_evidence.record(CURVE, old['received'], old['epoch'], old['timestamp'])


async def test_unchanged_valid_packet_refreshes_only_its_register(co):
    co.source_evidence.receipts.clear()
    packet = co.producer._build_default_request_packet()
    packet.set_packet_source_address_class(AddressClassEnum.Indoor)
    packet.set_packet_source_channel(0)
    packet.set_packet_source_address(1)
    packet.set_packet_data_type(DataType.Notification)
    packet.set_packet_messages([co.producer._build_message(CURVE, 10)])
    await co.process_packet(packet.to_raw(), receipt(co))
    assert co.is_message_fresh(CURVE)
    assert not co.is_message_fresh(DHW)
    before = co.source_evidence.receipts[CURVE]
    await co.process_packet(packet.to_raw(), receipt(co))
    assert co.source_evidence.receipts[CURVE][0] >= before[0]
    key = co.processor._normalize_name(CURVE)
    assert co.data['number'][key]['value'] == 1
    assert co.data['number'][key]['nasa_last_seen'].endswith('+00:00')


async def test_parallel_worker_cannot_move_clock_backwards(co):
    old = receipt(co)
    clock, stamp = SourceEvidence.clock()
    co.source_evidence.record(CURVE, clock + 1, co.source_evidence.epoch, stamp)
    assert not co.source_evidence.record(CURVE, old['received'], old['epoch'], old['timestamp'])


async def test_source_expiry_and_fsv_poll_budget(co):
    clock, _ = SourceEvidence.clock()
    co.source_evidence.receipts[CURVE] = (clock - 901, '2000-01-01T00:00:00+00:00')
    assert not co.is_message_fresh(CURVE)
    co.polling_yaml = {'groups': {'FSV': ['ENUM_IN_FSV_3011']},
                       'fetch_interval': [{'name': 'FSV', 'enable': True, 'schedule': '30m'}]}
    assert co.message_max_age('ENUM_IN_FSV_3011') == 3630
    assert co.message_max_age(CURVE) == 900
    co.polling = False
    assert co.message_max_age('ENUM_IN_FSV_3011') == 900


@pytest.mark.parametrize('platform,cls,name,value', [
    ('sensor', EHSSentinelSensor, 'NASA_INDOOR_DHW_CURRENT_TEMP', '45'),
    ('number', EHSSentinelNumber, DHW, '50'),
    ('select', EHSSentinelSelect, 'NASA_DHW_OPMODE', 'Eco'),
    ('switch', EHSSentinelSwitch, 'NASA_DHW_POWER', 'on'),
    ('binary_sensor', EHSSentinelBinarySensor, 'NASA_DHW_POWER', 'on'),
])
async def test_restore_is_not_evidence_and_never_overwrites_packet(hass, co, platform, cls, name, value):
    co.source_evidence.receipts.clear()
    key = co.processor._normalize_name(name)
    co.data.setdefault(platform, {}).setdefault(key, {'value': None, 'seen_once': False})
    entity = cls(co, key, nasa_name=name)
    entity.hass = hass
    restored = State(f'{platform}.test', value, {'seen_once': True, 'source_fresh': True,
                                              'nasa_last_seen': datetime.now(timezone.utc).isoformat()})
    with patch.object(entity, 'async_get_last_state', AsyncMock(return_value=restored)), \
         patch.object(entity, 'async_write_ha_state'):
        await entity.async_added_to_hass()
        assert not entity.available
        assert entity.extra_state_attributes['source_fresh'] is False
        co.data[platform][key].update(value='new-device-value', seen_once=True)
        await entity.async_added_to_hass()
        assert co.data[platform][key]['value'] == 'new-device-value'
    await entity.async_will_remove_from_hass()


async def test_entity_and_service_report_failed_write(co, hass):
    key = co.processor._normalize_name(CURVE)
    entity = EHSSentinelNumber(co, key, nasa_name=CURVE)
    co.data['number'][key]['seen_once'] = True
    co.producer.write_request = AsyncMock(return_value=False)
    with pytest.raises(HomeAssistantError, match='not confirmed'):
        await entity.async_set_native_value(1)
    hass.data['ehs_sentinel'] = {'test': co}
    call = MagicMock(hass=hass, data={'nasa_key': CURVE, 'nasa_value': '1'})
    with pytest.raises(ServiceValidationError, match='not confirmed'):
        await async_send_signal_service(call)


async def test_concurrent_requests_cannot_replace_confirmation_events(co, no_pacing):
    seen = []
    async def confirm():
        for name, info in list(co._write_confirmations.items()):
            seen.append(name)
            co.confirm_write(name, info['value'], receipt(co, name))
    co.producer.writer.drain.side_effect = confirm
    results = await asyncio.gather(
        co.producer.write_request(CURVE, '1', read_request_after=True),
        co.producer.write_request(DHW, '50', read_request_after=True))
    assert results == [True, True]
    assert seen == [CURVE, DHW]
    assert not co._write_confirmations


async def test_read_success_requires_actual_event(co, no_pacing):
    async def confirm():
        co.confirm_read(CURVE, receipt(co))
    co.producer.writer.drain.side_effect = confirm
    assert await co.producer.read_request([CURVE], retry_mode=True)
    assert co.producer.writer.write.call_count == 1
    assert not co._read_confirmations


async def test_stale_source_and_wrong_destination_send_nothing(co):
    assert not await co.producer.write_request(CURVE, '1', read_request_after=True, dest_address=99)
    co.source_evidence.receipts.clear()
    assert not await co.producer.write_request(CURVE, '1', read_request_after=True)
    co.producer.writer.write.assert_not_called()


@pytest.mark.parametrize('cls,platform,name,method,args', [
    (EHSSentinelNumber, 'number', CURVE, 'async_set_native_value', (1,)),
    (EHSSentinelSelect, 'select', 'NASA_DHW_OPMODE', 'async_select_option', ('Eco',)),
    (EHSSentinelSwitch, 'switch', 'NASA_DHW_POWER', 'async_turn_on', ()),
    (EHSSentinelSwitch, 'switch', 'NASA_DHW_POWER', 'async_turn_off', ()),
])
async def test_all_writable_platforms_propagate_failed_ack(co, cls, platform, name, method, args):
    key = co.processor._normalize_name(name)
    co.data[platform][key].update(value=0, seen_once=True)
    entity = cls(co, key, nasa_name=name)
    co.producer.write_request = AsyncMock(return_value=False)
    with pytest.raises(HomeAssistantError, match='not confirmed'):
        await getattr(entity, method)(*args)
    assert co.data[platform][key]['value'] == 0


async def test_import_stops_at_first_failed_field(hass, co, tmp_path):
    hass.config.config_dir = str(tmp_path)
    directory = tmp_path / 'www/ehs_sentinel/logs'
    directory.mkdir(parents=True)
    keys = [k for k, meta in co.nasa_repo.items() if '_FSV_' in k and meta['hass_opts']['writable']][:3]
    (directory / 'test.yaml').write_text(yaml.safe_dump(dict.fromkeys(keys, 'ON')))
    hass.data['ehs_sentinel'] = {'test': co}
    co.producer.write_request = AsyncMock(side_effect=[True, False, True])
    with pytest.raises(ServiceValidationError, match='earlier fields may have changed'):
        await async_import_fsv_file_service(MagicMock(hass=hass, data={'file_name': 'test.yaml'}))
    assert co.producer.write_request.await_count == 2


async def test_real_decoded_device_readback_confirms_off_value(co, no_pacing):
    name = 'NASA_DHW_POWER'
    async def respond():
        packet = co.producer._build_default_request_packet()
        packet.set_packet_source_address_class(AddressClassEnum.Indoor)
        packet.set_packet_source_channel(0)
        packet.set_packet_source_address(1)
        packet.set_packet_data_type(DataType.Resposne)
        packet.set_packet_messages([co.producer._build_message(name, 0)])
        await co.process_packet(packet.to_raw(), receipt(co, name))
    co.producer.writer.drain.side_effect = respond
    assert await co.producer.write_request(name, 'OFF', read_request_after=True)
    key = co.processor._normalize_name(name)
    assert co.data['switch'][key]['value'] == 'OFF'


async def test_disconnect_publishes_unavailable_and_timer_detects_expiry(co):
    key = co.processor._normalize_name(CURVE)
    entity = EHSSentinelNumber(co, key, nasa_name=CURVE)
    entity.hass = co.hass
    co.data['number'][key].update(seen_once=True, _entity=entity)
    assert entity.available
    with patch.object(entity, 'async_write_ha_state') as publish:
        clock, stamp = SourceEvidence.clock()
        co.source_evidence.receipts[CURVE] = (clock - 901, stamp)
        co._publish_freshness()
        assert not entity.available
        publish.assert_called_once()
        co.set_transport(False)
        assert not entity.available
        assert not entity.extra_state_attributes['source_fresh']


async def test_packet_workers_do_not_publish_in_reverse_order(co):
    first_entered, release = asyncio.Event(), asyncio.Event()
    order = []
    async def process(_buffer, info):
        if info['sequence'] == 1:
            first_entered.set()
            await release.wait()
        order.append(info['sequence'])
    with patch.object(co, '_process_packet', process):
        first = asyncio.create_task(co.process_packet(b'', {'sequence': 1}))
        await first_entered.wait()
        second = asyncio.create_task(co.process_packet(b'', {'sequence': 2}))
        await asyncio.sleep(0)
        assert order == []
        release.set()
        await asyncio.gather(first, second)
    assert order == [1, 2]


@pytest.mark.parametrize('first_result', [False, RuntimeError('temporary request error')])
async def test_control_refresh_recovers_and_does_not_starve_power(co, first_result):
    co.producer.read_request = AsyncMock(side_effect=[first_result, True, True, True])
    cycles = 0
    async def next_cycle(seconds):
        nonlocal cycles
        assert seconds == 300
        cycles += 1
        if cycles == 2:
            co.running = False
    with patch('custom_components.ehs_sentinel.coordinator.asyncio.sleep', next_cycle):
        await co._poll_control_sources()
    assert cycles == 2
    calls = co.producer.read_request.await_args_list
    assert len(calls) == 4
    for call in (calls[1], calls[3]):
        assert 'NASA_OUTDOOR_CONTROL_WATTMETER_ALL_UNIT' in call.args[0]
    assert all(call.kwargs['retry_mode'] for call in calls)


async def test_control_refresh_honours_shutdown(co):
    co.producer.read_request = AsyncMock(side_effect=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await co._poll_control_sources()
    assert co.producer.read_request.await_count == 1
