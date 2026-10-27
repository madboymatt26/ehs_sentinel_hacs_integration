"""Shared availability and confirmed-write behavior; no optimistic state writes."""

from homeassistant.exceptions import HomeAssistantError


class SourceSafeEntity:
    # Keep the authoritative receipt clock live for safety checks, but do not
    # create a unique recorder attribute dictionary for each unchanged receipt.
    # Availability/source_fresh transitions remain recorded.
    _unrecorded_attributes = frozenset({'nasa_last_seen'})

    @property
    def available(self):
        data = next((items[self._key] for items in self.coordinator.data.values()
                     if self._key in items), {})
        return bool(data.get('seen_once') and self.coordinator.is_message_fresh(self._nasa_name))

    @property
    def extra_state_attributes(self):
        data = next((items[self._key] for items in self.coordinator.data.values()
                     if self._key in items), {})
        return {
            'nasa_name': self._nasa_name,
            'nasa_last_seen': data.get('nasa_last_seen'),
            'source_fresh': self.available,
            'source_max_age_seconds': self.coordinator.message_max_age(self._nasa_name),
        }

    async def confirmed_write(self, value):
        if not self.available or not await self.coordinator.producer.write_request(
                message=self._nasa_name, value=str(value), read_request_after=True):
            raise HomeAssistantError(f'EHS command for {self._nasa_name} not confirmed; state may be unchanged or uncertain')
