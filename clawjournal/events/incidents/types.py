"""Constants and lightweight types for the incidents pipeline."""

from __future__ import annotations

LOOP_INCIDENT_KIND = "loop_exact_repeat"

# Future beats add entries here as they introduce new kinds. Keeping
# the set explicit lets the schema CHECK / read-time validators stay
# in sync with the producer code.
ValidIncidentKinds = frozenset({LOOP_INCIDENT_KIND})
