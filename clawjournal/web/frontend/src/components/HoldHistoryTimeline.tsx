import { useCallback, useEffect, useState } from 'react';
import { api } from '../api.ts';
import type { HoldHistoryEntry } from '../types.ts';

interface HoldHistoryTimelineProps {
  sessionId: string;
  /** When the banner mutates state, pass a nonce so this re-fetches. */
  refreshKey?: number;
}

/**
 * Expandable timeline of hold-state transitions for a session.
 *
 * Reads `GET /api/sessions/<id>/hold-history`; each row is an
 * append-only audit entry written inside the same transaction as the
 * underlying state change, so the timeline can't disagree with the
 * session's current state.
 */
export function HoldHistoryTimeline({ sessionId, refreshKey }: HoldHistoryTimelineProps) {
  const [history, setHistory] = useState<HoldHistoryEntry[]>([]);
  const [expanded, setExpanded] = useState(false);
  const [loading, setLoading] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await api.sessions.holdHistory(sessionId);
      setHistory(res.history);
    } finally {
      setLoading(false);
    }
  }, [sessionId]);

  useEffect(() => {
    if (expanded) {
      load();
    }
  }, [expanded, refreshKey, load]);

  return (
    <details
      className="hold-history"
      open={expanded}
      onToggle={(e) => setExpanded((e.target as HTMLDetailsElement).open)}
    >
      <summary>Hold history</summary>
      {loading && <div className="hold-history__loading">Loading…</div>}
      {!loading && history.length === 0 && (
        <div className="hold-history__empty">No transitions recorded yet.</div>
      )}
      <ol className="hold-history__list">
        {history.map((row) => (
          <li key={row.history_id} className="hold-history__row">
            <span className="hold-history__time">
              {new Date(row.changed_at).toLocaleString()}
            </span>
            <span className="hold-history__transition">
              {row.from_state ?? '—'} → <strong>{row.to_state}</strong>
            </span>
            <span className="hold-history__actor">({row.changed_by})</span>
            {row.embargo_until && (
              <span className="hold-history__embargo">
                embargo until {new Date(row.embargo_until).toLocaleDateString()}
              </span>
            )}
            {row.reason && <span className="hold-history__reason">{row.reason}</span>}
          </li>
        ))}
      </ol>
    </details>
  );
}
