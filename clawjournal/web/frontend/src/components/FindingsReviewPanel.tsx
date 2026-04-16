import { useCallback, useEffect, useState } from 'react';
import { api } from '../api.ts';
import type { FindingEntityGroup } from '../types.ts';

interface FindingsReviewPanelProps {
  sessionId: string;
}

/**
 * Review surface for DB-backed findings.
 *
 * One row per `(engine, entity_type, entity_hash)` group — the API has
 * already deduped by entity so one decision covers every occurrence.
 * Raw match text is never in the response; only a masked preview
 * (`before [...] after`) and the hash prefix. Per-row buttons call
 * `PATCH /api/findings` with every `finding_id` in the group; the
 * "also ignore across all sessions" toggle triggers the retroactive
 * allowlist path, which the server applies in the same transaction.
 */
export function FindingsReviewPanel({ sessionId }: FindingsReviewPanelProps) {
  const [groups, setGroups] = useState<FindingEntityGroup[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showAll, setShowAll] = useState(false);
  const [pendingId, setPendingId] = useState<string | null>(null);
  const [globalIgnore, setGlobalIgnore] = useState(false);
  const [reasonByHash, setReasonByHash] = useState<Record<string, string>>({});

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await api.sessions.findings(sessionId, {
        groupBy: 'entity',
        ...(showAll ? {} : { status: 'open' }),
      });
      setGroups(res.entities ?? []);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      setLoading(false);
    }
  }, [sessionId, showAll]);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const decide = useCallback(
    async (group: FindingEntityGroup, status: 'accepted' | 'ignored') => {
      setPendingId(group.entity_hash);
      try {
        await api.findings.patch(group.finding_ids, status, {
          reason: reasonByHash[group.entity_hash] || undefined,
          global: status === 'ignored' ? globalIgnore : false,
        });
        await refresh();
      } catch (exc) {
        setError(exc instanceof Error ? exc.message : String(exc));
      } finally {
        setPendingId(null);
      }
    },
    [globalIgnore, reasonByHash, refresh],
  );

  if (loading && groups.length === 0) {
    return <div className="findings-panel findings-panel--loading">Loading findings…</div>;
  }

  return (
    <section className="findings-panel">
      <header className="findings-panel__header">
        <h3>Findings</h3>
        <label className="findings-panel__toggle">
          <input
            type="checkbox"
            checked={showAll}
            onChange={(e) => setShowAll(e.target.checked)}
          />{' '}
          Show decided rows
        </label>
        <label className="findings-panel__toggle">
          <input
            type="checkbox"
            checked={globalIgnore}
            onChange={(e) => setGlobalIgnore(e.target.checked)}
          />{' '}
          Also ignore across all sessions
        </label>
      </header>

      {error && <div className="findings-panel__error">{error}</div>}

      {groups.length === 0 && !error && (
        <div className="findings-panel__empty">
          No sensitive entities detected for this session.
        </div>
      )}

      <ul className="findings-panel__list">
        {groups.map((group) => {
          const preview = group.sample_preview;
          const hashPrefix = group.entity_hash.slice(0, 8);
          const isPending = pendingId === group.entity_hash;
          return (
            <li key={group.entity_hash} className="findings-panel__row">
              <div className="findings-panel__meta">
                <span className="findings-panel__badge">{group.engine}</span>
                {group.rule && <span className="findings-panel__badge">{group.rule}</span>}
                {group.entity_type && (
                  <span className="findings-panel__badge">{group.entity_type}</span>
                )}
                <span className="findings-panel__badge findings-panel__badge--mono">
                  #{hashPrefix}
                </span>
                <span className="findings-panel__badge">
                  {group.occurrences}×
                </span>
                <span className="findings-panel__badge">
                  p={group.max_confidence.toFixed(2)}
                </span>
                <span className="findings-panel__badge findings-panel__status">
                  {group.status}
                </span>
              </div>
              {preview && (
                <div className="findings-panel__preview">
                  <span className="findings-panel__ctx">{preview.before}</span>
                  <span className="findings-panel__match">
                    {preview.match_placeholder}
                  </span>
                  <span className="findings-panel__ctx">{preview.after}</span>
                </div>
              )}
              <div className="findings-panel__controls">
                <input
                  type="text"
                  className="findings-panel__reason"
                  placeholder="reason (optional)"
                  value={reasonByHash[group.entity_hash] ?? ''}
                  onChange={(e) =>
                    setReasonByHash((prev) => ({
                      ...prev,
                      [group.entity_hash]: e.target.value,
                    }))
                  }
                />
                <button
                  type="button"
                  className="findings-panel__btn"
                  disabled={isPending}
                  onClick={() => decide(group, 'accepted')}
                >
                  Accept
                </button>
                <button
                  type="button"
                  className="findings-panel__btn findings-panel__btn--ignore"
                  disabled={isPending}
                  onClick={() => decide(group, 'ignored')}
                >
                  Ignore
                </button>
              </div>
            </li>
          );
        })}
      </ul>
    </section>
  );
}
