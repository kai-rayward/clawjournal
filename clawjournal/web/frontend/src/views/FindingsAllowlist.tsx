import { useCallback, useEffect, useState } from 'react';
import { api } from '../api.ts';
import type { FindingsAllowlistEntry } from '../types.ts';

/**
 * Management view for the cross-session findings allowlist (tier 3).
 *
 * Adding an entry hashes the plaintext locally on the daemon and flips
 * every matching `open` finding to `ignored` in the same transaction.
 * Removing an entry reassigns or reverts its decisions symmetrically.
 * The `entity_hash` is never shown — users identify entries by label,
 * type, and reason (all non-sensitive metadata).
 */
export function FindingsAllowlist() {
  const [entries, setEntries] = useState<FindingsAllowlistEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const [entityText, setEntityText] = useState('');
  const [entityType, setEntityType] = useState('');
  const [entityLabel, setEntityLabel] = useState('');
  const [reason, setReason] = useState('');

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await api.findings.allowlist.list();
      setEntries(res.entries);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  async function add(e: React.FormEvent) {
    e.preventDefault();
    if (!entityText.trim()) return;
    setSubmitting(true);
    setError(null);
    try {
      await api.findings.allowlist.add({
        entity_text: entityText,
        entity_type: entityType || null,
        entity_label: entityLabel || null,
        reason: reason || null,
      });
      setEntityText('');
      setEntityType('');
      setEntityLabel('');
      setReason('');
      await refresh();
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      setSubmitting(false);
    }
  }

  async function remove(id: string) {
    setError(null);
    try {
      await api.findings.allowlist.remove(id);
      await refresh();
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    }
  }

  return (
    <div className="findings-allowlist">
      <h2>Findings allowlist</h2>
      <p className="findings-allowlist__help">
        Entities added here are hashed locally and auto-ignored in every session
        on scan. Removing an entry reassigns or reverts its decisions in the same
        transaction.
      </p>

      <form className="findings-allowlist__form" onSubmit={add}>
        <input
          type="text"
          placeholder="entity text (will be hashed, not stored)"
          value={entityText}
          onChange={(e) => setEntityText(e.target.value)}
          required
        />
        <input
          type="text"
          placeholder="entity_type (e.g. email; blank = any)"
          value={entityType}
          onChange={(e) => setEntityType(e.target.value)}
        />
        <input
          type="text"
          placeholder="label (non-sensitive mnemonic)"
          value={entityLabel}
          onChange={(e) => setEntityLabel(e.target.value)}
        />
        <input
          type="text"
          placeholder="reason (optional)"
          value={reason}
          onChange={(e) => setReason(e.target.value)}
        />
        <button type="submit" disabled={submitting || !entityText.trim()}>
          Add
        </button>
      </form>

      {error && <div className="findings-allowlist__error">{error}</div>}

      {loading ? (
        <div>Loading…</div>
      ) : entries.length === 0 ? (
        <div className="findings-allowlist__empty">
          No entries yet. Add one above to auto-ignore a specific entity across
          every session.
        </div>
      ) : (
        <table className="findings-allowlist__table">
          <thead>
            <tr>
              <th>Label</th>
              <th>Type</th>
              <th>Scope</th>
              <th>Reason</th>
              <th>Added</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {entries.map((entry) => (
              <tr key={entry.allowlist_id}>
                <td>{entry.entity_label || <em>—</em>}</td>
                <td>{entry.entity_type || <em>any</em>}</td>
                <td>{entry.scope}</td>
                <td>{entry.reason || <em>—</em>}</td>
                <td>{new Date(entry.added_at).toLocaleString()}</td>
                <td>
                  <button type="button" onClick={() => remove(entry.allowlist_id)}>
                    Remove
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
