import { useState } from 'react';
import { api } from '../api.ts';
import type { HoldState, Session } from '../types.ts';

interface HoldStateBannerProps {
  session: Pick<Session, 'session_id' | 'hold_state' | 'embargo_until'>;
  onChange?: () => void;
}

const LABELS: Record<HoldState, string> = {
  auto_redacted: 'Auto-redacted',
  pending_review: 'Pending review',
  released: 'Released',
  embargoed: 'Embargoed',
};

/**
 * Hold-state display + transition controls for a session.
 *
 * Surfaces the current state (with embargo expiry when relevant) and
 * exposes Release / Hold / Embargo actions that reach the extended
 * `update_session` endpoint. The banner never guesses at transitions
 * — the server validates and rejects past-dated embargoes with a
 * structured error, which we surface inline.
 */
export function HoldStateBanner({ session, onChange }: HoldStateBannerProps) {
  const current: HoldState = session.hold_state ?? 'auto_redacted';
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState<HoldState | null>(null);
  const [reason, setReason] = useState('');
  const [embargoUntil, setEmbargoUntil] = useState('');

  async function transition(target: HoldState, embargoUntilValue?: string) {
    setPending(target);
    setError(null);
    try {
      await api.sessions.update(session.session_id, {
        hold_state: target,
        reason: reason || undefined,
        embargo_until: embargoUntilValue,
      });
      onChange?.();
      setReason('');
      setEmbargoUntil('');
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      setPending(null);
    }
  }

  return (
    <div className={`hold-banner hold-banner--${current}`}>
      <div className="hold-banner__summary">
        <span className="hold-banner__label">Hold state:</span>
        <span className="hold-banner__value">{LABELS[current]}</span>
        {session.embargo_until && current === 'embargoed' && (
          <span className="hold-banner__embargo">
            until {new Date(session.embargo_until).toLocaleDateString()}
          </span>
        )}
      </div>

      {error && <div className="hold-banner__error">{error}</div>}

      <div className="hold-banner__controls">
        <input
          type="text"
          className="hold-banner__reason"
          placeholder="reason (optional)"
          value={reason}
          onChange={(e) => setReason(e.target.value)}
        />
        <button
          type="button"
          className="hold-banner__btn"
          disabled={pending !== null || current === 'pending_review'}
          onClick={() => transition('pending_review')}
        >
          Hold
        </button>
        <button
          type="button"
          className="hold-banner__btn hold-banner__btn--primary"
          disabled={pending !== null || current === 'released'}
          onClick={() => transition('released')}
        >
          Release
        </button>
        <input
          type="date"
          className="hold-banner__date"
          value={embargoUntil}
          onChange={(e) => setEmbargoUntil(e.target.value)}
        />
        <button
          type="button"
          className="hold-banner__btn"
          disabled={pending !== null || !embargoUntil}
          onClick={() => transition('embargoed', embargoUntil)}
        >
          Embargo
        </button>
      </div>
    </div>
  );
}
