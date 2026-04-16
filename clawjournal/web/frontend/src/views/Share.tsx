import { useState, useEffect, useCallback } from 'react';
import { Link, useSearchParams } from 'react-router-dom';
import type { Session, Share as ShareType, ToolUse } from '../types.ts';
import { api } from '../api.ts';
import { useToast } from '../components/Toast.tsx';
import { Spinner } from '../components/Spinner.tsx';
import { EmptyState } from '../components/Spinner.tsx';
import { ConfirmDialog } from '../components/ConfirmDialog.tsx';
import { RedactedText } from '../components/RedactedText.tsx';
import { RedactionReportPanel } from '../components/RedactionReportPanel.tsx';
import { SessionDrawer } from '../components/SessionDrawer.tsx';
import { ToolUseCard } from '../components/ToolUseCard.tsx';
import { ShareTabs } from '../components/ShareTabs.tsx';
import { Stepper } from '../components/Stepper.tsx';
import { TraceCard } from '../components/TraceCard.tsx';
import { colors } from '../theme.ts';

function hexAlpha(hex: string, alpha: number): string {
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  return `rgba(${r},${g},${b},${alpha})`;
}

function sourceFullLabel(s: { source: string; client_origin?: string | null; runtime_channel?: string | null }): { label: string; color: string } {
  if (s.source === 'codex') return (s as any).client_origin === 'desktop' ? { label: 'Codex Desktop', color: '#0891b2' } : { label: 'Codex', color: '#16a34a' };
  if (s.source === 'claude') {
    if ((s as any).client_origin === 'desktop' || (s as any).runtime_channel === 'local-agent') return { label: 'Claude Desktop', color: '#7c3aed' };
    return { label: 'Claude Code', color: '#d97706' };
  }
  if (s.source === 'openclaw') return { label: 'OpenClaw', color: '#6b7280' };
  return { label: s.source, color: '#6b7280' };
}

function SourceBadge({ s }: { s: { source: string; client_origin?: string | null; runtime_channel?: string | null } }) {
  const { label, color } = sourceFullLabel(s);
  return <span style={{ fontSize: 10, fontWeight: 600, padding: '1px 6px', borderRadius: 4, background: hexAlpha(color, 0.10), color, marginRight: 3 }}>{label}</span>;
}

function ThinkingBlock({ text }: { text: string }) {
  const [open, setOpen] = useState(false);
  return (
    <div style={{ margin: '6px 0' }}>
      <button
        onClick={() => setOpen(!open)}
        style={{
          background: 'none',
          border: 'none',
          color: colors.gray500,
          cursor: 'pointer',
          fontSize: 13,
          padding: 0,
          textDecoration: 'underline',
        }}
      >
        {open ? 'Hide thinking' : 'Show thinking'}
      </button>
      {open && (
        <pre
          style={{
            background: colors.yellow50,
            border: `1px solid ${colors.yellow200}`,
            borderRadius: 6,
            padding: 10,
            fontSize: 13,
            whiteSpace: 'pre-wrap',
            wordBreak: 'break-word',
            marginTop: 4,
            maxHeight: 300,
            overflow: 'auto',
          }}
        >
          {text}
        </pre>
      )}
    </div>
  );
}

// 5-step flow
type StepKey = 'preview' | 'redact' | 'review' | 'package' | 'download';

const STEPS = [
  { key: 'preview', label: 'Preview' },
  { key: 'redact', label: 'Redact' },
  { key: 'review', label: 'Review' },
  { key: 'package', label: 'Package' },
  { key: 'download', label: 'Download' },
];

interface ReadySession {
  session_id: string;
  project: string;
  model: string | null;
  source: string;
  display_title: string;
  ai_quality_score: number | null;
  user_messages: number;
  assistant_messages: number;
  tool_uses: number;
  input_tokens: number;
  outcome_badge: string | null;
  client_origin?: string | null;
  runtime_channel?: string | null;
  start_time?: string | null;
  review_status?: string;
}

interface ShareReadyStats {
  count: number;
  total_approved: number;
  projects: string[];
  models: string[];
  recommended_session_ids: string[];
  sessions: ReadySession[];
}

interface RedactedReviewMessage {
  role: 'user' | 'assistant' | 'system';
  content: string;
  thinking?: string;
  tool_uses?: ToolUse[];
  timestamp?: string;
}

interface AiPiiFindingLocal {
  entity_type: string;
  entity_text: string;
  confidence: number;
  field: string;
  source: string;
}

interface RedactedSessionData {
  messages: RedactedReviewMessage[];
  loading: boolean;
  redactionCount?: number;
  aiPiiFindings?: AiPiiFindingLocal[];
  aiCoverage?: 'full' | 'rules_only';
}

function autoDescription(share: ShareType): string {
  if (share.submission_note) return share.submission_note;
  if (share.sessions && share.sessions.length > 0) {
    const projects = [...new Set(share.sessions.map(s => s.project).filter(Boolean))].slice(0, 3);
    if (projects.length > 0) {
      return `${share.session_count} sessions from ${projects.join(', ')}`;
    }
  }
  return `${share.session_count} sessions`;
}

function formatDate(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}

function scoreBadge(score: number | null): string {
  if (score == null) return '';
  return '\u2605'.repeat(Math.min(score, 5));
}

function outcomeBadge(outcome: string | null): string {
  if (!outcome) return '';
  const b = outcome.toLowerCase();
  if (b === 'resolved') return '\u2713 resolved';
  if (b === 'partial') return '~ partial';
  if (b === 'failed') return '\u2717 failed';
  if (b === 'abandoned') return '\u2717 abandoned';
  if (b === 'exploratory') return '\u2014 exploratory';
  if (b === 'trivial') return '\u2014 trivial';
  if (b.includes('pass')) return '\u2713 passed';
  if (b.includes('fail')) return '\u2717 failed';
  if (b.includes('analysis')) return '\u2014 analysis';
  if (b.includes('completed')) return '\u2713 completed';
  if (b.includes('errored')) return '\u2717 errored';
  return '';
}

const formatTokens = (t: number) => t >= 1_000_000 ? `${(t / 1_000_000).toFixed(1)}M` : `${(t / 1000).toFixed(0)}k`;

export function Share() {
  const { toast } = useToast();
  const [searchParams, setSearchParams] = useSearchParams();
  const [activeStep, setActiveStep] = useState<StepKey>(
    () => (searchParams.get('step') as StepKey) || 'preview',
  );
  const [completedKeys, setCompletedKeys] = useState<Set<string>>(() => {
    const step = (searchParams.get('step') as StepKey) || 'preview';
    const idx = STEPS.findIndex((s) => s.key === step);
    return new Set(STEPS.slice(0, Math.max(0, idx)).map((s) => s.key));
  });
  const [readyStats, setReadyStats] = useState<ShareReadyStats | null>(null);
  const [shares, setShares] = useState<ShareType[]>([]);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(() => {
    const csv = searchParams.get('ids');
    return new Set(csv ? csv.split(',').filter(Boolean) : []);
  });
  const [selectionInitialized, setSelectionInitialized] = useState(
    () => !!searchParams.get('ids'),
  );
  const [note, setNote] = useState(() => searchParams.get('note') || '');
  const [drawerSessionId, setDrawerSessionId] = useState<string | null>(null);
  const [confirmReset, setConfirmReset] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [sourceFilter, setSourceFilter] = useState('');
  const [projectFilter, setProjectFilter] = useState('');
  const [scoreFilter, setScoreFilter] = useState(0);
  const [dateFilter, setDateFilter] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  // Redaction state
  const [redactedSessions, setRedactedSessions] = useState<Record<string, RedactedSessionData>>({});
  const [redactionProgress, setRedactionProgress] = useState<{ done: number; total: number; currentTitle: string }>({ done: 0, total: 0, currentTitle: '' });

  // Review state
  const [expandedSessionId, setExpandedSessionId] = useState<string | null>(null);
  const [showRedactionReport, setShowRedactionReport] = useState<string | null>(null);
  const [confirmPackage, setConfirmPackage] = useState(false);

  // Package / Download state
  const [packagedShareId, setPackagedShareId] = useState<string | null>(
    () => searchParams.get('share'),
  );
  const [packaging, setPackaging] = useState(false);
  const [uploading, setUploading] = useState(false);

  // Top candidates (when queue empty)
  const [candidates, setCandidates] = useState<Session[]>([]);
  const [scoringBackend, setScoringBackend] = useState<{ backend: string | null; display_name: string | null } | null>(null);

  useEffect(() => {
    Promise.all([
      api.shareReady({ includeUnapproved: true }),
      api.shares.list(),
      api.scoringBackend().catch(() => ({ backend: null, display_name: null })),
    ]).then(([stats, shareList, backend]) => {
      setReadyStats(stats);
      setShares(shareList);
      setScoringBackend(backend);
      if (!selectionInitialized && stats.sessions.length > 0) {
        const recommended = stats.recommended_session_ids?.length
          ? new Set(stats.recommended_session_ids)
          : new Set(stats.sessions.slice(0, 10).map((s: any) => s.session_id));
        setSelectedIds(recommended);
        setSelectionInitialized(true);
      }
      if (stats.sessions.length === 0) {
        api.sessions.list({ status: 'new', sort: 'ai_quality_score', order: 'desc', limit: 10 })
          .then(setCandidates)
          .catch(() => setCandidates([]));
      }
      setLoading(false);
    }).catch((e) => {
      toast(e instanceof Error ? e.message : 'Failed to load data', 'error');
      setLoading(false);
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Mirror wizard state to URL so reload / back-forward navigation resumes here.
  // `redactedSessions` stays in memory; on URL-restore the Redact step refills
  // any missing entries lazily.
  useEffect(() => {
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      if (activeStep === 'preview') next.delete('step'); else next.set('step', activeStep);
      const csv = Array.from(selectedIds).join(',');
      if (csv) next.set('ids', csv); else next.delete('ids');
      if (note) next.set('note', note); else next.delete('note');
      if (packagedShareId) next.set('share', packagedShareId); else next.delete('share');
      return next;
    }, { replace: true });
  }, [activeStep, selectedIds, note, packagedShareId, setSearchParams]);

  // When selection changes after redaction has run, drop cached redacted data
  // for sessions no longer selected so Review never shows stale entries.
  useEffect(() => {
    setRedactedSessions((prev) => {
      let changed = false;
      const next: Record<string, RedactedSessionData> = {};
      for (const [sid, data] of Object.entries(prev)) {
        if (selectedIds.has(sid)) next[sid] = data;
        else changed = true;
      }
      return changed ? next : prev;
    });
  }, [selectedIds]);

  const reload = () => {
    api.shareReady({ includeUnapproved: true }).then((stats) => {
      setReadyStats(stats);
      if (stats.sessions.length === 0) {
        api.sessions.list({ status: 'new', sort: 'ai_quality_score', order: 'desc', limit: 10 })
          .then(setCandidates)
          .catch(() => {});
      } else {
        setCandidates([]);
      }
    }).catch(() => {});
    api.shares.list().then(setShares).catch(() => {});
  };

  const selectedSessions = readyStats
    ? readyStats.sessions.filter(s => selectedIds.has(s.session_id))
    : [];

  const startFreshShare = useCallback(() => {
    setSelectedIds(new Set());
    setCompletedKeys(new Set());
    setPackagedShareId(null);
    setNote('');
    setRedactedSessions({});
    setActiveStep('preview');
  }, []);

  const onStepClick = (key: string) => {
    const k = key as StepKey;
    if (k === activeStep) return;
    if (k === 'preview') {
      if (activeStep === 'download') {
        // Confirm before clearing the just-finished share — accidental
        // backstep on Download wiped the previous selection silently.
        setConfirmReset(true);
        return;
      }
      setActiveStep('preview');
      return;
    }
    // Only allow jumping to completed later steps
    if (completedKeys.has(k)) setActiveStep(k);
  };

  // --- Run redaction on all selected sessions (parallel, 3 at a time) ---
  const REDACTION_CONCURRENCY = 3;

  const handlePrepareRedaction = useCallback(async () => {
    const sessions = selectedSessions;
    if (sessions.length === 0) return;

    setCompletedKeys((prev) => {
      const next = new Set(prev);
      next.add('preview');
      return next;
    });
    setActiveStep('redact');

    // Incremental: only redact sessions not already cached. Going back to
    // Preview, tweaking selection, and returning shouldn't re-run work for
    // unchanged sessions.
    const cached = redactedSessions;
    const missing = sessions.filter((s) => !cached[s.session_id]);
    if (missing.length === 0) {
      setCompletedKeys((prev) => {
        const next = new Set(prev);
        next.add('preview'); next.add('redact');
        return next;
      });
      setActiveStep('review');
      return;
    }
    setRedactionProgress({ done: 0, total: missing.length, currentTitle: '' });

    const results: Record<string, RedactedSessionData> = { ...cached };
    let doneCount = 0;

    const processOne = async (s: ReadySession) => {
      try {
        const report = await api.sessions.redactionReport(s.session_id, { aiPii: true });
        const msgs = (report.redacted_session.messages || []).map((m) => ({
          role: m.role,
          content: m.content || '',
          thinking: m.thinking,
          tool_uses: m.tool_uses,
          timestamp: m.timestamp,
        }));
        results[s.session_id] = {
          messages: msgs, loading: false,
          redactionCount: report.redaction_count,
          aiPiiFindings: report.ai_pii_findings || [],
          aiCoverage: report.ai_coverage || 'rules_only',
        };
      } catch {
        results[s.session_id] = { messages: [{ role: 'system', content: '(unable to load redacted content)' }], loading: false, redactionCount: 0, aiCoverage: 'rules_only' };
      }
      doneCount++;
      setRedactionProgress({ done: doneCount, total: missing.length, currentTitle: s.display_title || 'Untitled' });
      setRedactedSessions({ ...results });
    };

    for (let i = 0; i < missing.length; i += REDACTION_CONCURRENCY) {
      const batch = missing.slice(i, i + REDACTION_CONCURRENCY);
      await Promise.all(batch.map(processOne));
    }
    setRedactionProgress({ done: missing.length, total: missing.length, currentTitle: '' });
    setCompletedKeys((prev) => {
      const next = new Set(prev);
      next.add('preview');
      next.add('redact');
      return next;
    });
    setActiveStep('review');
  }, [selectedSessions, redactedSessions]);

  const handlePackage = async () => {
    setConfirmPackage(false);
    setPackaging(true);
    setError(null);
    setCompletedKeys((prev) => {
      const next = new Set(prev);
      next.add('preview');
      next.add('redact');
      next.add('review');
      return next;
    });
    setActiveStep('package');
    try {
      const ids = selectedSessions.map(s => s.session_id);
      // Auto-approve any unapproved sessions in the selection — the user
      // explicitly added them via the Preview list, so opt them into the
      // approved set before packaging. Failures are non-fatal: the share
      // create call is the source of truth.
      const needApproval = selectedSessions.filter((s) => s.review_status && s.review_status !== 'approved');
      if (needApproval.length > 0) {
        await Promise.all(
          needApproval.map((s) =>
            api.sessions.update(s.session_id, { status: 'approved' }).catch(() => undefined),
          ),
        );
      }
      const { share_id } = await api.shares.create(ids, note || undefined);
      setPackagedShareId(share_id);
      // Export on server so subsequent download/upload work.
      await api.shares.export(share_id);
      setCompletedKeys((prev) => {
        const next = new Set(prev);
        next.add('package');
        return next;
      });
      setActiveStep('download');
      reload();
      toast('Share packaged', 'success');
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : 'Package failed';
      setError(msg);
      toast(msg, 'error');
    } finally {
      setPackaging(false);
    }
  };

  const handleDownloadZip = async () => {
    if (!packagedShareId) return;
    try {
      await api.shares.download(packagedShareId);
      toast('Download started', 'success');
    } catch (err: unknown) {
      toast(err instanceof Error ? err.message : 'Download failed', 'error');
    }
  };

  const handleUploadHosted = async () => {
    if (!packagedShareId) return;
    setUploading(true);
    try {
      await api.shares.upload(packagedShareId, true);
      reload();
      toast('Uploaded to hosted ingest', 'success');
    } catch (err: unknown) {
      toast(err instanceof Error ? err.message : 'Upload failed', 'error');
    } finally {
      setUploading(false);
    }
  };

  const handleExportToDirectory = async () => {
    if (!packagedShareId) return;
    try {
      const res = await api.shares.export(packagedShareId);
      toast(`Exported to ${res.export_path}`, 'success');
    } catch (err: unknown) {
      toast(err instanceof Error ? err.message : 'Export failed', 'error');
    }
  };

  const btnPrimary = {
    padding: '10px 24px', background: colors.primary500, color: colors.white, border: 'none',
    borderRadius: '8px', fontSize: '15px', fontWeight: 600 as const, cursor: 'pointer' as const,
  };
  const btnSecondary = {
    padding: '8px 16px', background: colors.gray100, color: colors.gray700, border: `1px solid ${colors.gray300}`,
    borderRadius: '6px', fontSize: '14px', fontWeight: 500 as const, cursor: 'pointer' as const,
  };
  const cardBg = {
    background: colors.white, border: `1px solid ${colors.gray200}`, borderRadius: '12px',
    padding: '32px', textAlign: 'center' as const,
  };

  if (loading) {
    return <div style={{ padding: '24px', maxWidth: '720px', margin: '0 auto' }}>
      <Spinner text="Loading share data..." />
    </div>;
  }

  const stepperHeader = (
    <>
      <ShareTabs />
      <Stepper
        steps={STEPS}
        activeKey={activeStep}
        completedKeys={completedKeys}
        onStepClick={onStepClick}
      />
    </>
  );

  // =====================================================
  // STEP 1: PREVIEW
  // =====================================================
  if (activeStep === 'preview') {
    const historyShares = shares.filter(b => b.status === 'shared' || b.status === 'exported');
    const allSessions = readyStats?.sessions || [];
    const totalTokens = selectedSessions.reduce((sum, s) => sum + (s.input_tokens || 0), 0);
    const uniqueProjects = [...new Set(selectedSessions.map(s => s.project).filter(Boolean))];
    const estimatedBytes = totalTokens * 0.3;
    const estimatedMB = estimatedBytes / (1024 * 1024);
    const overLimit = estimatedMB > 500;

    const sources = [...new Set(allSessions.map(s => s.source).filter(Boolean))].sort();
    const projects = [...new Set(allSessions.map(s => s.project).filter(Boolean))].sort();

    const dateCutoffMs = dateFilter ? (() => {
      const days = dateFilter === '7d' ? 7 : dateFilter === '30d' ? 30 : 90;
      return Date.now() - days * 86_400_000;
    })() : null;

    const filteredSessions = allSessions.filter(s => {
      if (searchQuery && !(s.display_title || '').toLowerCase().includes(searchQuery.toLowerCase())
          && !(s.project || '').toLowerCase().includes(searchQuery.toLowerCase())) return false;
      if (sourceFilter && s.source !== sourceFilter) return false;
      if (projectFilter && s.project !== projectFilter) return false;
      if (scoreFilter > 0 && (s.ai_quality_score == null || s.ai_quality_score < scoreFilter)) return false;
      if (dateCutoffMs && (!s.start_time || new Date(s.start_time).getTime() < dateCutoffMs)) return false;
      return true;
    });

    const selectLatest10 = () => setSelectedIds(new Set(filteredSessions.slice(0, 10).map(s => s.session_id)));
    const selectAll = () => setSelectedIds(new Set(filteredSessions.map(s => s.session_id)));
    const clearSelection = () => setSelectedIds(new Set());
    const toggleSession = (id: string) => {
      setSelectedIds(prev => {
        const next = new Set(prev);
        if (next.has(id)) next.delete(id); else next.add(id);
        return next;
      });
    };

    return (
      <div style={{ padding: '24px', maxWidth: '960px', margin: '0 auto' }}>
        {stepperHeader}
        <h2 style={{ margin: '0 0 4px', fontSize: '20px', fontWeight: 600, color: colors.gray900 }}>Share</h2>
        <p style={{ margin: '0 0 16px', fontSize: '14px', color: colors.gray500 }}>
          Export and share selected sessions
        </p>
        {readyStats?.recommended_session_ids?.length ? (
          <p style={{ margin: '0 0 14px', fontSize: '13px', color: colors.gray500, lineHeight: 1.5 }}>
            Preselected: the 10 most recent approved traces that have not been shared before.
            Unapproved traces are also listed below — selecting one auto-approves it on package.
          </p>
        ) : null}

        {allSessions.length > 0 ? (
          <>
            <div style={{ display: 'flex', gap: 8, marginBottom: 10 }}>
              <input
                type="text"
                value={searchQuery}
                onChange={e => setSearchQuery(e.target.value)}
                placeholder="Search by title or project..."
                style={{
                  flex: 1, padding: '6px 10px', fontSize: 13,
                  border: `1px solid ${colors.gray300}`, borderRadius: 6,
                  outline: 'none',
                }}
              />
              {sources.length > 1 && (
                <select value={sourceFilter} onChange={e => setSourceFilter(e.target.value)}
                  style={{ padding: '6px 8px', fontSize: 12, border: `1px solid ${colors.gray300}`, borderRadius: 6, background: colors.white }}>
                  <option value="">All sources</option>
                  {sources.map(src => <option key={src} value={src}>{src}</option>)}
                </select>
              )}
              {projects.length > 1 && (
                <select value={projectFilter} onChange={e => setProjectFilter(e.target.value)}
                  style={{ padding: '6px 8px', fontSize: 12, border: `1px solid ${colors.gray300}`, borderRadius: 6, background: colors.white, maxWidth: 180 }}>
                  <option value="">All projects</option>
                  {projects.map(p => <option key={p} value={p}>{p}</option>)}
                </select>
              )}
              <select value={scoreFilter} onChange={e => setScoreFilter(Number(e.target.value))}
                style={{ padding: '6px 8px', fontSize: 12, border: `1px solid ${colors.gray300}`, borderRadius: 6, background: colors.white }}>
                <option value={0}>Any score</option>
                <option value={3}>{'\u2605'.repeat(3)}+ (3+)</option>
                <option value={4}>{'\u2605'.repeat(4)}+ (4+)</option>
                <option value={5}>{'\u2605'.repeat(5)} (5)</option>
              </select>
              <select value={dateFilter} onChange={e => setDateFilter(e.target.value)}
                style={{ padding: '6px 8px', fontSize: 12, border: `1px solid ${colors.gray300}`, borderRadius: 6, background: colors.white }}>
                <option value="">Any date</option>
                <option value="7d">Last 7 days</option>
                <option value="30d">Last 30 days</option>
                <option value="90d">Last 90 days</option>
              </select>
            </div>

            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
              <span style={{ fontSize: 13, color: colors.gray500 }}>
                {filteredSessions.length === allSessions.length
                  ? `${allSessions.length} sessions`
                  : `${filteredSessions.length} of ${allSessions.length} sessions`}
              </span>
              <div style={{ display: 'flex', gap: 6 }}>
                <button onClick={selectLatest10} style={{ ...btnSecondary, fontSize: 11, padding: '2px 8px' }}>Latest 10</button>
                <button onClick={selectAll} style={{ ...btnSecondary, fontSize: 11, padding: '2px 8px' }}>Select all</button>
                <button onClick={clearSelection} style={{ ...btnSecondary, fontSize: 11, padding: '2px 8px' }}>Clear</button>
              </div>
            </div>

            <div style={{ border: `1px solid ${colors.gray200}`, borderRadius: 8, overflow: 'hidden', marginBottom: 16, maxHeight: '40vh', overflowY: 'auto' }}>
              {filteredSessions.map((s, i) => {
                const checked = selectedIds.has(s.session_id);
                return (
                  <div key={s.session_id} style={{
                    display: 'flex', alignItems: 'center', gap: 8,
                    padding: '8px 12px', cursor: 'pointer',
                    borderBottom: i < filteredSessions.length - 1 ? `1px solid ${colors.gray100}` : 'none',
                    background: checked ? colors.white : colors.gray50,
                    opacity: checked ? 1 : 0.6,
                  }} onClick={() => toggleSession(s.session_id)}>
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={() => toggleSession(s.session_id)}
                      onClick={e => e.stopPropagation()}
                      style={{ cursor: 'pointer', flexShrink: 0 }}
                    />
                    <span style={{ fontSize: 13, color: '#fbbf24', minWidth: 36, letterSpacing: -1 }}>
                      {scoreBadge(s.ai_quality_score)}
                    </span>
                    <SourceBadge s={s} />
                    {s.review_status && s.review_status !== 'approved' && (
                      <span
                        title="Not yet approved — will be auto-approved when packaged"
                        style={{
                          fontSize: 10, fontWeight: 600, padding: '1px 6px', borderRadius: 4,
                          background: hexAlpha('#9ca3af', 0.18), color: '#4b5563',
                          marginRight: 3, flexShrink: 0,
                        }}
                      >
                        {s.review_status}
                      </span>
                    )}
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ fontSize: 13, fontWeight: 500, color: colors.gray900, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {s.display_title || 'Untitled'}
                      </div>
                      <div style={{ fontSize: 11, color: colors.gray400 }}>
                        {s.project} &middot; {s.user_messages + s.assistant_messages} msgs
                        {s.tool_uses ? ` · ${s.tool_uses} tools` : ''}
                        {' · '}{formatTokens(s.input_tokens)} tokens
                        {s.outcome_badge ? ` · ${outcomeBadge(s.outcome_badge)}` : ''}
                      </div>
                    </div>
                    <button
                      onClick={(e) => { e.stopPropagation(); setDrawerSessionId(s.session_id); }}
                      title="Preview this trace without changing your selection"
                      style={{
                        flexShrink: 0, padding: '3px 10px', fontSize: 11,
                        background: colors.white, color: colors.gray600,
                        border: `1px solid ${colors.gray300}`, borderRadius: 4,
                        cursor: 'pointer',
                      }}
                    >
                      Preview
                    </button>
                  </div>
                );
              })}
              {filteredSessions.length === 0 && (
                <div style={{ padding: '16px', textAlign: 'center', fontSize: 13, color: colors.gray400 }}>
                  No sessions match your filters.
                </div>
              )}
            </div>

            <div style={{ marginBottom: 16 }}>
              <label style={{ fontSize: 13, fontWeight: 500, color: colors.gray600 }}>Note (optional)</label>
              <input
                type="text" value={note} onChange={e => setNote(e.target.value)}
                placeholder="e.g. Week 14 patent translation traces"
                style={{
                  display: 'block', width: '100%', padding: '7px 10px', marginTop: 4,
                  border: `1px solid ${colors.gray300}`, borderRadius: 6, fontSize: 13, boxSizing: 'border-box',
                }}
              />
            </div>

            <div style={{
              ...cardBg, padding: '12px 16px',
              display: 'flex', justifyContent: 'space-between', alignItems: 'center',
              position: 'sticky', bottom: 0, marginBottom: 24,
              borderTop: `1px solid ${colors.gray200}`,
            }}>
              <div style={{ fontSize: 13, color: colors.gray600, textAlign: 'left' }}>
                <strong>{selectedSessions.length}</strong> selected
                {selectedSessions.length > 0 && (
                  <span style={{ color: colors.gray400 }}>
                    {' · '}{uniqueProjects.length} project{uniqueProjects.length !== 1 ? 's' : ''}
                    {' · '}{formatTokens(totalTokens)} tokens
                    {estimatedMB >= 0.1 && (
                      <span style={{ color: overLimit ? colors.red500 : undefined }}>
                        {' · ~'}{estimatedMB.toFixed(1)} MB
                        {overLimit && ' (over 500 MB limit)'}
                      </span>
                    )}
                  </span>
                )}
              </div>
              <div style={{ display: 'flex', gap: 8 }}>
                <button
                  onClick={handlePrepareRedaction}
                  disabled={selectedSessions.length === 0}
                  style={{ ...btnPrimary, opacity: selectedSessions.length === 0 ? 0.4 : 1 }}
                >
                  Redact selected traces {'→'}
                </button>
              </div>
            </div>
          </>
        ) : candidates.length > 0 ? (
          <>
            <div style={{ marginBottom: 12 }}>
              <h3 style={{ margin: '0 0 4px', fontSize: 16, fontWeight: 600, color: colors.gray900 }}>
                Top traces to review
              </h3>
              <p
                style={{ margin: 0, fontSize: 12, color: colors.gray500 }}
                title="ClawJournal uses your current coding agent as the default scorer."
              >
                {scoringBackend?.display_name
                  ? `Scored by ${scoringBackend.display_name}`
                  : 'Scored by your configured agent'}
              </p>
            </div>
            <div style={{ border: `1px solid ${colors.gray200}`, borderRadius: 8, overflow: 'hidden', marginBottom: 24 }}>
              {candidates.map((s) => (
                <TraceCard
                  key={s.session_id}
                  session={s}
                  showSelection={false}
                  showQuickActions={true}
                  quickActionMode="share"
                  onStatusChange={(newStatus) => {
                    if (newStatus === 'approved') {
                      setSelectedIds((prev) => {
                        const next = new Set(prev);
                        next.add(s.session_id);
                        return next;
                      });
                    }
                    reload();
                  }}
                />
              ))}
            </div>
          </>
        ) : (
          <div style={{ ...cardBg, marginBottom: 32 }}>
            <EmptyState
              title="No traces ready to share"
              description="Go to Sessions to approve traces for sharing."
              action={<Link to="/" style={{ color: colors.primary500, fontWeight: 600, textDecoration: 'none' }}>Go to Sessions</Link>}
            />
          </div>
        )}

        {historyShares.length > 0 && (
          <div>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
              <h3 style={{ margin: 0, fontSize: '16px', fontWeight: 600, color: colors.gray900 }}>Recent Sharing History</h3>
              <span style={{ fontSize: '13px', color: colors.gray500 }}>
                {historyShares.filter(b => b.status === 'shared').length} shared total
              </span>
            </div>
            {historyShares.slice(0, 4).map(share => (
              <div key={share.share_id} style={{
                background: colors.white, border: `1px solid ${colors.gray200}`, borderRadius: '8px',
                padding: '14px 16px', marginBottom: '8px',
              }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                  <div style={{ flex: 1 }}>
                    <div style={{ fontSize: '14px', fontWeight: 500, color: colors.gray900 }}>{autoDescription(share)}</div>
                    <div style={{ fontSize: '13px', color: colors.gray500, marginTop: '2px' }}>
                      {formatDate(share.created_at)} &middot; {share.session_count} sessions &middot;{' '}
                      {share.status === 'shared'
                        ? <span style={{ color: colors.green500 }}>Shared &#x2713;</span>
                        : <span style={{ color: colors.gray500 }}>Saved locally</span>}
                    </div>
                  </div>
                  <div style={{ display: 'flex', gap: '6px' }}>
                    <button
                      onClick={async () => {
                        try {
                          await api.shares.download(share.share_id);
                          toast('Download started', 'success');
                        } catch (err: unknown) {
                          toast(err instanceof Error ? err.message : 'Download failed', 'error');
                        }
                      }}
                      style={btnSecondary}
                    >
                      Download zip
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
        <SessionDrawer
          sessionId={drawerSessionId}
          onClose={() => setDrawerSessionId(null)}
        />
      </div>
    );
  }

  // =====================================================
  // STEP 2: REDACT
  // =====================================================
  if (activeStep === 'redact') {
    const pct = redactionProgress.total > 0 ? (redactionProgress.done / redactionProgress.total) * 100 : 0;
    return (
      <div style={{ padding: '24px', maxWidth: '960px', margin: '0 auto' }}>
        {stepperHeader}
        <div style={{
          background: colors.white, border: `2px solid ${colors.primary200}`, borderRadius: '12px',
          overflow: 'hidden',
        }}>
          <div style={{
            background: `linear-gradient(135deg, ${colors.primary500}, ${colors.blue500})`,
            padding: '20px 32px',
            textAlign: 'center',
          }}>
            <div style={{ fontSize: '28px', marginBottom: '8px' }}>&#x1F6E1;</div>
            <h3 style={{ margin: '0 0 4px', fontSize: '18px', fontWeight: 700, color: colors.white }}>
              Protecting your data
            </h3>
            <p style={{ margin: 0, fontSize: '14px', color: 'rgba(255,255,255,0.85)' }}>
              Redacting {redactionProgress.total} trace{redactionProgress.total !== 1 ? 's' : ''}...
            </p>
          </div>

          <div style={{ padding: '24px 32px', textAlign: 'center' }}>
            <div style={{
              width: '100%', maxWidth: '400px', height: '10px', background: colors.gray100,
              borderRadius: '5px', margin: '0 auto 8px', overflow: 'hidden',
            }}>
              <div style={{
                height: '100%', borderRadius: '5px',
                background: `linear-gradient(90deg, ${colors.primary500}, ${colors.blue500})`,
                width: `${Math.max(pct, 5)}%`,
                transition: 'width 0.3s ease',
              }} />
            </div>

            <p style={{ margin: '0 0 20px', fontSize: '14px', fontWeight: 600, color: colors.primary500 }}>
              {redactionProgress.done} of {redactionProgress.total} complete
            </p>

            <div style={{
              textAlign: 'left', maxWidth: '400px', margin: '0 auto 20px',
              background: colors.gray50, borderRadius: 8, padding: '12px 16px',
            }}>
              {selectedSessions.map((s, i) => {
                const done = redactedSessions[s.session_id] && !redactedSessions[s.session_id].loading;
                const isCurrent = i === redactionProgress.done && redactionProgress.done < redactionProgress.total;
                return (
                  <div key={s.session_id} style={{
                    display: 'flex', alignItems: 'center', gap: 8,
                    fontSize: 13, padding: '5px 0',
                    color: done ? colors.green700 : isCurrent ? colors.gray900 : colors.gray400,
                    fontWeight: isCurrent ? 600 : 400,
                  }}>
                    {done ? (
                      <span style={{
                        display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
                        width: 20, height: 20, borderRadius: '50%',
                        background: colors.green100, color: colors.green500, fontSize: 12, fontWeight: 700,
                      }}>&#x2713;</span>
                    ) : isCurrent ? (
                      <span style={{
                        display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
                        width: 20, height: 20, borderRadius: '50%',
                        background: colors.primary100, color: colors.primary500, fontSize: 12,
                        animation: 'spin 1s linear infinite',
                      }}>&#x21BB;</span>
                    ) : (
                      <span style={{
                        display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
                        width: 20, height: 20, borderRadius: '50%',
                        background: colors.gray200, color: colors.gray400, fontSize: 10,
                      }}>&#x2022;</span>
                    )}
                    <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {s.display_title || 'Untitled'}
                    </span>
                  </div>
                );
              })}
            </div>

            <button onClick={() => setActiveStep('preview')} style={btnSecondary}>Cancel</button>
          </div>
        </div>
        <style>{`@keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }`}</style>
      </div>
    );
  }

  // =====================================================
  // STEP 3: REVIEW
  // =====================================================
  if (activeStep === 'review') {
    const totalRedactions = Object.values(redactedSessions).reduce((sum, s) => sum + (s.redactionCount || 0), 0);
    const rulesOnlyCount = Object.values(redactedSessions).filter(s => !s.loading && s.aiCoverage === 'rules_only').length;
    const totalTokens = selectedSessions.reduce((sum, s) => sum + (s.input_tokens || 0), 0);
    const estimatedMB = (totalTokens * 0.3) / (1024 * 1024);

    const activeSessionId = expandedSessionId && selectedIds.has(expandedSessionId) ? expandedSessionId : selectedSessions[0]?.session_id || null;
    const activeData = activeSessionId ? redactedSessions[activeSessionId] : null;
    const activeSession = selectedSessions.find(s => s.session_id === activeSessionId);
    const activeRedactionCount = activeData?.redactionCount || 0;

    const nextWithFindings = (() => {
      if (!activeSessionId) return null;
      const currentIdx = selectedSessions.findIndex(s => s.session_id === activeSessionId);
      for (let i = 1; i < selectedSessions.length; i++) {
        const idx = (currentIdx + i) % selectedSessions.length;
        const s = selectedSessions[idx];
        const d = redactedSessions[s.session_id];
        if (d && (d.redactionCount || 0) > 0) return s.session_id;
      }
      return null;
    })();

    return (
      <div style={{ display: 'flex', flexDirection: 'column', height: '100vh' }}>
        <div style={{ padding: '16px 20px 0' }}>
          {stepperHeader}
        </div>
        <div style={{
          display: 'flex', justifyContent: 'space-between', alignItems: 'center',
          padding: '10px 16px', borderBottom: `1px solid ${colors.gray200}`, background: colors.white,
          flexShrink: 0,
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
            <h2 style={{ margin: 0, fontSize: '16px', fontWeight: 600, color: colors.gray900 }}>
              Review redacted export
            </h2>
            <div style={{ display: 'flex', gap: 12, fontSize: 12, color: colors.gray500 }}>
              <span><strong>{selectedSessions.length}</strong> traces</span>
              <span><strong>{totalRedactions}</strong> redacted</span>
              <span>~{estimatedMB.toFixed(1)} MB</span>
            </div>
          </div>
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            <span style={{ fontSize: 11, color: colors.gray400 }}>Nothing leaves your machine until you package.</span>
            <button
              onClick={() => setConfirmPackage(true)}
              disabled={selectedSessions.length === 0}
              style={{ ...btnPrimary, fontSize: 13, padding: '8px 18px', opacity: selectedSessions.length === 0 ? 0.5 : 1 }}
            >
              Looks good &mdash; package {'→'}
            </button>
          </div>
        </div>

        {rulesOnlyCount > 0 && (
          <div style={{
            padding: '8px 16px', fontSize: 13,
            background: '#fffbeb', borderBottom: '1px solid #fde68a', color: '#92400e',
          }}>
            <strong>{rulesOnlyCount} of {selectedSessions.length}</strong> trace{rulesOnlyCount > 1 ? 's' : ''} used rule-based redaction only (AI detection unavailable). These may have lower PII coverage.
          </div>
        )}

        <div style={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
          <div style={{
            width: 220, flexShrink: 0, borderRight: `1px solid ${colors.gray200}`,
            overflowY: 'auto', background: colors.gray50,
          }}>
            <div style={{ padding: '8px 0' }}>
              {selectedSessions.map(s => {
                const data = redactedSessions[s.session_id];
                const count = data?.redactionCount || 0;
                const isActive = s.session_id === activeSessionId;
                return (
                  <div
                    key={s.session_id}
                    onClick={() => { setExpandedSessionId(s.session_id); setShowRedactionReport(null); }}
                    style={{
                      padding: '8px 12px', cursor: 'pointer',
                      background: isActive ? colors.white : 'transparent',
                      borderLeft: isActive ? `3px solid ${colors.primary500}` : '3px solid transparent',
                      borderBottom: `1px solid ${colors.gray100}`,
                    }}
                  >
                    <div style={{
                      fontSize: 13, fontWeight: isActive ? 600 : 400,
                      color: isActive ? colors.gray900 : colors.gray600,
                      overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                    }}>
                      {s.display_title || 'Untitled'}
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginTop: 2 }}>
                      <SourceBadge s={s} />
                      <span style={{
                        fontSize: 11, fontWeight: 600, padding: '1px 6px', borderRadius: 8,
                        background: count > 0 ? colors.yellow100 : colors.green50,
                        color: count > 0 ? colors.yellow700 : colors.green500,
                      }}>
                        {count > 0 ? `\u26A0 ${count}` : '\u2713 0'}
                      </span>
                      {data && !data.loading && (
                        <span style={{
                          fontSize: 10, fontWeight: 600, padding: '1px 5px', borderRadius: 8,
                          background: data.aiCoverage === 'full' ? colors.green50 : '#fffbeb',
                          color: data.aiCoverage === 'full' ? colors.green500 : '#b45309',
                        }}>
                          {data.aiCoverage === 'full' ? 'AI+Rules' : 'Rules only'}
                        </span>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          <div style={{ flex: 1, overflowY: 'auto', padding: '16px 20px' }}>
            {selectedSessions.length === 0 ? (
              <div style={{ padding: '60px 20px', textAlign: 'center' }}>
                <div style={{ fontSize: 32, marginBottom: 8 }}>&#x2205;</div>
                <h3 style={{ margin: '0 0 8px', fontSize: 16, fontWeight: 600, color: colors.gray700 }}>
                  All traces excluded
                </h3>
                <p style={{ margin: '0 0 16px', fontSize: 13, color: colors.gray400 }}>
                  Go back to re-select traces for sharing.
                </p>
                <button onClick={() => { setExpandedSessionId(null); setActiveStep('preview'); }} style={btnSecondary}>
                  {'← Back to selection'}
                </button>
              </div>
            ) : activeSession && activeData ? (
              <>
                <div style={{ marginBottom: 12 }}>
                  <h3 style={{ margin: '0 0 4px', fontSize: 16, fontWeight: 600, color: colors.gray900 }}>
                    {activeSession.display_title || 'Untitled'}
                  </h3>
                  <div style={{ fontSize: 12, color: colors.gray400 }}>
                    <SourceBadge s={activeSession} />
                    {activeSession.project}
                    {activeSession.ai_quality_score ? ` · ${scoreBadge(activeSession.ai_quality_score)}` : ''}
                    {' · '}{activeSession.user_messages + activeSession.assistant_messages} msgs
                    {activeSession.tool_uses ? ` · ${activeSession.tool_uses} tools` : ''}
                    {' · '}{formatTokens(activeSession.input_tokens)} tokens
                  </div>
                </div>

                <div style={{
                  display: 'flex', alignItems: 'center', gap: 8,
                  padding: '6px 10px', marginBottom: 12, borderRadius: 6,
                  background: colors.green50, border: `1px solid ${colors.green200}`,
                  fontSize: 12, color: colors.green700,
                }}>
                  <span style={{ fontWeight: 700 }}>Redacted preview</span>
                  <span>&mdash; Secrets, API keys, and PII replaced with <span style={{
                    background: colors.red100, color: colors.red700, borderRadius: 3,
                    padding: '0 4px', fontWeight: 600, fontSize: 11,
                  }}>[REDACTED]</span></span>
                </div>

                {activeData.messages.length === 0 ? (
                  <div style={{ color: colors.gray400, padding: '8px 0' }}>No message content available.</div>
                ) : activeData.messages.map((msg, i) => (
                  <div key={i} style={{
                    padding: '8px 0', borderBottom: `1px solid ${colors.gray100}`,
                  }}>
                    <div style={{
                      fontWeight: 600, fontSize: '11px', textTransform: 'uppercase',
                      color: msg.role === 'user' ? colors.blue500 : colors.primary500,
                      marginBottom: '3px',
                    }}>
                      {msg.role} #{i + 1}
                    </div>
                    <div style={{
                      whiteSpace: 'pre-wrap', wordBreak: 'break-word',
                      fontSize: '13px', lineHeight: '1.6', color: colors.gray700,
                    }}>
                      <RedactedText text={msg.content} />
                    </div>
                    {msg.thinking && <ThinkingBlock text={msg.thinking} />}
                    {msg.tool_uses && msg.tool_uses.length > 0 && (
                      <div style={{ marginTop: '8px' }}>
                        {msg.tool_uses.map((t, ti) => (
                          <ToolUseCard key={ti} tu={t} />
                        ))}
                      </div>
                    )}
                  </div>
                ))}
              </>
            ) : (
              <div style={{ padding: '40px 20px', textAlign: 'center', color: colors.gray400, fontSize: 14 }}>
                Select a trace from the left panel to view its redacted content.
              </div>
            )}
          </div>

          <div style={{
            width: 220, flexShrink: 0, borderLeft: `1px solid ${colors.gray200}`,
            overflowY: 'auto', background: colors.white, padding: '16px 12px',
          }}>
            {activeSession && activeData ? (
              <>
                <div style={{ marginBottom: 16 }}>
                  <div style={{
                    fontSize: 11, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.04em',
                    color: colors.gray400, marginBottom: 8,
                  }}>
                    Findings ({activeRedactionCount})
                  </div>
                  {activeRedactionCount === 0 ? (
                    <div style={{
                      padding: '12px', borderRadius: 6,
                      background: colors.green50, fontSize: 12, color: colors.green700, textAlign: 'center',
                    }}>
                      &#x2713; No sensitive data detected
                    </div>
                  ) : (
                    <div style={{
                      padding: '8px', borderRadius: 6,
                      background: colors.yellow50, border: `1px solid ${colors.yellow200}`,
                    }}>
                      <div style={{ fontSize: 12, color: colors.yellow700, marginBottom: 6 }}>
                        {activeRedactionCount} item{activeRedactionCount !== 1 ? 's' : ''} redacted in this trace
                      </div>
                      <button
                        onClick={() => setShowRedactionReport(showRedactionReport === activeSessionId ? null : activeSessionId)}
                        style={{
                          background: colors.white, border: `1px solid ${colors.gray300}`,
                          borderRadius: 4, padding: '4px 10px', fontSize: 11, fontWeight: 600,
                          cursor: 'pointer', color: colors.gray700, width: '100%',
                        }}
                      >
                        {showRedactionReport === activeSessionId ? 'Hide details' : 'View details'}
                      </button>
                    </div>
                  )}

                  {showRedactionReport === activeSessionId && (
                    <div style={{ marginTop: 8 }}>
                      <RedactionReportPanel sessionId={activeSessionId!} />
                    </div>
                  )}
                </div>

                {activeData.aiPiiFindings && activeData.aiPiiFindings.length > 0 && (
                  <div style={{ marginBottom: 16 }}>
                    <div style={{
                      fontSize: 11, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.04em',
                      color: colors.primary500, marginBottom: 8,
                    }}>
                      AI PII Detection ({activeData.aiPiiFindings.length})
                    </div>
                    <div style={{
                      padding: '8px', borderRadius: 6,
                      background: colors.primary50, border: `1px solid ${colors.primary200}`,
                    }}>
                      {activeData.aiPiiFindings.map((f, i) => (
                        <div key={i} style={{
                          fontSize: 11, padding: '3px 0',
                          borderBottom: i < activeData.aiPiiFindings!.length - 1 ? `1px solid ${colors.gray200}` : 'none',
                          color: colors.gray700,
                        }}>
                          <span style={{
                            display: 'inline-block', padding: '1px 4px', borderRadius: 3,
                            background: colors.primary100, color: colors.primary500,
                            fontSize: 10, fontWeight: 600, marginRight: 4,
                          }}>
                            {f.entity_type.replace(/_/g, ' ')}
                          </span>
                          <span style={{ fontFamily: 'monospace', fontSize: 10 }}>
                            {f.confidence >= 0.85 ? '\u25CF' : f.confidence >= 0.5 ? '\u25D0' : '\u25CB'}
                          </span>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                <div style={{ marginBottom: 16 }}>
                  <div style={{
                    fontSize: 11, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.04em',
                    color: colors.gray400, marginBottom: 8,
                  }}>
                    Actions
                  </div>
                  <button
                    onClick={() => {
                      const next = new Set(selectedIds);
                      next.delete(activeSessionId!);
                      setSelectedIds(next);
                      const idx = selectedSessions.findIndex(s => s.session_id === activeSessionId);
                      const nextSession = selectedSessions[idx + 1] || selectedSessions[idx - 1];
                      setExpandedSessionId(nextSession?.session_id || null);
                    }}
                    style={{
                      ...btnSecondary, width: '100%', marginBottom: 6, fontSize: 12,
                      background: colors.red50, color: colors.red700, borderColor: colors.red200,
                    }}
                  >
                    Exclude this trace
                  </button>
                  {nextWithFindings && (
                    <button
                      onClick={() => { setExpandedSessionId(nextWithFindings); setShowRedactionReport(null); }}
                      style={{ ...btnSecondary, width: '100%', marginBottom: 6, fontSize: 12 }}
                    >
                      Next with findings {'→'}
                    </button>
                  )}
                </div>

                <div style={{
                  background: colors.green50, borderRadius: 6, padding: '10px',
                }}>
                  <div style={{ fontSize: 11, fontWeight: 600, color: colors.green700, marginBottom: 4 }}>Protected:</div>
                  {['API keys & tokens', 'Names & emails', 'File paths & timestamps'].map(item => (
                    <div key={item} style={{ fontSize: 11, color: colors.green700, marginBottom: 1 }}>&#x2713; {item}</div>
                  ))}
                </div>
              </>
            ) : (
              <div style={{ color: colors.gray400, fontSize: 12 }}>No trace selected</div>
            )}
          </div>
        </div>

        <ConfirmDialog
          open={confirmPackage}
          title="Package share?"
          message={`This will create a share record for ${selectedSessions.length} trace${selectedSessions.length > 1 ? 's' : ''}.\n\n${totalRedactions} items were redacted.${rulesOnlyCount > 0 ? ` ${rulesOnlyCount} trace${rulesOnlyCount > 1 ? 's' : ''} could not use AI-based PII detection \u2014 review these before continuing.` : ' All sensitive data has been removed.'}${note ? `\n\nNote: "${note}"` : ''}`}
          confirmLabel="Package"
          onConfirm={handlePackage}
          onCancel={() => setConfirmPackage(false)}
        />
      </div>
    );
  }

  // =====================================================
  // STEP 4: PACKAGE
  // =====================================================
  if (activeStep === 'package') {
    return (
      <div style={{ padding: '24px', maxWidth: '720px', margin: '0 auto' }}>
        {stepperHeader}
        <div style={cardBg}>
          <h3 style={{ margin: '0 0 16px', fontSize: '18px', fontWeight: 600, color: colors.gray900 }}>
            Packaging your share...
          </h3>
          {packaging ? (
            <Spinner text="Creating share record..." />
          ) : (
            <p style={{ fontSize: 13, color: colors.gray500 }}>
              {error || 'Preparing package.'}
            </p>
          )}
          <div style={{ marginTop: 16 }}>
            <button onClick={() => setActiveStep('review')} style={btnSecondary}>Back to review</button>
          </div>
        </div>
      </div>
    );
  }

  // =====================================================
  // STEP 5: DOWNLOAD
  // =====================================================
  if (activeStep === 'download') {
    // Hosted ingest may be disabled server-side; we always show the button
    // and rely on the server to return an error if unavailable.
    const showHosted = true;
    return (
      <div style={{ padding: '24px', maxWidth: '720px', margin: '0 auto' }}>
        {stepperHeader}
        <div style={cardBg}>
          <div style={{ fontSize: '40px', marginBottom: '8px' }}>&#x2713;</div>
          <h3 style={{ margin: '0 0 8px', fontSize: '18px', fontWeight: 600, color: colors.green500 }}>
            Share packaged
          </h3>
          <p style={{ margin: '0 0 20px', fontSize: '14px', color: colors.gray700 }}>
            {selectedSessions.length} redacted trace{selectedSessions.length !== 1 ? 's' : ''} ready to distribute.
          </p>

          <div style={{ display: 'flex', flexDirection: 'column', gap: 10, alignItems: 'center', marginBottom: 20 }}>
            <button onClick={handleDownloadZip} style={{ ...btnPrimary, minWidth: 220 }}>
              Download zip
            </button>
            {showHosted && (
              <button
                onClick={handleUploadHosted}
                disabled={uploading}
                style={{ ...btnSecondary, minWidth: 220, opacity: uploading ? 0.5 : 1 }}
              >
                {uploading ? 'Uploading...' : 'Upload to hosted ingest'}
              </button>
            )}
            <button onClick={handleExportToDirectory} style={{ ...btnSecondary, minWidth: 220 }}>
              Export to directory
            </button>
          </div>

          <div>
            <button
              onClick={() => { startFreshShare(); reload(); }}
              style={btnSecondary}
            >
              Start a new share
            </button>
          </div>
        </div>
        <ConfirmDialog
          open={confirmReset}
          title="Start a new share?"
          message="Going back to Preview from here clears the selection, note, and packaged share. Continue?"
          confirmLabel="Start fresh"
          onConfirm={() => { setConfirmReset(false); startFreshShare(); reload(); }}
          onCancel={() => setConfirmReset(false)}
        />
      </div>
    );
  }

  return null;
}
