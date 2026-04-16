import { useState } from 'react';
import type { ToolUse } from '../types.ts';
import { colors } from '../theme.ts';
import { RedactedText } from './RedactedText.tsx';

/**
 * Collapsible card for a single tool use. Shows the tool name + status
 * pill in the header; expanding reveals input and output blocks rendered
 * through `RedactedText` so `[REDACTED_*]` placeholders stay visible.
 */
export function ToolUseCard({ tu }: { tu: ToolUse }) {
  const [open, setOpen] = useState(false);
  const statusColor =
    tu.status === 'success' || tu.status === 'ok'
      ? colors.green700
      : tu.status === 'error'
        ? colors.red700
        : colors.gray500;
  const statusBg =
    tu.status === 'success' || tu.status === 'ok'
      ? colors.green100
      : tu.status === 'error'
        ? colors.red100
        : colors.gray100;

  const renderBlock = (data: Record<string, unknown> | string) => {
    const text = typeof data === 'string' ? data : JSON.stringify(data, null, 2);
    return (
      <pre
        style={{
          background: colors.gray50,
          border: `1px solid ${colors.gray200}`,
          borderRadius: 4,
          padding: 8,
          fontSize: 12,
          whiteSpace: 'pre-wrap',
          wordBreak: 'break-word',
          maxHeight: 250,
          overflow: 'auto',
          margin: '4px 0',
        }}
      >
        <RedactedText text={text} />
      </pre>
    );
  };

  return (
    <div
      style={{
        border: `1px solid ${colors.gray200}`,
        borderRadius: 6,
        margin: '6px 0',
        overflow: 'hidden',
      }}
    >
      <div
        onClick={() => setOpen(!open)}
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: 8,
          padding: '6px 10px',
          cursor: 'pointer',
          background: colors.gray50,
          fontSize: 14,
        }}
      >
        <span style={{ fontFamily: 'monospace', fontWeight: 600 }}>{tu.tool}</span>
        <span
          style={{
            fontSize: 11,
            padding: '1px 6px',
            borderRadius: 9999,
            background: statusBg,
            color: statusColor,
            fontWeight: 500,
          }}
        >
          {tu.status}
        </span>
        <span style={{ marginLeft: 'auto', color: colors.gray400, fontSize: 12 }}>
          {open ? '\u25B2' : '\u25BC'}
        </span>
      </div>
      {open && (
        <div style={{ padding: '8px 10px' }}>
          <div style={{ fontSize: 12, fontWeight: 600, color: colors.gray500, marginBottom: 2 }}>
            Input
          </div>
          {renderBlock(tu.input)}
          <div
            style={{ fontSize: 11, fontWeight: 600, color: colors.gray500, marginBottom: 2, marginTop: 8 }}
          >
            Output
          </div>
          {renderBlock(tu.output)}
        </div>
      )}
    </div>
  );
}
