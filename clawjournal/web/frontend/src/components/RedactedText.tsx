import { colors } from '../theme.ts';

/**
 * Render text with `[REDACTED_*]` placeholder spans tinted so the eye picks
 * them out of the surrounding content. Accepts `null` / `undefined` so it
 * can be dropped into any message body without a guard at the call site.
 */
export function RedactedText({ text }: { text: string | null | undefined }) {
  if (!text) return null;
  const parts = text.split(/(\[REDACTED[^\]]*\])/g);
  return (
    <>
      {parts.map((part, i) =>
        part.startsWith('[REDACTED') ? (
          <span
            key={i}
            style={{
              background: colors.red100,
              color: colors.red700,
              borderRadius: 3,
              padding: '0 3px',
              fontWeight: 600,
              fontSize: '0.9em',
            }}
          >
            {part}
          </span>
        ) : (
          <span key={i}>{part}</span>
        ),
      )}
    </>
  );
}
