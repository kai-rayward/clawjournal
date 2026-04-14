import { colors } from '../theme.ts';

export function Spinner({ text = 'Loading...' }: { text?: string }) {
  return (
    <div style={{ textAlign: 'center', padding: 40 }}>
      <div style={{
        display: 'inline-block',
        width: 24,
        height: 24,
        border: `3px solid ${colors.gray200}`,
        borderTopColor: colors.primary500,
        borderRadius: '50%',
        animation: 'spin 0.6s linear infinite',
        marginBottom: 10,
      }} />
      <div style={{ fontSize: 14, color: colors.gray400 }}>{text}</div>
      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
    </div>
  );
}

export function EmptyState({ icon, title, description, action }: {
  icon?: string;
  title: string;
  description?: string;
  action?: React.ReactNode;
}) {
  return (
    <div style={{
      textAlign: 'center',
      padding: '48px 20px',
      color: colors.gray400,
    }}>
      {icon && <div style={{ fontSize: 32, marginBottom: 8 }}>{icon}</div>}
      <h3 style={{ margin: '0 0 6px', fontSize: 16, fontWeight: 600, color: colors.gray500 }}>
        {title}
      </h3>
      {description && (
        <p style={{ margin: '0 0 16px', fontSize: 14, color: colors.gray400, maxWidth: 400, marginInline: 'auto', lineHeight: 1.5 }}>
          {description}
        </p>
      )}
      {action}
    </div>
  );
}
