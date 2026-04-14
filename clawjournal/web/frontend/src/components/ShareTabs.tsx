import { NavLink } from 'react-router-dom';
import { colors } from '../theme.ts';

const TABS = [
  { to: '/share', label: 'Queue', end: true },
  { to: '/share/rules', label: 'Redaction rules', end: false },
];

export function ShareTabs() {
  return (
    <div
      style={{
        display: 'flex',
        gap: 4,
        borderBottom: `1px solid ${colors.gray200}`,
        marginBottom: 20,
      }}
    >
      {TABS.map((tab) => (
        <NavLink
          key={tab.to}
          to={tab.to}
          end={tab.end}
          style={({ isActive }) => ({
            padding: '8px 14px',
            fontSize: 14,
            fontWeight: isActive ? 600 : 500,
            color: isActive ? colors.gray900 : colors.gray500,
            textDecoration: 'none',
            borderBottom: isActive ? `2px solid ${colors.gray800}` : '2px solid transparent',
            marginBottom: -1,
          })}
        >
          {tab.label}
        </NavLink>
      ))}
    </div>
  );
}
