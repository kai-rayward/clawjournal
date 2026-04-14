import { colors } from '../theme.ts';

export interface StepperStep {
  key: string;
  label: string;
}

interface StepperProps {
  steps: StepperStep[];
  activeKey: string;
  completedKeys: Set<string>;
  onStepClick: (key: string) => void;
}

export function Stepper({ steps, activeKey, completedKeys, onStepClick }: StepperProps) {
  const activeIdx = steps.findIndex((s) => s.key === activeKey);

  return (
    <div
      style={{
        display: 'flex',
        gap: 6,
        flexWrap: 'wrap',
        alignItems: 'center',
        marginBottom: 20,
      }}
    >
      {steps.map((step, idx) => {
        const isActive = step.key === activeKey;
        const isCompleted = completedKeys.has(step.key);
        const isPast = idx < activeIdx;
        const clickable = isActive || isCompleted;

        // later steps are disabled unless already completed
        const disabled = !clickable;

        const background = isActive
          ? colors.primary500
          : isCompleted || isPast
            ? colors.primary100
            : colors.gray100;
        const color = isActive
          ? colors.white
          : isCompleted || isPast
            ? colors.primary500
            : colors.gray400;
        const border = isActive
          ? `1px solid ${colors.primary500}`
          : isCompleted || isPast
            ? `1px solid ${colors.primary200}`
            : `1px solid ${colors.gray200}`;

        return (
          <div key={step.key} style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <button
              type="button"
              onClick={() => {
                if (!disabled) onStepClick(step.key);
              }}
              disabled={disabled}
              style={{
                padding: '6px 14px',
                borderRadius: 9999,
                fontSize: 13,
                fontWeight: isActive ? 700 : 600,
                background,
                color,
                border,
                cursor: disabled ? 'not-allowed' : 'pointer',
                opacity: disabled ? 0.6 : 1,
                whiteSpace: 'nowrap',
              }}
            >
              <span style={{ marginRight: 6, fontVariantNumeric: 'tabular-nums' }}>{idx + 1}</span>
              {step.label}
            </button>
            {idx < steps.length - 1 && (
              <span style={{ color: colors.gray300, fontSize: 14, userSelect: 'none' }}>
                {'\u2192'}
              </span>
            )}
          </div>
        );
      })}
    </div>
  );
}
