import { createContext, useContext, useState, useCallback, useRef } from 'react';
import { colors } from '../theme.ts';

export interface ToastAction {
  label: string;
  onClick: () => void;
}

export interface ToastOptions {
  type?: 'success' | 'error' | 'info';
  duration?: number;          // ms, default 3500
  action?: ToastAction;
}

interface ToastItem {
  id: number;
  message: string;
  type: 'success' | 'error' | 'info';
  action?: ToastAction;
}

interface ToastCtx {
  toast: (message: string, typeOrOptions?: 'success' | 'error' | 'info' | ToastOptions) => void;
}

const Ctx = createContext<ToastCtx>({ toast: () => {} });

export function useToast() {
  return useContext(Ctx);
}

export function ToastProvider({ children }: { children: React.ReactNode }) {
  const [items, setItems] = useState<ToastItem[]>([]);
  const counter = useRef(0);

  const toast = useCallback((
    message: string,
    typeOrOptions?: 'success' | 'error' | 'info' | ToastOptions,
  ) => {
    const opts: ToastOptions =
      typeof typeOrOptions === 'string' ? { type: typeOrOptions }
      : typeOrOptions ?? {};
    const id = ++counter.current;
    const type = opts.type ?? 'info';
    const duration = opts.duration ?? 3500;
    setItems(prev => [...prev, { id, message, type, action: opts.action }]);
    setTimeout(() => setItems(prev => prev.filter(t => t.id !== id)), duration);
  }, []);

  const bgMap = { success: colors.green700, error: colors.red500, info: colors.gray800 };

  return (
    <Ctx.Provider value={{ toast }}>
      {children}
      <div style={{
        position: 'fixed',
        bottom: 20,
        right: 20,
        display: 'flex',
        flexDirection: 'column',
        gap: 8,
        zIndex: 9999,
        pointerEvents: 'none',
      }}>
        {items.map(t => (
          <div key={t.id} style={{
            background: bgMap[t.type],
            color: colors.white,
            padding: '10px 18px',
            borderRadius: 8,
            fontSize: 14,
            fontWeight: 500,
            boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
            animation: 'toast-in 0.25s ease-out',
            pointerEvents: 'auto',
            maxWidth: 360,
          }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              <span>{t.message}</span>
              {t.action && (
                <button
                  onClick={() => {
                    t.action!.onClick();
                    setItems(prev => prev.filter(x => x.id !== t.id));
                  }}
                  style={{
                    background: 'transparent',
                    border: '1px solid rgba(255,255,255,0.35)',
                    color: colors.white,
                    borderRadius: 6,
                    padding: '4px 10px',
                    fontSize: 13,
                    fontWeight: 500,
                    cursor: 'pointer',
                  }}
                >
                  {t.action.label}
                </button>
              )}
            </div>
          </div>
        ))}
      </div>
      <style>{`@keyframes toast-in { from { opacity: 0; transform: translateY(12px); } to { opacity: 1; transform: translateY(0); } }`}</style>
    </Ctx.Provider>
  );
}
