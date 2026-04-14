import type React from 'react';

/* ------------------------------------------------------------------ */
/*  Design tokens — warm, minimal, quiet                              */
/* ------------------------------------------------------------------ */

export const colors = {
  // Warm neutrals (sand/linen base)
  gray50: '#faf8f5',
  gray100: '#f3f0eb',
  gray200: '#e8e4dd',
  gray300: '#d5d0c7',
  gray400: '#a09a8f',
  gray500: '#7a756b',
  gray600: '#5c5850',
  gray700: '#403d37',
  gray800: '#2a2825',
  gray900: '#1b1a17',

  // Primary (warm amber)
  primary50: '#fef9ee',
  primary100: '#fdf0d5',
  primary200: '#f9dca0',
  primary400: '#d4940a',
  primary500: '#b47d08',
  primary700: '#8a6008',

  // Blue (slate-blue, muted)
  blue50: '#f4f6fa',
  blue100: '#e4e8f0',
  blue400: '#7889a8',
  blue500: '#5f7191',
  blue600: '#4d5f7e',
  blue700: '#3d4d68',

  // Green (sage)
  green50: '#f4f8f2',
  green100: '#e4eede',
  green200: '#c8ddbf',
  green400: '#6a9e5a',
  green500: '#558745',
  green700: '#3d6830',
  green800: '#2d5422',

  // Red (terracotta)
  red50: '#fdf5f2',
  red100: '#fae5dd',
  red200: '#f2cabb',
  red400: '#c4624a',
  red500: '#ab503a',
  red700: '#7a3425',

  // Yellow / Amber
  yellow50: '#fefbf0',
  yellow100: '#fdf3d4',
  yellow200: '#fbe49a',
  yellow400: '#c4890a',
  yellow700: '#7a5508',

  // Teal (eucalyptus)
  teal400: '#4d9e8a',

  // Indigo (dusty)
  indigo400: '#7a7aad',

  // Emerald (forest)
  emerald400: '#4d9e6e',

  white: '#fefdfb',
  black: '#1b1a17',
} as const;

/* ------------------------------------------------------------------ */
/*  Font                                                              */
/* ------------------------------------------------------------------ */

export const fontFamily = "'Inter', system-ui, -apple-system, 'Segoe UI', sans-serif";

/* ------------------------------------------------------------------ */
/*  Buttons                                                           */
/* ------------------------------------------------------------------ */

export const btnPrimary: React.CSSProperties = {
  padding: '8px 20px',
  background: colors.gray800,
  color: colors.gray50,
  border: 'none',
  borderRadius: 8,
  fontSize: 14,
  fontWeight: 500,
  cursor: 'pointer',
  whiteSpace: 'nowrap',
  fontFamily,
};

export const btnSecondary: React.CSSProperties = {
  padding: '7px 14px',
  background: colors.gray50,
  color: colors.gray700,
  border: `1px solid ${colors.gray300}`,
  borderRadius: 8,
  fontSize: 14,
  fontWeight: 500,
  cursor: 'pointer',
  whiteSpace: 'nowrap',
  fontFamily,
};

export const btnDanger: React.CSSProperties = {
  padding: '7px 14px',
  background: colors.red50,
  color: colors.red700,
  border: `1px solid ${colors.red200}`,
  borderRadius: 8,
  fontSize: 14,
  fontWeight: 500,
  cursor: 'pointer',
  whiteSpace: 'nowrap',
  fontFamily,
};

export const btnGhost: React.CSSProperties = {
  background: 'none',
  border: 'none',
  color: colors.gray600,
  cursor: 'pointer',
  fontSize: 13,
  padding: 0,
  textDecoration: 'underline',
  textUnderlineOffset: '2px',
  fontFamily,
};

export const selectStyle: React.CSSProperties = {
  padding: '6px 10px',
  borderRadius: 8,
  border: `1px solid ${colors.gray300}`,
  fontSize: 13,
  background: colors.white,
  cursor: 'pointer',
  color: colors.gray700,
  fontFamily,
};

export const inputStyle: React.CSSProperties = {
  width: '100%',
  padding: '7px 10px',
  border: `1px solid ${colors.gray300}`,
  borderRadius: 8,
  fontSize: 14,
  fontFamily,
  boxSizing: 'border-box' as const,
};

export const labelStyle: React.CSSProperties = {
  display: 'block',
  fontWeight: 500,
  color: colors.gray600,
  marginTop: 8,
  marginBottom: 3,
  fontSize: 13,
  fontFamily,
};

export const cardStyle: React.CSSProperties = {
  background: colors.white,
  border: `1px solid ${colors.gray200}`,
  borderRadius: 10,
  overflow: 'hidden',
};

/* ------------------------------------------------------------------ */
/*  Section header                                                    */
/* ------------------------------------------------------------------ */

export const sectionHeaderStyle: React.CSSProperties = {
  fontWeight: 600,
  fontSize: 12,
  color: colors.gray400,
  letterSpacing: '0.02em',
  marginBottom: 6,
};
