import React from 'react';

type PillProps = { label: string; title?: string };

export const PolicyPill: React.FC<PillProps> = ({ label, title }) => (
  <span
    title={title}
    style={{
      display: 'inline-block',
      padding: '2px 8px',
      margin: '2px 6px 2px 0',
      fontSize: 12,
      borderRadius: 999,
      border: '1px solid #D0D7DE',
      background: '#F6F8FA',
      color: '#24292F',
      lineHeight: '18px',
      verticalAlign: 'middle',
    }}
  >
    {label}
  </span>
);
