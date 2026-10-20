/** Empty-state text for a panel whose content arrives in a later task. */
export function Placeholder({ children }: { children: string }) {
  return <p className="p-4 text-[13px] text-status/70">{children}</p>;
}
