/**
 * Panel tab: the active tab is primary, bold and underlined (ui-spec: "active tab" in primary;
 * wireframes: bold + underline), so the state never depends on color alone.
 */
export const TAB_TRIGGER =
  'flex items-center text-[13px] text-status/70 hover:text-status ' +
  'data-[state=active]:font-semibold data-[state=active]:text-primary ' +
  'data-[state=active]:underline data-[state=active]:underline-offset-4 ' +
  'focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-primary';
