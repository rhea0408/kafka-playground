/**
 * The Follow toggle of the event list and the partition log: a real toggle button (aria-pressed).
 * On: filled primary with a check mark; off: outlined. The check mark means the state never
 * depends on color alone.
 */
export function FollowToggle({
  following,
  onChange,
  controls,
}: {
  following: boolean;
  onChange: (following: boolean) => void;
  /** Id of the list it scrolls. */
  controls: string;
}) {
  return (
    <button
      type="button"
      aria-pressed={following}
      aria-controls={controls}
      title={
        following
          ? 'Following the newest rows. Scroll up to pause.'
          : 'Paused. Press to jump to the newest rows and follow them.'
      }
      onClick={() => {
        onChange(!following);
      }}
      className={
        'inline-flex h-6 shrink-0 items-center gap-1 rounded-sm border px-2 text-[12px] ' +
        'focus-visible:outline-2 focus-visible:outline-offset-1 focus-visible:outline-primary ' +
        (following
          ? 'border-primary bg-primary text-surface'
          : 'border-border bg-surface text-status hover:bg-muted')
      }
    >
      <span aria-hidden="true" className="inline-block w-3 text-center">
        {following ? '✓' : ''}
      </span>
      Follow
    </button>
  );
}
