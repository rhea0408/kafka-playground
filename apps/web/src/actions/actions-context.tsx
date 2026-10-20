import { createContext, useContext, type ReactNode } from 'react';
import type { Actions } from './actions.ts';

const ActionsContext = createContext<Actions | null>(null);

/** Makes the app's actions (the command path) available to components. */
export function ActionsProvider({ actions, children }: { actions: Actions; children: ReactNode }) {
  return <ActionsContext.Provider value={actions}>{children}</ActionsContext.Provider>;
}

export function useActions(): Actions {
  const actions = useContext(ActionsContext);
  if (actions === null) throw new Error('useActions() needs an <ActionsProvider> above it');
  return actions;
}
