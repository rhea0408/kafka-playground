import type { Component, ComponentKind, KeyStrategy } from '@kp/schema';
import * as Tooltip from '@radix-ui/react-tooltip';
import { useState } from 'react';
import { useActions } from '../actions/actions-context.tsx';
import { findComponent, useTopologyStore } from '../stores/topology-store.ts';
import { useUiStore, type Selection } from '../stores/ui-store.ts';
import {
  BooleanField,
  EnumField,
  FixedField,
  TextField,
  type Commit,
  type DraftState,
} from './controls.tsx';
import {
  EXTRA_FIELDS,
  fieldsOf,
  isEditable,
  metaOf,
  type ExtraFieldMeta,
  type FieldMeta,
} from './field-meta.ts';
import { FieldFrame, helperLine, helpText, KindChip, type FieldError } from './FieldFrame.tsx';
import { describeDefault, describeValue, toInputText } from './field-value.ts';
import { KeyStrategyField } from './KeyStrategyField.tsx';
import { ManualProduce } from './ManualProduce.tsx';
import { PartitionsStepper } from './PartitionsStepper.tsx';
import { ConsumedBy, TopicStats } from './TopicSummary.tsx';

const KIND_TITLES: Readonly<Record<ComponentKind, string>> = {
  broker: 'Broker',
  topic: 'Topic',
  producer: 'Producer',
  consumer: 'Consumer',
};

/** What the Properties tab shows for a selection (M2-decisions T10). */
export type PanelView =
  | { readonly type: 'empty' }
  | { readonly type: 'component'; readonly id: string }
  | { readonly type: 'connection'; readonly edgeId: string }
  | { readonly type: 'many'; readonly count: number };

export function panelView(selection: Selection): PanelView {
  const { nodeIds, edgeIds } = selection;
  const count = nodeIds.length + edgeIds.length;
  if (count === 0) return { type: 'empty' };
  const [node] = nodeIds;
  if (count === 1 && node !== undefined) return { type: 'component', id: node };
  const [edge] = edgeIds;
  if (count === 1 && edge !== undefined) return { type: 'connection', edgeId: edge };
  return { type: 'many', count };
}

const SECTION_TITLE = 'text-[11px] font-bold text-status/70';
const NOTE = 'text-[13px] text-status/70';

/**
 * Properties tab of the right panel (FR-1.3, wireframe 05b). It follows the canvas selection:
 * one component shows its config form; nothing, a connection or several items show a short note.
 */
export function PropertiesPanel() {
  const selection = useUiStore((s) => s.selection);
  const view = panelView(selection);
  const component = useTopologyStore((s) =>
    view.type === 'component' ? findComponent(s, view.id) : undefined,
  );

  return (
    <Tooltip.Provider delayDuration={300}>
      <div className="flex flex-col gap-3 p-4" data-testid="properties-panel">
        {view.type === 'empty' && (
          <p className={NOTE}>Select a component on the canvas to see and edit its config.</p>
        )}
        {view.type === 'connection' && (
          <p className={NOTE}>
            Connection <span className="font-mono">{view.edgeId}</span> selected. Connections have
            no config; press Delete or Backspace on the canvas to remove it.
          </p>
        )}
        {view.type === 'many' && (
          <p className={NOTE}>
            {view.count} items selected. Select a single component to edit its config.
          </p>
        )}
        {view.type === 'component' &&
          (component === undefined ? (
            <p className={NOTE}>Select a component on the canvas to see and edit its config.</p>
          ) : (
            // Keyed by id: drafts and errors never leak from one component to the next.
            <ComponentForm key={component.id} component={component} />
          ))}
      </div>
    </Tooltip.Provider>
  );
}

function ComponentForm({ component }: { component: Component }) {
  const actions = useActions();
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const title = KIND_TITLES[component.kind];
  return (
    <>
      <header className="mt-2.5">
        <div className="text-[11px] text-status/70">{title.toUpperCase()}</div>
        <h2 className="truncate text-base font-bold">{component.id}</h2>
      </header>
      {component.kind === 'topic' && (
        <TopicStats topicId={component.id} partitions={component.partitions} />
      )}
      {component.kind === 'producer' && <ManualProduce producerId={component.id} />}
      <section aria-labelledby="pp-config" className="flex flex-col gap-3">
        <h3 id="pp-config" className={`${SECTION_TITLE} mt-1`}>
          CONFIGURATION
        </h3>
        <IdRow component={component} />
        {fieldsOf(component.kind).map((field) => (
          <ConfigRow
            key={String(field)}
            component={component}
            field={String(field)}
            meta={metaOf(component.kind, field)}
          />
        ))}
        {EXTRA_FIELDS[component.kind].map((extra) => (
          <ExtraRow key={extra.key} extra={extra} />
        ))}
      </section>
      {component.kind === 'topic' && <ConsumedBy topicId={component.id} />}
      <button
        type="button"
        className="mt-1 h-10 w-[328px] rounded-md border-[1.5px] border-danger bg-surface text-[13px] text-danger-text hover:bg-danger-bg focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary"
        aria-describedby={deleteError === null ? undefined : 'pp-delete-error'}
        onClick={() => {
          void actions.removeComponent(component.id).then((result) => {
            if (!result.ok) setDeleteError(result.reason);
          });
        }}
      >
        Delete {title.toLowerCase()}
      </button>
      {deleteError !== null && (
        <p id="pp-delete-error" className="text-[11px] text-danger-text">
          Not deleted: {deleteError}
        </p>
      )}
    </>
  );
}

/** Ids are fixed (T2: the engine rejects an id change). Topics can't be renamed in Kafka either. */
function IdRow({ component }: { component: Component }) {
  const isTopic = component.kind === 'topic';
  const reason = isTopic
    ? 'Kafka can’t rename a topic: create a new one and move the data.'
    : 'Ids are fixed: remove the component and add a new one to change it.';
  return (
    <FieldFrame
      inputId="pp-id"
      field="id"
      label={isTopic ? 'Name' : 'Id'}
      kafkaName={isTopic ? '--topic' : undefined}
      help={isTopic ? 'The topic name. It is also its id on the canvas.' : 'The id on the canvas.'}
      helper={reason}
    >
      {(describedBy) => (
        <FixedField
          inputId="pp-id"
          text={component.id}
          describedBy={describedBy}
          disabled={false}
        />
      )}
    </FieldFrame>
  );
}

/** A real Kafka config the engine doesn't simulate yet: disabled, with its real default. */
function ExtraRow({ extra }: { extra: ExtraFieldMeta }) {
  const inputId = `pp-${extra.key}`;
  return (
    <FieldFrame
      inputId={inputId}
      field={extra.key}
      label={extra.label}
      kafkaName={extra.kafkaName}
      helperChip={<KindChip meta={extra} />}
      help={helpText(extra)}
      helper={helperLine(extra)}
    >
      {(describedBy) => (
        <FixedField
          inputId={inputId}
          text={describeDefault(extra.kafkaDefault, extra.control)}
          describedBy={describedBy}
          disabled
        />
      )}
    </FieldFrame>
  );
}

const sameValue = (a: unknown, b: unknown): boolean => JSON.stringify(a) === JSON.stringify(b);

function ConfigRow(props: { component: Component; field: string; meta: FieldMeta }) {
  const { component, field, meta } = props;
  const actions = useActions();
  const [state, setState] = useState<DraftState>({ draft: null, error: null });
  const value: unknown = (component as unknown as Readonly<Record<string, unknown>>)[field];
  const inputId = `pp-${field}`;

  /** Sends only this field, and only if it changed (T2-7: a no-op patch is fine but pointless). */
  const commit: Commit = async (next) => {
    if (sameValue(next, value)) return null;
    const patch: Readonly<Record<string, unknown>> = { [field]: next };
    const result = await actions.updateConfig(component.id, patch);
    return result.ok ? null : result.reason;
  };

  /** Selects and checkboxes save on change; their only possible error is the engine's. */
  const commitChoice = (next: unknown): void => {
    void commit(next).then((reason) => {
      setState({
        draft: null,
        error: reason === null ? null : { source: 'engine', message: reason },
      });
    });
  };

  const setError = (error: FieldError | null): void => {
    setState({ draft: state.draft, error });
  };

  const isPartitions = meta.control.type === 'partitions';
  return (
    <FieldFrame
      inputId={inputId}
      field={field}
      label={meta.label}
      kafkaName={meta.kind === 'game-setting' ? undefined : meta.kafkaName}
      chip={meta.kind === 'not-modelled' ? undefined : <KindChip meta={meta} />}
      helperChip={meta.kind === 'not-modelled' ? <KindChip meta={meta} /> : undefined}
      help={helpText(meta)}
      helper={helperLine(meta)}
      // The partitions stepper places its own errors (next to its Add button).
      error={isPartitions ? null : state.error}
    >
      {(describedBy) => {
        if (!isEditable(meta)) {
          return (
            <FixedField
              inputId={inputId}
              text={
                meta.control.type === 'int' || meta.control.type === 'number'
                  ? describeValue(value, meta.control)
                  : toInputText(value)
              }
              describedBy={describedBy}
              disabled={meta.kind === 'not-modelled'}
            />
          );
        }
        const control = meta.control;
        switch (control.type) {
          case 'partitions':
            return component.kind === 'topic' ? (
              <PartitionsStepper
                inputId={inputId}
                topicId={component.id}
                current={component.partitions}
                describedBy={describedBy}
                commit={commit}
              />
            ) : null;
          case 'keyStrategy':
            return component.kind === 'producer' ? (
              <KeyStrategyField
                inputId={inputId}
                value={value as KeyStrategy}
                describedBy={describedBy}
                error={state.error}
                setError={setError}
                commit={commit}
              />
            ) : null;
          case 'enum':
            return (
              <EnumField
                inputId={inputId}
                value={value}
                options={control.options}
                describedBy={describedBy}
                invalid={state.error !== null}
                commit={commitChoice}
              />
            );
          case 'boolean':
            return (
              <BooleanField
                inputId={inputId}
                value={value}
                describedBy={describedBy}
                invalid={state.error !== null}
                commit={commitChoice}
              />
            );
          case 'int':
          case 'number':
          case 'text':
            return (
              <TextField
                inputId={inputId}
                kind={component.kind}
                field={field}
                control={control}
                value={value}
                describedBy={describedBy}
                state={state}
                setState={setState}
                commit={commit}
              />
            );
        }
      }}
    </FieldFrame>
  );
}
