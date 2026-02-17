export interface DeadlineCollectionResult<T> {
  fulfilled: T[];
  failed: number;
  pendingAtDeadline: number;
  timedOut: boolean;
  total: number;
}

type SettledResult<T> =
  | {
      index: number;
      status: 'fulfilled';
      value: T;
    }
  | {
      index: number;
      status: 'rejected';
      reason: unknown;
    };

/**
 * Collects fulfilled values from promises as they settle, optionally stopping at a deadline.
 *
 * If deadlineMs is undefined, this waits for all promises to settle.
 * If deadlineMs is reached first, unresolved promises are ignored and pendingAtDeadline is set.
 */
export async function collectUntilDeadline<T>(
  promises: Promise<T>[],
  deadlineMs?: number
): Promise<DeadlineCollectionResult<T>> {
  if (promises.length === 0) {
    return {
      fulfilled: [],
      failed: 0,
      pendingAtDeadline: 0,
      timedOut: false,
      total: 0,
    };
  }

  const wrapped = promises.map((promise, index) =>
    promise
      .then(
        (value): SettledResult<T> => ({
          index,
          status: 'fulfilled',
          value,
        })
      )
      .catch(
        (reason): SettledResult<T> => ({
          index,
          status: 'rejected',
          reason,
        })
      )
  );

  const pending = new Map<number, Promise<SettledResult<T>>>();
  wrapped.forEach((promise, index) => pending.set(index, promise));

  const fulfilled: T[] = [];
  let failed = 0;
  let timedOut = false;
  const start = Date.now();

  while (pending.size > 0) {
    let settled: SettledResult<T> | null = null;

    if (deadlineMs === undefined) {
      settled = await Promise.race(pending.values());
    } else {
      const elapsed = Date.now() - start;
      const remaining = deadlineMs - elapsed;
      if (remaining <= 0) {
        timedOut = true;
        break;
      }

      const timeoutResult = await Promise.race<SettledResult<T> | null>([
        Promise.race(pending.values()),
        new Promise<null>((resolve) => setTimeout(() => resolve(null), remaining)),
      ]);

      if (timeoutResult === null) {
        timedOut = true;
        break;
      }

      settled = timeoutResult;
    }

    pending.delete(settled.index);

    if (settled.status === 'fulfilled') {
      fulfilled.push(settled.value);
    } else {
      failed++;
    }
  }

  return {
    fulfilled,
    failed,
    pendingAtDeadline: pending.size,
    timedOut,
    total: promises.length,
  };
}
