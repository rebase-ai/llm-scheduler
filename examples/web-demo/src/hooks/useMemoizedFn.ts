import { useMemo, useRef } from 'react';

type noop = (this: any, ...args: any[]) => any;

type PickFunction<T extends noop> = (this: ThisParameterType<T>, ...args: Parameters<T>) => ReturnType<T>;

/**
 * useMemoizedFn is a custom hook that returns a memoized version of the provided function.
 * This ensures that the function reference remains stable across re-renders, preventing unnecessary re-executions.
 *
 * @template T - The type of the function to be memoized.
 * @param {T} fn - The function to be memoized. It should be a valid function.
 * @returns {T} - The memoized function.
 *
 * @throws Will log an error to the console if the provided argument is not a function.
 *
 * @example
 * const memoizedCallback = useMemoizedFn((value) => {
 *   console.log(value);
 * });
 *
 * // The memoizedCallback reference will remain stable across re-renders.
 */
function useMemoizedFn<T extends noop>(fn: T) {
  if (typeof fn !== 'function') {
    console.error(`useMemoizedFn expected parameter is a function, got ${typeof fn}`);
  }

  const fnRef = useRef<T>(fn);

  // why not write `fnRef.current = fn`?
  // https://github.com/alibaba/hooks/issues/728
  fnRef.current = useMemo<T>(() => fn, [fn]);

  const memoizedFn = useRef<PickFunction<T>>(undefined);
  if (!memoizedFn.current) {
    memoizedFn.current = function (this, ...args) {
      return fnRef.current.apply(this, args);
    };
  }

  return memoizedFn.current as T;
}

export default useMemoizedFn;
