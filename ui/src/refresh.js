// Overlapping callers request one following read and await that read, rather
// than returning as soon as an older in-flight request has been noticed.
export function coalescedRefresh(run) {
  let active = null;
  let queued = null;
  function begin() {
    const cycle = Promise.resolve().then(run);
    active = cycle;
    function finished() {
      active = null;
      if (queued) {
        const waiting = queued;
        queued = null;
        begin().then(waiting.resolve, waiting.reject);
      }
    }
    cycle.then(finished, finished);
    return cycle;
  }
  return function refresh() {
    if (!active) return begin();
    if (!queued) queued = Promise.withResolvers();
    return queued.promise;
  };
}
