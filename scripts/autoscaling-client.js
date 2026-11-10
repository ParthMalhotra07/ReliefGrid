// Pure client logic shared by k6 and deterministic Node tests.
// Retry only a definitive CAS rejection, after a successful authoritative read.
// Unknown outcomes and unrelated errors remain failures, never fresh mutations.
export function cancelRequest(put, reread, onConflict = () => {}) {
  for (let attempt = 0; attempt < 3; attempt++) {
    const response = put();
    if (response.status !== 409) return response;
    let error;
    try { error = response.json(); } catch { return response; }
    if (error.code !== 'VERSION_CONFLICT' || error.outcomeUnknown !== false) return response;
    onConflict(response);
    if (attempt === 2) return response;
    const current = reread();
    if (current.status !== 200) return current;
  }
}
