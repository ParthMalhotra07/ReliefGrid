// Colour theme: "system" follows prefers-color-scheme; "light" and "dark"
// override it. Stored per browser; storage may be unavailable.
export const THEMES = [
  ["system", "System"],
  ["light", "Light"],
  ["dark", "Dark"],
];
const KEY = "reliefgrid.dispatch.theme.v1";
export function savedTheme() {
  try {
    const value = localStorage.getItem(KEY);
    return THEMES.some(([key]) => key === value) ? value : "system";
  } catch {
    return "system";
  }
}
export function applyTheme(value) {
  const root = document.documentElement;
  if (value === "light" || value === "dark") root.dataset.theme = value;
  else delete root.dataset.theme;
}
export function saveTheme(value) {
  applyTheme(value);
  try {
    localStorage.setItem(KEY, value);
  } catch {
    /* The choice still applies to this page. */
  }
}
