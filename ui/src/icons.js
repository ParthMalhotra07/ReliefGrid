// Resource type icons as inline SVG, bundled with the app: no icon fonts or
// external requests. Each draws in currentColor on a 16 x 16 grid.
const paths = {
  water:
    '<path d="M8 1.5C8 1.5 3.5 7 3.5 10a4.5 4.5 0 0 0 9 0C12.5 7 8 1.5 8 1.5z"/>',
  food: '<path d="M2 7.5h12a6 6 0 0 1-12 0z"/><path d="M5.25 1.5h1.5v4.5h-1.5zM7.25 1.5h1.5v4.5h-1.5zM9.25 1.5h1.5v4.5h-1.5z"/>',
  medicine: '<path d="M6 1.5h4v4.5h4.5v4H10v4.5H6V10H1.5V6H6z"/>',
  vehicle:
    '<path d="M1 3.5h8.5v7.5H1zM10.5 6h2.8l2.2 2.7V11h-5z"/><circle cx="4" cy="12.5" r="1.8"/><circle cx="12.5" cy="12.5" r="1.8"/>',
  house: '<path d="M8 1.5 1 7.5h2V14.5h3.8v-4h2.4v4H13V7.5h2z"/>',
};
export const TYPE_ICONS = {
  WATER: "water",
  FOOD: "food",
  MEDICINE: "medicine",
  TRANSPORTATION: "vehicle",
  SHELTER: "house",
};
export const typeIcon = (type) => {
  const name = TYPE_ICONS[type];
  return name
    ? `<svg class="type-icon" data-icon="${name}" viewBox="0 0 16 16" width="12" height="12" fill="currentColor" aria-hidden="true" focusable="false">${paths[name]}</svg>`
    : "";
};
