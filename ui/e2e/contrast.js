// WCAG 2.x contrast for every visible text element against its effective
// background. Map tiles are images, so text drawn straight on the map is
// skipped; controls, tooltips and pop-ups with their own background count.
export async function contrastFailures(page) {
  return page.evaluate(() => {
    const parse = (value) => {
      const m = value.match(/rgba?\(([^)]+)\)/);
      if (!m) return null;
      const [r, g, b, a = 1] = m[1]
        .split(/[ ,/]+/)
        .filter(Boolean)
        .map(Number);
      return [r, g, b, a];
    };
    const blend = (top, bottom) => {
      const a = top[3];
      return [0, 1, 2].map((i) => top[i] * a + bottom[i] * (1 - a)).concat(1);
    };
    const channel = (v) => {
      const c = v / 255;
      return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
    };
    const luminance = ([r, g, b]) =>
      0.2126 * channel(r) + 0.7152 * channel(g) + 0.0722 * channel(b);
    const ratio = (a, b) => {
      const [x, y] = [luminance(a), luminance(b)].sort((p, q) => q - p);
      return (x + 0.05) / (y + 0.05);
    };
    // Background behind an element: blend semi-transparent layers down to
    // the first opaque one; null when an image (map tiles) is behind it.
    const background = (element) => {
      const layers = [];
      for (let node = element; node; node = node.parentElement) {
        const style = getComputedStyle(node);
        if (
          node.classList.contains("leaflet-container") ||
          (style.backgroundImage !== "none" &&
            !style.backgroundImage.startsWith("linear-gradient"))
        )
          return null;
        const color = parse(style.backgroundColor);
        if (color && color[3] > 0) {
          layers.push(color);
          if (color[3] >= 1) break;
        }
      }
      let result = [255, 255, 255, 1];
      if (layers.at(-1)?.[3] >= 1) result = layers.pop();
      else {
        const html = parse(
          getComputedStyle(document.documentElement).backgroundColor,
        );
        if (html && html[3] > 0) result = [...html.slice(0, 3), 1];
      }
      while (layers.length) result = blend(layers.pop(), result);
      return result;
    };
    const failures = [];
    for (const element of document.querySelectorAll("body *")) {
      const text = [...element.childNodes]
        .filter((n) => n.nodeType === 3)
        .map((n) => n.textContent.trim())
        .join("");
      if (!text) continue;
      const box = element.getBoundingClientRect();
      if (!box.width || !box.height) continue;
      const style = getComputedStyle(element);
      if (
        style.visibility === "hidden" ||
        element.closest("[inert], [aria-hidden='true']")
      )
        continue;
      if (element.closest(":disabled, [aria-disabled='true']")) continue;
      let opacity = 1;
      for (let node = element; node; node = node.parentElement)
        opacity *= Number(getComputedStyle(node).opacity);
      if (opacity < 0.05) continue;
      const back = background(element);
      if (!back) continue;
      const fore = parse(style.color);
      const ink = blend([...fore.slice(0, 3), fore[3] * opacity], back);
      const size = parseFloat(style.fontSize);
      const bold = Number(style.fontWeight) >= 700;
      const large = size >= 24 || (bold && size >= 18.66);
      const value = ratio(ink, back);
      if (value < (large ? 3 : 4.5))
        failures.push(
          `${value.toFixed(2)} ${style.color} on rgb(${back.slice(0, 3).join(",")}) ${element.tagName.toLowerCase()}.${[...element.classList].join(".")} "${text.slice(0, 30)}"`,
        );
    }
    return [...new Set(failures)];
  });
}
