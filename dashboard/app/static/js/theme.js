// Light / dark mode. Loaded in <head> so the page never flashes the wrong theme.
// No stored choice: follow the system setting. The toggle stores the choice in localStorage.
(function () {
  var KEY = "lfas-theme";
  function stored() { try { return localStorage.getItem(KEY); } catch (e) { return null; } }
  function system() { return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light"; }
  function current() { return stored() || system(); }
  function apply(theme) {
    document.documentElement.setAttribute("data-theme", theme);
    var label = document.querySelector("[data-theme-label]");
    if (label) label.textContent = theme === "dark" ? "Light mode" : "Dark mode";
    document.dispatchEvent(new CustomEvent("themechange", { detail: theme }));
  }
  apply(current());
  window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", function () {
    if (!stored()) apply(system());
  });
  document.addEventListener("DOMContentLoaded", function () {
    apply(current());
    var btn = document.querySelector("[data-theme-toggle]");
    if (btn) btn.addEventListener("click", function () {
      var next = current() === "dark" ? "light" : "dark";
      try { localStorage.setItem(KEY, next); } catch (e) { /* private window: still switch for this page */ }
      apply(next);
    });
  });
})();
