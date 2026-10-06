(() => {
  const status = document.createElement("p");
  status.id = "calendar-live-status";
  status.className = "sync-status";
  status.setAttribute("role", "status");
  status.textContent = "Auto-refresh every 5 seconds; storage synchronization may take longer.";
  document.querySelector(".heatmap-panel").before(status);
  const refresh = async () => {
    try {
      if (document.hidden) return;
      const response = await fetch(window.location.href, {cache: "no-store"});
      if (!response.ok) throw new Error("HTTP " + response.status);
      const page = new DOMParser().parseFromString(await response.text(), "text/html");
      const updated = page.querySelector(".heatmap-panel");
      if (!updated) throw new Error("Calendar unavailable");
      const cells = new Map([...updated.querySelectorAll(".heat-day")].map(
        cell => [cell.getAttribute("href"), cell]));
      document.querySelectorAll(".heat-day").forEach(cell => {
        const next = cells.get(cell.getAttribute("href"));
        if (!next) return;
        // Keep existing nodes and tooltip listeners while updating status and evidence.
        for (const name of ["class", "data-tip", "title", "aria-label"]) {
          if (next.hasAttribute(name)) cell.setAttribute(name, next.getAttribute(name));
          else cell.removeAttribute(name);
        }
      });
      document.querySelector(".year-counts").innerHTML = updated.querySelector(".year-counts").innerHTML;
      const banner = document.querySelector(".main > .sync-status:not(#calendar-live-status)");
      const nextBanner = page.querySelector(".main > .sync-status");
      if (banner && nextBanner) banner.replaceWith(nextBanner);
      status.textContent = "Calendar updated " + new Date().toLocaleTimeString() +
        "; auto-refresh every 5 seconds. Storage synchronization may take longer.";
    } catch (error) {
      status.textContent = "Calendar refresh failed (" + error.message + "). Retrying in 5 seconds.";
    } finally {
      setTimeout(refresh, 5000);
    }
  };
  setTimeout(refresh, 5000);
})();
