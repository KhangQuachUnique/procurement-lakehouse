(() => {
  const tooltip = document.getElementById("day-tooltip");
  if (!tooltip) return;
  const cells = document.querySelectorAll(".heat-day[data-tip]");
  const move = (event) => {
    const pad = 14;
    const rect = tooltip.getBoundingClientRect();
    let left = event.clientX + pad;
    let top = event.clientY + pad;
    if (left + rect.width + 8 > window.innerWidth) left = event.clientX - rect.width - pad;
    if (top + rect.height + 8 > window.innerHeight) top = event.clientY - rect.height - pad;
    tooltip.style.left = Math.max(8, left) + "px";
    tooltip.style.top = Math.max(8, top) + "px";
  };
  cells.forEach((cell) => {
    cell.addEventListener("mouseenter", (event) => {
      tooltip.textContent = cell.dataset.tip || "";
      tooltip.classList.add("show");
      move(event);
    });
    cell.addEventListener("mousemove", move);
    cell.addEventListener("mouseleave", () => tooltip.classList.remove("show"));
  });
})();
