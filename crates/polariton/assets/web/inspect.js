/* PRODUCT-0155: the inspection script. Lookup and presentation only.
   Polariton computed every region and every hit; this file tests packaged
   primitives and shows packaged prose. It contains no plot logic, no
   network, no evaluation, and — because it is inlined into a shell that is
   both HTML and XML — no literal less-than or ampersand characters. */
"use strict";

function polaritonFround(v) { return Math.fround(v); }

function polaritonInsideRect(r, x, y) {
	if (r.inclusive_max) {
		if (x >= r.x0) { if (r.x1 >= x) { if (y >= r.y0) { if (r.y1 >= y) { return true; } } } }
		return false;
	}
	if (x >= r.x0) { if (r.x1 > x) { if (y >= r.y0) { if (r.y1 > y) { return true; } } } }
	return false;
}

/* Squared distance in 32-bit float arithmetic, the desktop's own. */
function polaritonDistanceSq(cx, cy, x, y) {
	var dx = polaritonFround(x - cx);
	var dy = polaritonFround(y - cy);
	return polaritonFround(polaritonFround(dx * dx) + polaritonFround(dy * dy));
}

/* Returns { index: hit index or -1, ops: primitive tests performed }. */
function polaritonResolve(projection, x, y) {
	/* The desktop receives its cursor in 32-bit floats; so does this. */
	x = polaritonFround(x);
	y = polaritonFround(y);
	var ops = 0;
	var stages = projection.stages;
	for (var s = 0; s in stages; s++) {
		var stage = stages[s];
		if (stage.within) {
			ops++;
			if (!polaritonInsideRect(stage.within, x, y)) { continue; }
		}
		/* Into the regions' own space, the desktop's way: cursor minus origin. */
		var lx = polaritonFround(x - stage.origin[0]);
		var ly = polaritonFround(y - stage.origin[1]);
		var regions = stage.regions;
		var best = -1;
		var bestDistance = 0;
		var bestOrdinal = 0;
		for (var i = 0; i in regions; i++) {
			var region = regions[i];
			ops++;
			if (region.kind === "rect") {
				if (polaritonInsideRect(region, lx, ly)) {
					if (stage.policy === "first-in-order") { return { index: region.hit, ops: ops }; }
				}
				continue;
			}
			var radiusSq = polaritonFround(region.r * region.r);
			var d = polaritonDistanceSq(region.cx, region.cy, lx, ly);
			if (d > radiusSq) { continue; }
			if (stage.policy === "first-in-order") { return { index: region.hit, ops: ops }; }
			if (best === -1) { best = i; bestDistance = d; bestOrdinal = region.ordinal; continue; }
			ops++;
			if (stage.tie === "later-wins") {
				if (bestDistance >= d) { best = i; bestDistance = d; bestOrdinal = region.ordinal; }
				continue;
			}
			if (bestDistance > d) { best = i; bestDistance = d; bestOrdinal = region.ordinal; continue; }
			if (bestDistance === d) { if (bestOrdinal > region.ordinal) { best = i; bestDistance = d; bestOrdinal = region.ordinal; } }
		}
		if (best >= 0) { return { index: regions[best].hit, ops: ops }; }
	}
	return { index: -1, ops: ops };
}

function polaritonInstall() {
	var tooltip = document.createElement("div");
	tooltip.className = "polariton-tooltip";
	tooltip.hidden = true;
	document.body.appendChild(tooltip);
	var figures = document.querySelectorAll("figure.figure");
	for (var f = 0; f in figures; f++) {
		(function (figure) {
			var data = figure.querySelector("script.polariton-hits");
			var svg = figure.querySelector("svg");
			if (!data) { return; }
			if (!svg) { return; }
			var projection = JSON.parse(data.textContent);
			svg.addEventListener("pointermove", function (event) {
				var box = svg.getBoundingClientRect();
				var view = svg.viewBox.baseVal;
				if (box.width === 0) { return; }
				if (box.height === 0) { return; }
				var x = (event.clientX - box.left) * (view.width / box.width);
				var y = (event.clientY - box.top) * (view.height / box.height);
				var result = polaritonResolve(projection, x, y);
				if (result.index === -1) { tooltip.hidden = true; return; }
				tooltip.textContent = projection.hits[result.index].presentation;
				tooltip.style.left = (event.clientX + 12) + "px";
				tooltip.style.top = (event.clientY + 12) + "px";
				tooltip.hidden = false;
			});
			svg.addEventListener("pointerleave", function () { tooltip.hidden = true; });
		})(figures[f]);
	}
}

if (typeof document !== "undefined") {
	if (document.readyState === "loading") {
		document.addEventListener("DOMContentLoaded", polaritonInstall);
	} else {
		polaritonInstall();
	}
}
