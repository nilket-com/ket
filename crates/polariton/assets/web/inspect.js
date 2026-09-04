/* PRODUCT-0155 and PRODUCT-0157: the inspection script. Lookup, visibility,
   and presentation only. Polariton computed every region, every hit, and
   every legend rectangle; this file tests packaged primitives, toggles one
   class on ordinal-addressed groups, and shows packaged prose. It contains
   no plot logic, no network, no evaluation, no attribute write other than
   that class, no geometry read, and — because it is inlined into a shell
   that is both HTML and XML — no literal less-than or ampersand characters. */
"use strict";

var POLARITON_HIDDEN_CLASS = "polariton-hidden";

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

/* Hidden state is a sorted array of ordinals. The algebra scans it (the
   universe is at most 4,096); the resolver never does — it takes a
   precomputed constant-time membership set (`polaritonHiddenSet`), so one
   visibility check is one lookup however many series are hidden. */
function polaritonIsHidden(hidden, ordinal) {
	for (var i = 0; i in hidden; i++) {
		if (hidden[i] === ordinal) { return true; }
	}
	return false;
}

function polaritonHiddenSet(hidden) {
	var set = {};
	for (var i = 0; i in hidden; i++) { set[hidden[i]] = true; }
	return set;
}

/* Returns { index: hit index or -1, ops: primitive tests performed }.
   PRODUCT-0157 §6: a region whose series is in `hiddenSet` is skipped before
   any containment test; the lookup is one counted operation. `hiddenSet` is
   the object `polaritonHiddenSet` builds, or absent. */
function polaritonResolve(projection, x, y, hiddenSet) {
	if (hiddenSet === undefined) { hiddenSet = {}; }
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
			if (region.series !== null) {
				if (region.series !== undefined) {
					ops++;
					if (hiddenSet[region.series] === true) { continue; }
				}
			}
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

/* PRODUCT-0157 §4: the desktop's action algebra over ordinals. `universe` is
   the table's ordinal list; unknown ordinals are ignored and repeated actions
   return the same array, so callers can test identity for "changed". */
function polaritonSortedCopy(values) {
	var out = values.slice();
	out.sort(function (a, b) { return a - b; });
	return out;
}

function polaritonApplyVisibility(hidden, action, universe) {
	var known = polaritonIsHidden(universe, action.ordinal);
	if (action.kind === "restore") {
		if (hidden.length === 0) { return hidden; }
		return [];
	}
	if (!known) { return hidden; }
	if (action.kind === "hide") {
		if (polaritonIsHidden(hidden, action.ordinal)) { return hidden; }
		return polaritonSortedCopy(hidden.concat([action.ordinal]));
	}
	if (action.kind === "show") {
		if (!polaritonIsHidden(hidden, action.ordinal)) { return hidden; }
		var kept = [];
		for (var i = 0; i in hidden; i++) {
			if (hidden[i] !== action.ordinal) { kept.push(hidden[i]); }
		}
		return kept;
	}
	if (action.kind === "isolate") {
		var others = [];
		for (var j = 0; j in universe; j++) {
			if (universe[j] !== action.ordinal) { others.push(universe[j]); }
		}
		others = polaritonSortedCopy(others);
		if (others.length === hidden.length) {
			var same = true;
			for (var k = 0; k in others; k++) {
				if (others[k] !== hidden[k]) { same = false; }
			}
			if (same) { return hidden; }
		}
		return others;
	}
	return hidden;
}

/* The desktop's `legend_action`: what one press on an entry means now. */
function polaritonLegendAction(hidden, ordinal, isolate, universe) {
	if (isolate) {
		if (!polaritonIsHidden(hidden, ordinal)) {
			var allOthersHidden = true;
			for (var i = 0; i in universe; i++) {
				if (universe[i] !== ordinal) {
					if (!polaritonIsHidden(hidden, universe[i])) { allOthersHidden = false; }
				}
			}
			if (allOthersHidden) { return { kind: "restore", ordinal: ordinal }; }
		}
		return { kind: "isolate", ordinal: ordinal };
	}
	if (polaritonIsHidden(hidden, ordinal)) { return { kind: "show", ordinal: ordinal }; }
	return { kind: "hide", ordinal: ordinal };
}

/* The legend entry under a point, from the packaged rectangles: the index of
   the entry, or -1. Rectangles are the desktop's half-open `contains`. */
function polaritonLegendEntryAt(table, x, y) {
	if (!table.legend) { return -1; }
	var entries = table.legend.entries;
	for (var i = 0; i in entries; i++) {
		var e = entries[i];
		if (x >= e.x0) { if (e.x1 > x) { if (y >= e.y0) { if (e.y1 > y) { return i; } } } }
	}
	return -1;
}

function polaritonInsideLegend(table, x, y) {
	if (!table.legend) { return false; }
	var l = table.legend;
	if (x >= l.x0) { if (l.x1 > x) { if (y >= l.y0) { if (l.y1 > y) { return true; } } } }
	return false;
}

function polaritonUniverse(table) {
	var universe = [];
	for (var i = 0; i in table.series; i++) { universe.push(table.series[i].ordinal); }
	return universe;
}

/* PRODUCT-0157 §4, as one state machine shared by the pure sequence and the
   installed handlers. State is { hidden, armed }: the hidden array and the
   index of the entry a bare press armed, or -1. Forbidden modifiers (alt,
   ctrl, meta) are rejected before shift is consulted; shift acts on the
   press and consumes the release; a bare press arms a toggle that fires only
   on release over the same entry; cancel, blur, and any lost ownership
   disarm. */
function polaritonGesturePress(table, state, x, y, mods) {
	var next = { hidden: state.hidden, armed: -1 };
	if (table.actions.kind !== "available") { return next; }
	var entry = polaritonLegendEntryAt(table, x, y);
	if (entry === -1) { return next; }
	var ordinal = table.legend.entries[entry].series;
	if (ordinal === null) { return next; }
	if (ordinal === undefined) { return next; }
	if (mods.alt) { return next; }
	if (mods.ctrl) { return next; }
	if (mods.meta) { return next; }
	var universe = polaritonUniverse(table);
	if (mods.shift) {
		next.hidden = polaritonApplyVisibility(state.hidden, polaritonLegendAction(state.hidden, ordinal, true, universe), universe);
		return next;
	}
	next.armed = entry;
	return next;
}

function polaritonGestureRelease(table, state, x, y) {
	var next = { hidden: state.hidden, armed: -1 };
	if (state.armed === -1) { return next; }
	var released = polaritonLegendEntryAt(table, x, y);
	if (released !== state.armed) { return next; }
	var ordinal = table.legend.entries[state.armed].series;
	var universe = polaritonUniverse(table);
	next.hidden = polaritonApplyVisibility(state.hidden, polaritonLegendAction(state.hidden, ordinal, false, universe), universe);
	return next;
}

function polaritonGestureDisarm(state) {
	return { hidden: state.hidden, armed: -1 };
}

/* The pure sequence: events are { type, x, y, shift, alt, ctrl, meta } with
   type "down", "up", "cancel", or "blur", in viewBox units. Returns the
   hidden array. */
function polaritonLegendSequence(table, hidden, events) {
	var state = { hidden: hidden, armed: -1 };
	for (var i = 0; i in events; i++) {
		var ev = events[i];
		if (ev.type === "down") {
			state = polaritonGesturePress(table, state, ev.x, ev.y, { shift: ev.shift, alt: ev.alt, ctrl: ev.ctrl, meta: ev.meta });
			continue;
		}
		if (ev.type === "up") { state = polaritonGestureRelease(table, state, ev.x, ev.y); continue; }
		state = polaritonGestureDisarm(state);
	}
	return state.hidden;
}

/* PRODUCT-0157 §5: apply a hidden set to the SVG by class alone. */
function polaritonApplyToSvg(svg, universe, hidden) {
	var set = polaritonHiddenSet(hidden);
	for (var i = 0; i in universe; i++) {
		var ordinal = universe[i];
		var selector = "[data-series=\"s" + ordinal + "\"], [data-legend=\"s" + ordinal + "\"]";
		var groups = svg.querySelectorAll(selector);
		var hide = set[ordinal] === true;
		for (var g = 0; g in groups; g++) {
			if (hide) { groups[g].classList.add(POLARITON_HIDDEN_CLASS); }
			else { groups[g].classList.remove(POLARITON_HIDDEN_CLASS); }
		}
	}
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
			var seriesData = figure.querySelector("script.polariton-series");
			var svg = figure.querySelector("svg");
			if (!svg) { return; }
			var projection = data ? JSON.parse(data.textContent) : null;
			var table = seriesData ? JSON.parse(seriesData.textContent) : null;
			var actionable = false;
			if (table) { if (table.actions.kind === "available") { actionable = true; } }
			var universe = actionable ? polaritonUniverse(table) : [];
			/* PRODUCT-0157 §3: the snapshot. The shell already emitted the class
			   on the hidden groups so nothing hidden paints before this runs; an
			   inert figure's ink is the desktop's own and receives no class. */
			var state = { hidden: actionable ? polaritonSortedCopy(table.initially_hidden) : [], armed: -1 };
			var hiddenSet = polaritonHiddenSet(state.hidden);
			if (actionable) { polaritonApplyToSvg(svg, universe, state.hidden); }
			function toView(event) {
				var box = svg.getBoundingClientRect();
				var view = svg.viewBox.baseVal;
				if (box.width === 0) { return null; }
				if (box.height === 0) { return null; }
				return {
					x: (event.clientX - box.left) * (view.width / box.width),
					y: (event.clientY - box.top) * (view.height / box.height)
				};
			}
			function commit(next) {
				var changed = next.hidden !== state.hidden;
				state = next;
				if (!changed) { return; }
				hiddenSet = polaritonHiddenSet(state.hidden);
				polaritonApplyToSvg(svg, universe, state.hidden);
				tooltip.hidden = true;
			}
			svg.addEventListener("pointermove", function (event) {
				if (!projection) { return; }
				var p = toView(event);
				if (!p) { return; }
				var result = polaritonResolve(projection, p.x, p.y, hiddenSet);
				if (result.index === -1) { tooltip.hidden = true; return; }
				tooltip.textContent = projection.hits[result.index].presentation;
				tooltip.style.left = (event.clientX + 12) + "px";
				tooltip.style.top = (event.clientY + 12) + "px";
				tooltip.hidden = false;
			});
			svg.addEventListener("pointerleave", function () { tooltip.hidden = true; });
			if (!actionable) { return; }
			svg.addEventListener("pointerdown", function (event) {
				if (event.button !== 0) { commit(polaritonGestureDisarm(state)); return; }
				var p = toView(event);
				if (!p) { commit(polaritonGestureDisarm(state)); return; }
				if (!polaritonInsideLegend(table, p.x, p.y)) { commit(polaritonGestureDisarm(state)); return; }
				event.preventDefault();
				var next = polaritonGesturePress(table, state, p.x, p.y, { shift: event.shiftKey, alt: event.altKey, ctrl: event.ctrlKey, meta: event.metaKey });
				/* Ownership: the release reaches this element wherever it lands. */
				if (next.armed !== -1) { if (svg.setPointerCapture) { svg.setPointerCapture(event.pointerId); } }
				commit(next);
			});
			svg.addEventListener("pointerup", function (event) {
				if (state.armed === -1) { return; }
				var p = toView(event);
				if (!p) { commit(polaritonGestureDisarm(state)); return; }
				commit(polaritonGestureRelease(table, state, p.x, p.y));
			});
			svg.addEventListener("pointercancel", function () { commit(polaritonGestureDisarm(state)); });
			svg.addEventListener("lostpointercapture", function () { commit(polaritonGestureDisarm(state)); });
			window.addEventListener("pointerup", function () { commit(polaritonGestureDisarm(state)); });
			window.addEventListener("blur", function () { commit(polaritonGestureDisarm(state)); });
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
