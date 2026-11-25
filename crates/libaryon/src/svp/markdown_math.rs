//! Unicode math for the shared Markdown preview.
//!
//! `$\infty$` displays as `∞` and `$\mathcal{B}_{\text{weak}}$` as `ℬ_weak`.
//! An index uses subscript or superscript characters when Unicode can
//! represent the entire index. Other indices use `_` or `^`. Spaces in the
//! math source separate operators and operands in the rendered line.
//!
//! Unsupported commands, malformed groups, and nesting past [`MAX_DEPTH`]
//! leave the whole expression available for the caller to display as TeX.

/// Project `tex`, the body of a `$…$` or `$$…$$` span, to one line of Unicode
/// text. `None` when any part of it is outside the supported subset or when
/// nothing visible would remain.
pub(super) fn render_math(tex: &str) -> Option<String> {
	let mut lowering = Lowering {
		rest: tex,
		depth: 0,
	};
	let mut text = String::with_capacity(tex.len());
	lowering.sequence(&mut text, false)?;
	let trimmed = text.trim();
	(!trimmed.is_empty()).then(|| trimmed.to_string())
}

/// How deeply groups and arguments may nest. The parser recurses once per
/// level and reads arbitrary files, so the bound is what keeps a line of
/// `{{{{…` from becoming a stack overflow; real formulas nest two or three
/// deep, and one past the bound is simply unsupported.
const MAX_DEPTH: u32 = 16;

struct Lowering<'a> {
	rest: &'a str,
	depth: u32,
}

impl<'a> Lowering<'a> {
	fn peek(&self) -> Option<char> {
		self.rest.chars().next()
	}

	fn bump(&mut self) -> Option<char> {
		let ch = self.peek()?;
		self.rest = &self.rest[ch.len_utf8()..];
		Some(ch)
	}

	fn skip_whitespace(&mut self) {
		self.rest = self.rest.trim_start();
	}

	/// Run `parse` one level deeper. Every recursive path (a group, an
	/// argument, a command that takes an argument) goes through here.
	fn nested<T>(&mut self, parse: impl FnOnce(&mut Self) -> Option<T>) -> Option<T> {
		if self.depth == MAX_DEPTH {
			return None;
		}
		self.depth += 1;
		let parsed = parse(self);
		self.depth -= 1;
		parsed
	}

	/// Everything up to the end of the input, or up to the `}` closing a group
	/// when `in_group`. A `}` with no group open, or an input that ends inside
	/// a group, is malformed.
	fn sequence(&mut self, out: &mut String, in_group: bool) -> Option<()> {
		loop {
			let Some(ch) = self.peek() else {
				return (!in_group).then_some(());
			};
			match ch {
				'}' => {
					self.bump();
					return in_group.then_some(());
				}
				'{' => {
					self.bump();
					self.nested(|lowering| lowering.sequence(out, true))?;
				}
				'_' | '^' => {
					self.bump();
					let index = self.atom()?;
					push_index(out, ch == '_', index.trim());
				}
				'\\' => self.command(out)?,
				ch if ch.is_whitespace() => {
					self.skip_whitespace();
					push_space(out);
				}
				_ => {
					self.bump();
					push_plain_char(out, ch)?;
				}
			}
		}
	}

	/// One argument, rendered on its own because its caller transforms it: a
	/// braced group, a command with its own arguments, or a single character.
	fn atom(&mut self) -> Option<String> {
		self.nested(|lowering| {
			lowering.skip_whitespace();
			let mut out = String::new();
			match lowering.peek()? {
				'{' => {
					lowering.bump();
					lowering.sequence(&mut out, true)?;
				}
				'\\' => lowering.command(&mut out)?,
				'}' | '_' | '^' => return None,
				ch => {
					lowering.bump();
					push_plain_char(&mut out, ch)?;
				}
			}
			Some(out)
		})
	}

	fn command(&mut self, out: &mut String) -> Option<()> {
		self.bump();
		let rest: &'a str = self.rest;
		let name_len = rest.bytes().take_while(u8::is_ascii_alphabetic).count();
		if name_len == 0 {
			let text = control_symbol(self.bump()?)?;
			if text == " " {
				push_space(out);
			} else {
				out.push_str(text);
			}
			return Some(());
		}
		let (name, rest) = rest.split_at(name_len);
		self.rest = rest;

		if let Some(symbol) = symbol(name) {
			if symbol == " " {
				push_space(out);
			} else {
				out.push_str(symbol);
			}
			return Some(());
		}
		if let Some(alphabet) = Alphabet::for_command(name) {
			let argument = self.atom()?;
			out.extend(argument.chars().map(|ch| alphabet.map(ch)));
			return Some(());
		}
		match name {
			"text" | "textrm" | "mathrm" | "operatorname" => self.text_argument(out),
			// The delimiter after `\left` / `\right` is ordinary input; only the
			// invisible `.` has to go.
			"left" | "right" => {
				self.skip_whitespace();
				if self.peek() == Some('.') {
					self.bump();
				}
				Some(())
			}
			_ => None,
		}
	}

	/// The argument of `\text{…}`: literal text, spaces kept, no math inside.
	/// Iterative, so its brace depth needs no bound.
	fn text_argument(&mut self, out: &mut String) -> Option<()> {
		self.skip_whitespace();
		if self.bump()? != '{' {
			return None;
		}
		let mut depth = 0u32;
		loop {
			match self.bump()? {
				'{' => depth += 1,
				'}' if depth == 0 => return Some(()),
				'}' => depth -= 1,
				'\\' => match self.bump()? {
					ch @ ('{' | '}' | '$' | '%' | '&' | '_' | '#' | ' ') => out.push(ch),
					_ => return None,
				},
				'$' => return None,
				'~' => out.push(' '),
				ch => out.push(ch),
			}
		}
	}
}

/// Append a character that stands for itself in math, or its math reading.
/// The characters TeX reserves for structure this projection does not model
/// make the expression unsupported.
fn push_plain_char(out: &mut String, ch: char) -> Option<()> {
	match ch {
		'&' | '$' | '#' | '%' => return None,
		'-' => out.push('−'),
		'\'' => out.push('′'),
		'~' => push_space(out),
		ch => out.push(ch),
	}
	Some(())
}

/// Source whitespace and spacing commands collapse to one space.
fn push_space(out: &mut String) {
	if !out.ends_with(' ') {
		out.push(' ');
	}
}

/// `\,` and its kin: a backslash followed by one non-letter.
fn control_symbol(ch: char) -> Option<&'static str> {
	Some(match ch {
		',' | ':' | ';' | ' ' => " ",
		'!' => "",
		'{' => "{",
		'}' => "}",
		'$' => "$",
		'%' => "%",
		'&' => "&",
		'_' => "_",
		'#' => "#",
		'|' => "‖",
		_ => return None,
	})
}

/// Append a sub- or superscript. Unicode's script characters are used only
/// when the whole index has them, so an index never mixes raised and linear
/// characters; `K` has no subscript form, hence `⋉_K`.
fn push_index(out: &mut String, subscript: bool, index: &str) {
	if index.is_empty() {
		return;
	}
	// Marks that are already raised or conventionally written inline: `a†`,
	// `f′`, `A*`.
	if !subscript && matches!(index, "†" | "‡" | "′" | "″" | "‴" | "*") {
		out.push_str(index);
		return;
	}
	let script = if subscript {
		subscript_char
	} else {
		superscript_char
	};
	if let Some(mapped) = index.chars().map(script).collect::<Option<String>>() {
		out.push_str(&mapped);
		return;
	}
	out.push(if subscript { '_' } else { '^' });
	// A linear index of several tokens is parenthesized so it cannot merge
	// with what follows: `x_{i,j} y` is `x_(i,j) y`.
	if index.chars().count() <= 1 || index.chars().all(char::is_alphanumeric) {
		out.push_str(index);
	} else {
		out.push('(');
		out.push_str(index);
		out.push(')');
	}
}

fn subscript_char(ch: char) -> Option<char> {
	Some(match ch {
		'0'..='9' => char::from_u32(0x2080 + (ch as u32 - '0' as u32))?,
		'+' => '₊',
		'-' | '−' => '₋',
		'=' => '₌',
		'(' => '₍',
		')' => '₎',
		'a' => 'ₐ',
		'e' => 'ₑ',
		'h' => 'ₕ',
		'i' => 'ᵢ',
		'j' => 'ⱼ',
		'k' => 'ₖ',
		'l' => 'ₗ',
		'm' => 'ₘ',
		'n' => 'ₙ',
		'o' => 'ₒ',
		'p' => 'ₚ',
		'r' => 'ᵣ',
		's' => 'ₛ',
		't' => 'ₜ',
		'u' => 'ᵤ',
		'v' => 'ᵥ',
		'x' => 'ₓ',
		_ => return None,
	})
}

/// Superscripts stop at the characters ordinary text fonts carry. The
/// remaining raised letters are phonetic modifier letters that math and prose
/// fonts rarely cover, so `x^k` stays linear.
fn superscript_char(ch: char) -> Option<char> {
	Some(match ch {
		'0' => '⁰',
		'1' => '¹',
		'2' => '²',
		'3' => '³',
		'4' => '⁴',
		'5' => '⁵',
		'6' => '⁶',
		'7' => '⁷',
		'8' => '⁸',
		'9' => '⁹',
		'+' => '⁺',
		'-' | '−' => '⁻',
		'=' => '⁼',
		'(' => '⁽',
		')' => '⁾',
		'i' => 'ⁱ',
		'n' => 'ⁿ',
		_ => return None,
	})
}

/// A Unicode mathematical alphabet. Each is a contiguous run of code points
/// with holes where the letter had already been encoded in the Letterlike
/// Symbols block (`ℬ`, `ℝ`); those holes are unassigned, so the exceptions are
/// required rather than cosmetic.
#[derive(Clone, Copy)]
enum Alphabet {
	Bold,
	Script,
	DoubleStruck,
}

impl Alphabet {
	fn for_command(name: &str) -> Option<Self> {
		Some(match name {
			"mathbf" => Self::Bold,
			"mathcal" => Self::Script,
			"mathbb" => Self::DoubleStruck,
			_ => return None,
		})
	}

	/// The alphabet's form of `ch`, or `ch` itself where it has none.
	fn map(self, ch: char) -> char {
		let exception = match (self, ch) {
			(Self::Script, 'B') => 'ℬ',
			(Self::Script, 'E') => 'ℰ',
			(Self::Script, 'F') => 'ℱ',
			(Self::Script, 'H') => 'ℋ',
			(Self::Script, 'I') => 'ℐ',
			(Self::Script, 'L') => 'ℒ',
			(Self::Script, 'M') => 'ℳ',
			(Self::Script, 'R') => 'ℛ',
			(Self::Script, 'e') => 'ℯ',
			(Self::Script, 'g') => 'ℊ',
			(Self::Script, 'o') => 'ℴ',
			(Self::DoubleStruck, 'C') => 'ℂ',
			(Self::DoubleStruck, 'H') => 'ℍ',
			(Self::DoubleStruck, 'N') => 'ℕ',
			(Self::DoubleStruck, 'P') => 'ℙ',
			(Self::DoubleStruck, 'Q') => 'ℚ',
			(Self::DoubleStruck, 'R') => 'ℝ',
			(Self::DoubleStruck, 'Z') => 'ℤ',
			_ => '\0',
		};
		if exception != '\0' {
			return exception;
		}
		let (upper, lower, digit) = match self {
			Self::Bold => (0x1D400, 0x1D41A, Some(0x1D7CE)),
			Self::Script => (0x1D49C, 0x1D4B6, None),
			Self::DoubleStruck => (0x1D538, 0x1D552, Some(0x1D7D8)),
		};
		let code = match ch {
			'A'..='Z' => Some(upper + (ch as u32 - 'A' as u32)),
			'a'..='z' => Some(lower + (ch as u32 - 'a' as u32)),
			'0'..='9' => digit.map(|base| base + (ch as u32 - '0' as u32)),
			_ => None,
		};
		code.and_then(char::from_u32).unwrap_or(ch)
	}
}

/// Commands that take no argument and stand for fixed text.
fn symbol(name: &str) -> Option<&'static str> {
	Some(match name {
		// Greek.
		"alpha" => "α",
		"beta" => "β",
		"gamma" => "γ",
		"delta" => "δ",
		"epsilon" => "ϵ",
		"varepsilon" => "ε",
		"zeta" => "ζ",
		"eta" => "η",
		"theta" => "θ",
		"vartheta" => "ϑ",
		"iota" => "ι",
		"kappa" => "κ",
		"lambda" => "λ",
		"mu" => "μ",
		"nu" => "ν",
		"xi" => "ξ",
		"pi" => "π",
		"rho" => "ρ",
		"sigma" => "σ",
		"tau" => "τ",
		"upsilon" => "υ",
		"phi" => "ϕ",
		"varphi" => "φ",
		"chi" => "χ",
		"psi" => "ψ",
		"omega" => "ω",
		"Gamma" => "Γ",
		"Delta" => "Δ",
		"Theta" => "Θ",
		"Lambda" => "Λ",
		"Xi" => "Ξ",
		"Pi" => "Π",
		"Sigma" => "Σ",
		"Upsilon" => "Υ",
		"Phi" => "Φ",
		"Psi" => "Ψ",
		"Omega" => "Ω",
		// Letter-like.
		"infty" => "∞",
		"ell" => "ℓ",
		"partial" => "∂",
		"nabla" => "∇",
		"emptyset" | "varnothing" => "∅",
		"prime" => "′",
		"dagger" => "†",
		"top" => "⊤",
		"bot" | "perp" => "⊥",
		// Binary operators.
		"times" => "×",
		"ltimes" => "⋉",
		"rtimes" => "⋊",
		"bowtie" => "⋈",
		"otimes" => "⊗",
		"oplus" => "⊕",
		"cdot" => "·",
		"circ" => "∘",
		"pm" => "±",
		"cup" => "∪",
		"cap" => "∩",
		"uplus" => "⊎",
		"setminus" => "∖",
		"land" | "wedge" => "∧",
		"lor" | "vee" => "∨",
		"lnot" | "neg" => "¬",
		// Relations.
		"le" | "leq" => "≤",
		"ge" | "geq" => "≥",
		"ne" | "neq" => "≠",
		"approx" => "≈",
		"equiv" => "≡",
		"sim" => "∼",
		"cong" => "≅",
		"in" => "∈",
		"notin" => "∉",
		"subset" => "⊂",
		"subseteq" => "⊆",
		"supset" => "⊃",
		"supseteq" => "⊇",
		"mid" => "∣",
		"vdash" => "⊢",
		"models" => "⊨",
		// Arrows.
		"to" | "rightarrow" => "→",
		"leftarrow" => "←",
		"leftrightarrow" => "↔",
		"Rightarrow" | "implies" => "⇒",
		"Leftarrow" => "⇐",
		"Leftrightarrow" | "iff" => "⇔",
		"mapsto" => "↦",
		// Quantifiers and large operators.
		"forall" => "∀",
		"exists" => "∃",
		"nexists" => "∄",
		"sum" => "∑",
		"prod" => "∏",
		"int" => "∫",
		// Delimiters.
		"langle" => "⟨",
		"rangle" => "⟩",
		"lfloor" => "⌊",
		"rfloor" => "⌋",
		"lceil" => "⌈",
		"rceil" => "⌉",
		// Dots, spaces and punctuation.
		"ldots" | "dots" => "…",
		"cdots" => "⋯",
		"quad" | "qquad" => " ",
		// Named functions print upright as themselves.
		"det" => "det",
		"dim" => "dim",
		"exp" => "exp",
		"inf" => "inf",
		"ker" => "ker",
		"lim" => "lim",
		"log" => "log",
		"max" => "max",
		"min" => "min",
		"sup" => "sup",
		_ => return None,
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	fn math(tex: &str) -> String {
		render_math(tex).unwrap_or_else(|| panic!("`{tex}` is in the supported subset"))
	}

	/// Every math span in toron brainstorm 011, the document that showed the
	/// preview could not read math at all.
	#[test]
	fn brainstorm_011_expressions_render() {
		for (tex, expected) in [
			(
				r"\mathcal{B}_{\text{weak}} \ltimes_K \mathcal{B}_{\text{strong}}",
				"ℬ_weak ⋉_K ℬ_strong",
			),
			(r"\Omega_{\text{strong}}", "Ω_strong"),
			(r"\Omega_{\text{weak}}", "Ω_weak"),
			(r"\mathcal{B}_{\text{strong}}", "ℬ_strong"),
			(r"(B, K]", "(B, K]"),
			(r"> K", "> K"),
			(r"\infty", "∞"),
			(r"L", "L"),
			(
				r"(W \cup M) \cap R_{\text{arcs}} = \emptyset",
				"(W ∪ M) ∩ R_arcs = ∅",
			),
			(r"W \cap W_{\text{arcs}} = \emptyset", "W ∩ W_arcs = ∅"),
			(r"(K, L)", "(K, L)"),
			(r"\to", "→"),
			(r"B \le K", "B ≤ K"),
			(r"\iota", "ι"),
			(r"m \in \mathcal{M}_{\text{outbox}}", "m ∈ ℳ_outbox"),
			(r"K_i \ge R", "Kᵢ ≥ R"),
			(r"\iota \in \text{CallId}", "ι ∈ CallId"),
			(r"\mathbf{1} \otimes a^\dagger_m", "𝟏 ⊗ a†ₘ"),
		] {
			assert_eq!(math(tex), expected, "{tex}");
		}
	}

	/// Notation the repository's other plans use (wendelon brainstorms 014
	/// and 021, toron impl 0011).
	#[test]
	fn other_plan_expressions_render() {
		for (tex, expected) in [
			(r"A \cdot B \neq B \cdot A", "A · B ≠ B · A"),
			(r"\delta_\iota \oplus \omega", "δ_ι ⊕ ω"),
			(r"\sum_{i} x_i \equiv 0", "∑ᵢ xᵢ ≡ 0"),
			(r"\ker f \cong \mathbb{Z}", "ker f ≅ ℤ"),
			(r"\left( a \mid b \right)", "( a ∣ b )"),
			(
				r"A \setminus B \subseteq C \iff \forall x",
				"A ∖ B ⊆ C ⇔ ∀ x",
			),
			(r"O(1)", "O(1)"),
			(r"k=200", "k=200"),
		] {
			assert_eq!(math(tex), expected, "{tex}");
		}
	}

	#[test]
	fn an_index_is_raised_only_when_every_character_can_be() {
		assert_eq!(math("x_1"), "x₁");
		assert_eq!(math("x_{i+1}"), "xᵢ₊₁");
		assert_eq!(math("x^2"), "x²");
		assert_eq!(math("x^{-1}"), "x⁻¹");
		assert_eq!(math("2^{n}"), "2ⁿ");
		// `K` has no subscript form and `k` no dependable superscript.
		assert_eq!(math("x_K"), "x_K");
		assert_eq!(math("x^k"), "x^k");
		// One unavailable character keeps the whole index linear.
		assert_eq!(math("x_{iK}"), "x_iK");
		assert_eq!(math("x_{i,j}"), "x_(i,j)");
		assert_eq!(math("e^{i \\pi}"), "e^(i π)");
		assert_eq!(math("x_{}"), "x");
	}

	#[test]
	fn an_index_takes_one_argument() {
		assert_eq!(math("x_ab"), "xₐb");
		assert_eq!(math(r"x_\alpha"), "x_α");
		assert_eq!(math(r"A^\top"), "A^⊤");
		assert_eq!(math(r"f'"), "f′");
		assert_eq!(math(r"A^*"), "A*");
		assert_eq!(math(r"X_\mathcal{B}"), "X_ℬ");
	}

	#[test]
	fn alphabets_use_the_letterlike_exceptions() {
		assert_eq!(math(r"\mathcal{ABCE}"), "𝒜ℬ𝒞ℰ");
		assert_eq!(math(r"\mathbb{R}^n"), "ℝⁿ");
		assert_eq!(math(r"\mathbb{NZQk}"), "ℕℤℚ𝕜");
		assert_eq!(math(r"\mathbf{x0}"), "𝐱𝟎");
		// A character the alphabet lacks stays as written.
		assert_eq!(math(r"\mathcal{B+1}"), "ℬ+1");
		assert_eq!(math(r"\mathcal B"), "ℬ");
	}

	#[test]
	fn every_alphabet_letter_is_an_assigned_character() {
		for alphabet in [Alphabet::Bold, Alphabet::Script, Alphabet::DoubleStruck] {
			let mut seen = std::collections::HashSet::new();
			for ch in ('A'..='Z').chain('a'..='z') {
				let mapped = alphabet.map(ch);
				assert_ne!(mapped, ch, "{ch} has a form");
				assert!(mapped.is_alphabetic(), "{ch} maps to a letter: {mapped:?}");
				assert!(seen.insert(mapped), "{ch} maps to its own character");
			}
		}
	}

	#[test]
	fn spacing_commands_leave_no_residue() {
		assert_eq!(math(r"a\,b"), "a b");
		assert_eq!(math(r"a \quad  \; b"), "a b");
		assert_eq!(math(r"a\!b"), "ab");
		assert_eq!(math(r"\left( x \right)"), "( x )");
		assert_eq!(math(r"\left. x \right|"), "x |");
		assert_eq!(math(r"\langle x \rangle"), "⟨ x ⟩");
		assert_eq!(math("  a   +\n b  "), "a + b");
		assert_eq!(math("a - b"), "a − b");
	}

	/// The parser recurses per nesting level over arbitrary file content, so
	/// depth is bounded on every recursive path. Each shape renders at the
	/// bound, which shows the refusal one level later is the bound and not a
	/// malformed input.
	#[test]
	fn nesting_is_bounded_on_every_recursive_path() {
		let nest = |open: &str, innermost: &str, close: &str, levels: usize| {
			format!("{}{innermost}{}", open.repeat(levels), close.repeat(levels))
		};
		let limit = MAX_DEPTH as usize;
		for (open, innermost, close, at_limit) in [
			// Groups.
			("{", "x", "}", Some("x")),
			// Braced arguments.
			(r"\mathbf{", "x", "}", Some("𝐱")),
			// Commands taking commands with no braces at all.
			(r"\mathbf", " x", "", Some("𝐱")),
			// Index arguments; the rendering is long and beside the point.
			("x_{", "y", "}", None),
		] {
			let rendered = render_math(&nest(open, innermost, close, limit));
			assert!(rendered.is_some(), "{open} renders at the bound");
			if let Some(expected) = at_limit {
				assert_eq!(rendered.as_deref(), Some(expected), "{open}");
			}
			for levels in [limit + 1, 10_000] {
				assert_eq!(
					render_math(&nest(open, innermost, close, levels)),
					None,
					"{open} at {levels} levels"
				);
			}
		}
		// Sibling groups do not accumulate depth.
		assert_eq!(math(&"{x}".repeat(100)), "x".repeat(100));
	}

	#[test]
	fn text_is_literal() {
		assert_eq!(math(r"\text{if } x \in S"), "if x ∈ S");
		assert_eq!(math(r"\text{snake\_case a-b}"), "snake_case a-b");
		assert_eq!(math(r"\text{a {b} c}"), "a b c");
		assert_eq!(math(r"\operatorname{rank} A"), "rank A");
		assert_eq!(math(r"\{a, b\}"), "{a, b}");
		assert_eq!(math(r"\log n"), "log n");
		assert_eq!(math(r"\mathrm{d}x"), "dx");
	}

	#[test]
	fn the_unsupported_and_the_malformed_are_refused_whole() {
		for tex in [
			r"\unknown{x}",
			r"x + \unknown",
			r"\begin{matrix} a \end{matrix}",
			r"a & b",
			r"a \\ b",
			r"\bar{P}",
			r"\hat x",
			r"\sqrt{x}",
			r"\frac{a}{b}",
			r"a\frac{b}{c}d",
			r"\bigl( x \bigr)",
			r"\mathfrak{g}",
			r"\text{a $x$ b}",
			r"\text{\alpha}",
			r"\text x",
			"{a",
			"a}",
			"x_",
			"x^}",
			r"\frac{a}",
			r"\mathcal",
			"\\",
			"",
			"  ",
			"{}",
			r"\!",
		] {
			assert_eq!(render_math(tex), None, "{tex:?}");
		}
	}
}
