//! The `SIMILAR TO` pattern compiler and matcher (ADR 570).
//!
//! The closed vocabulary — `%`, `_`, `[...]`/`[^...]` with ranges, `|`,
//! `(...)`, `*`, `+`, `?`, `{n}`, `{n,}`, `{n,m}`, with a single-character
//! `ESCAPE` — compiles at bind into a Thompson-style NFA, and matching
//! simulates a state set over the input's characters. The simulation never
//! backtracks: worst case is `O(states × input length)`, so no pattern can
//! exhibit catastrophic runtime. The match is anchored to the whole value.

/// The compiled program's state cap. Bounded repetition expands by
/// duplication, so automaton size is a memory-and-time commitment; a pattern
/// that expands past this cap is rejected at bind rather than admitted as an
/// oversized resident program.
pub(crate) const SIMILAR_TO_MAX_STATES: usize = 4_096;

#[derive(Debug, Clone, PartialEq)]
enum CharTest {
	Literal(char),
	/// `_` — any single character.
	Any,
	Class {
		negated: bool,
		singles: Vec<char>,
		ranges: Vec<(char, char)>,
	},
}

impl CharTest {
	fn matches(&self, candidate: char) -> bool {
		match self {
			Self::Literal(expected) => candidate == *expected,
			Self::Any => true,
			Self::Class {
				negated,
				singles,
				ranges,
			} => {
				let inside = singles.contains(&candidate)
					|| ranges
						.iter()
						.any(|(low, high)| (*low..=*high).contains(&candidate));
				inside != *negated
			}
		}
	}
}

#[derive(Debug, Clone)]
enum State {
	Consume { test: CharTest, next: usize },
	Split { first: usize, second: usize },
	Match,
}

/// A compiled, anchored `SIMILAR TO` program.
#[derive(Debug, Clone)]
pub(crate) struct SimilarToProgram {
	states: Vec<State>,
	start: usize,
}

impl SimilarToProgram {
	/// Whole-value match over characters. State-set simulation: no
	/// backtracking, no recursion over pattern positions.
	pub(crate) fn matches(&self, input: &str) -> bool {
		let mut current = vec![false; self.states.len()];
		self.add_state(self.start, &mut current);
		for candidate in input.chars() {
			let mut next = vec![false; self.states.len()];
			let mut advanced = false;
			for (id, active) in current.iter().enumerate() {
				if !active {
					continue;
				}
				if let State::Consume { test, next: to } = &self.states[id]
					&& test.matches(candidate)
				{
					self.add_state(*to, &mut next);
					advanced = true;
				}
			}
			if !advanced {
				return false;
			}
			current = next;
		}
		current
			.iter()
			.enumerate()
			.any(|(id, active)| *active && matches!(self.states[id], State::Match))
	}

	/// Epsilon closure through `Split` states, iteratively — an explicit
	/// stack, so pattern nesting can never overflow the call stack.
	fn add_state(&self, id: usize, set: &mut [bool]) {
		let mut stack = vec![id];
		while let Some(id) = stack.pop() {
			if set[id] {
				continue;
			}
			set[id] = true;
			if let State::Split { first, second } = self.states[id] {
				stack.push(first);
				stack.push(second);
			}
		}
	}
}

/// A pattern element after parsing, before NFA construction.
#[derive(Debug, Clone)]
enum Ast {
	/// Ordered alternatives.
	Alternation(Vec<Ast>),
	Sequence(Vec<Ast>),
	Test(CharTest),
	/// `%` — zero or more of any character.
	AnyString,
	Repeat {
		atom: Box<Ast>,
		min: u32,
		/// `None` is unbounded (`{n,}`, `*`, `+`).
		max: Option<u32>,
	},
}

struct Parser {
	chars: Vec<char>,
	at: usize,
	escape: Option<char>,
}

impl Parser {
	fn error(&self, message: &str) -> String {
		format!("{message} at position {}", self.at + 1)
	}

	fn peek(&self) -> Option<char> {
		self.chars.get(self.at).copied()
	}

	fn bump(&mut self) -> Option<char> {
		let ch = self.peek();
		if ch.is_some() {
			self.at += 1;
		}
		ch
	}

	fn parse_alternation(&mut self) -> Result<Ast, String> {
		let mut branches = vec![self.parse_sequence()?];
		while self.peek() == Some('|') {
			self.bump();
			branches.push(self.parse_sequence()?);
		}
		if branches.len() == 1 {
			return Ok(branches.pop().expect("one branch"));
		}
		Ok(Ast::Alternation(branches))
	}

	fn parse_sequence(&mut self) -> Result<Ast, String> {
		let mut elements = Vec::new();
		while let Some(ch) = self.peek() {
			if ch == '|' || ch == ')' {
				break;
			}
			elements.push(self.parse_repeat()?);
		}
		Ok(Ast::Sequence(elements))
	}

	fn parse_repeat(&mut self) -> Result<Ast, String> {
		let atom = self.parse_atom()?;
		let (min, max) = match self.peek() {
			Some('*') => {
				self.bump();
				(0, None)
			}
			Some('+') => {
				self.bump();
				(1, None)
			}
			Some('?') => {
				self.bump();
				(0, Some(1))
			}
			Some('{') => {
				self.bump();
				self.parse_bounds()?
			}
			_ => return Ok(atom),
		};
		if matches!(atom, Ast::AnyString) {
			// `%` already means "any string"; repeating it is a mistake worth
			// naming rather than normalizing away.
			return Err(self.error("`%` does not accept a repetition operator"));
		}
		Ok(Ast::Repeat {
			atom: Box::new(atom),
			min,
			max,
		})
	}

	fn parse_bounds(&mut self) -> Result<(u32, Option<u32>), String> {
		let min = self.parse_count()?;
		match self.bump() {
			Some('}') => Ok((min, Some(min))),
			Some(',') => {
				if self.peek() == Some('}') {
					self.bump();
					return Ok((min, None));
				}
				let max = self.parse_count()?;
				if self.bump() != Some('}') {
					return Err(self.error("repetition bounds must close with `}`"));
				}
				if max < min {
					return Err(self.error("repetition maximum is below its minimum"));
				}
				Ok((min, Some(max)))
			}
			_ => Err(self.error("repetition bounds must close with `}`")),
		}
	}

	fn parse_count(&mut self) -> Result<u32, String> {
		let mut digits = String::new();
		while let Some(ch) = self.peek() {
			if !ch.is_ascii_digit() {
				break;
			}
			digits.push(ch);
			self.bump();
		}
		if digits.is_empty() {
			return Err(self.error("repetition bounds require a number"));
		}
		digits
			.parse::<u32>()
			.map_err(|_| self.error("repetition bound is too large"))
	}

	fn parse_atom(&mut self) -> Result<Ast, String> {
		let Some(ch) = self.bump() else {
			return Err(self.error("expected a pattern element"));
		};
		if self.escape == Some(ch) {
			let Some(literal) = self.bump() else {
				return Err(self.error("the escape character must be followed by a character"));
			};
			return Ok(Ast::Test(CharTest::Literal(literal)));
		}
		match ch {
			'%' => Ok(Ast::AnyString),
			'_' => Ok(Ast::Test(CharTest::Any)),
			'(' => {
				let inner = self.parse_alternation()?;
				if self.bump() != Some(')') {
					return Err(self.error("a group must close with `)`"));
				}
				Ok(inner)
			}
			'[' => self.parse_class(),
			'*' | '+' | '?' | '{' => Err(self.error("a repetition operator needs an element")),
			')' | '|' => unreachable!("sequence stops before delimiters"),
			literal => Ok(Ast::Test(CharTest::Literal(literal))),
		}
	}

	fn parse_class(&mut self) -> Result<Ast, String> {
		if self.peek() == Some('[') && self.chars.get(self.at + 1) == Some(&':') {
			return Err(self.error(
				"POSIX named classes are not supported; write the characters or ranges directly",
			));
		}
		let negated = if self.peek() == Some('^') {
			self.bump();
			true
		} else {
			false
		};
		let mut singles = Vec::new();
		let mut ranges = Vec::new();
		let mut pending: Option<char> = None;
		loop {
			let Some(ch) = self.bump() else {
				return Err(self.error("a character class must close with `]`"));
			};
			if ch == ']' {
				if let Some(single) = pending.take() {
					singles.push(single);
				}
				if singles.is_empty() && ranges.is_empty() {
					return Err(self.error("a character class cannot be empty"));
				}
				return Ok(Ast::Test(CharTest::Class {
					negated,
					singles,
					ranges,
				}));
			}
			let ch = if self.escape == Some(ch) {
				self.bump().ok_or_else(|| {
					self.error("the escape character must be followed by a character")
				})?
			} else if ch == '[' && self.peek() == Some(':') {
				return Err(self.error(
					"POSIX named classes are not supported; write the characters or ranges directly",
				));
			} else {
				ch
			};
			if ch == '-'
				&& let Some(low) = pending.take()
			{
				let Some(high) = self.bump() else {
					return Err(self.error("a character class must close with `]`"));
				};
				if high == ']' {
					// A trailing `-` is a literal.
					singles.push(low);
					singles.push('-');
					if singles.is_empty() && ranges.is_empty() {
						return Err(self.error("a character class cannot be empty"));
					}
					return Ok(Ast::Test(CharTest::Class {
						negated,
						singles,
						ranges,
					}));
				}
				if high < low {
					return Err(self.error("a character range runs backwards"));
				}
				ranges.push((low, high));
				continue;
			}
			if let Some(single) = pending.take() {
				singles.push(single);
			}
			pending = Some(ch);
		}
	}
}

struct Builder {
	states: Vec<State>,
}

impl Builder {
	fn push(&mut self, state: State) -> Result<usize, String> {
		if self.states.len() >= SIMILAR_TO_MAX_STATES {
			return Err(format!(
				"pattern expands past the {SIMILAR_TO_MAX_STATES}-state cap; \
				 reduce repetition bounds or simplify the pattern"
			));
		}
		self.states.push(state);
		Ok(self.states.len() - 1)
	}

	/// Compiles `ast` into states ending at `next`, returning the entry state.
	fn compile(&mut self, ast: &Ast, next: usize) -> Result<usize, String> {
		match ast {
			Ast::Test(test) => self.push(State::Consume {
				test: test.clone(),
				next,
			}),
			Ast::AnyString => {
				// `%` is a self-loop over any character, skippable entirely.
				let split = self.push(State::Split {
					first: next,
					second: next,
				})?;
				let consume = self.push(State::Consume {
					test: CharTest::Any,
					next: split,
				})?;
				self.states[split] = State::Split {
					first: consume,
					second: next,
				};
				Ok(split)
			}
			Ast::Sequence(elements) => {
				let mut entry = next;
				for element in elements.iter().rev() {
					entry = self.compile(element, entry)?;
				}
				Ok(entry)
			}
			Ast::Alternation(branches) => {
				let mut entry = self.compile(&branches[branches.len() - 1], next)?;
				for branch in branches[..branches.len() - 1].iter().rev() {
					let first = self.compile(branch, next)?;
					entry = self.push(State::Split {
						first,
						second: entry,
					})?;
				}
				Ok(entry)
			}
			Ast::Repeat { atom, min, max } => {
				// Tail first: the optional/unbounded region, then the required
				// prefix, compiled right to left so each copy chains forward.
				let mut entry = match max {
					None => {
						let split = self.push(State::Split {
							first: next,
							second: next,
						})?;
						let body = self.compile(atom, split)?;
						self.states[split] = State::Split {
							first: body,
							second: next,
						};
						split
					}
					Some(max) => {
						let mut entry = next;
						for _ in *min..*max {
							let body = self.compile(atom, entry)?;
							entry = self.push(State::Split {
								first: body,
								second: next,
							})?;
						}
						entry
					}
				};
				for _ in 0..*min {
					entry = self.compile(atom, entry)?;
				}
				Ok(entry)
			}
		}
	}
}

/// Compiles a `SIMILAR TO` pattern. Errors carry the 1-based position.
pub(crate) fn compile_similar_to(
	pattern: &str,
	escape: Option<char>,
) -> Result<SimilarToProgram, String> {
	if let Some(escape) = escape
		&& "%_[]()|*+?{}".contains(escape)
	{
		return Err(format!(
			"the escape character {escape:?} is part of the pattern vocabulary"
		));
	}
	let mut parser = Parser {
		chars: pattern.chars().collect(),
		at: 0,
		escape,
	};
	let ast = parser.parse_alternation()?;
	if parser.at < parser.chars.len() {
		return Err(parser.error("unexpected `)`"));
	}
	let mut builder = Builder { states: Vec::new() };
	let matched = builder.push(State::Match)?;
	let start = builder.compile(&ast, matched)?;
	Ok(SimilarToProgram {
		states: builder.states,
		start,
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	fn matched(pattern: &str, input: &str) -> bool {
		compile_similar_to(pattern, None)
			.unwrap_or_else(|error| panic!("{pattern:?}: {error}"))
			.matches(input)
	}

	#[test]
	fn vocabulary_operators_alone_and_composed() {
		assert!(matched("abc", "abc"));
		assert!(!matched("abc", "abcd"), "anchored at the end");
		assert!(!matched("abc", "xabc"), "anchored at the start");
		assert!(matched("%", ""));
		assert!(matched("a%c", "abbbc"));
		assert!(matched("a_c", "abc"));
		assert!(!matched("a_c", "ac"), "`_` requires one character");
		assert!(matched("a[bc]d", "acd"));
		assert!(!matched("a[bc]d", "aed"));
		assert!(matched("a[^bc]d", "aed"));
		assert!(matched("[a-f]{3}", "bed"));
		assert!(!matched("[a-f]{3}", "bez"));
		assert!(matched("cat|dog", "dog"));
		assert!(!matched("cat|dog", "catdog"), "alternation is anchored too");
		assert!(matched("(ab)+", "ababab"));
		assert!(!matched("(ab)+", "ababa"));
		assert!(matched("ab?c", "ac"));
		assert!(matched("a*", ""));
		assert!(matched("a{2,3}", "aaa"));
		assert!(!matched("a{2,3}", "aaaa"));
		assert!(matched("a{2,}", "aaaaaa"));
		assert!(!matched("a{2,}", "a"));
		assert!(matched("(a|b){2}c%", "bac and more"));
		assert!(matched("日本%", "日本語"), "characters, not bytes");
	}

	#[test]
	fn escape_makes_vocabulary_literal() {
		let program = compile_similar_to("100!%", Some('!')).expect("escaped percent");
		assert!(program.matches("100%"));
		assert!(!program.matches("1000"));
		let doubled = compile_similar_to("a!!b", Some('!')).expect("escaped escape");
		assert!(doubled.matches("a!b"));
		let class = compile_similar_to("[!]-]", Some('!')).expect("escaped in class");
		assert!(class.matches("]"));
		assert!(class.matches("-"));
	}

	#[test]
	fn malformed_patterns_fail_with_positions() {
		for (pattern, fragment) in [
			("[abc", "must close with `]`"),
			("a{", "require a number"),
			("a{3,1}", "below its minimum"),
			("*a", "needs an element"),
			("(ab", "must close with `)`"),
			("ab)", "unexpected `)`"),
			("[]", "cannot be empty"),
			("[z-a]", "runs backwards"),
			("%*", "does not accept a repetition"),
			("[[:alpha:]]", "POSIX named classes are not supported"),
		] {
			let error =
				compile_similar_to(pattern, None).expect_err(&format!("{pattern:?} must fail"));
			assert!(
				error.contains(fragment),
				"{pattern:?}: expected {fragment:?} in {error:?}"
			);
			assert!(error.contains("position"), "{pattern:?}: {error:?}");
		}
		let escape = compile_similar_to("a", Some('%')).expect_err("vocab escape");
		assert!(escape.contains("part of the pattern vocabulary"));
	}

	#[test]
	fn nested_repetition_cannot_backtrack() {
		// The classical backtracking bomb: `(a*)*b` against a long run of
		// `a` with no `b`. A backtracking matcher explores 2^n paths; the
		// state-set simulation answers in linear time. The generous bound
		// exists to make the property falsifiable, not to measure speed.
		let program = compile_similar_to("(a*)*b", None).expect("nested repetition compiles");
		let input = "a".repeat(4_096);
		let started = std::time::Instant::now();
		assert!(!program.matches(&input));
		assert!(program.matches(&format!("{input}b")));
		assert!(
			started.elapsed() < std::time::Duration::from_secs(5),
			"the non-backtracking bound was exceeded: {:?}",
			started.elapsed()
		);
	}

	#[test]
	fn the_state_cap_bounds_expansion() {
		assert!(
			compile_similar_to("a{2000}", None).is_ok(),
			"a pattern under the cap compiles"
		);
		let over = compile_similar_to("a{5000}", None).expect_err("over the cap");
		assert!(over.contains("state cap"), "{over}");
		let nested_blowup =
			compile_similar_to("(a{100}){100}", None).expect_err("multiplicative expansion");
		assert!(nested_blowup.contains("state cap"), "{nested_blowup}");
	}
}
