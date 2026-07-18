// The Language enum and SvpPipeline dispatcher that previously lived here
// have been replaced by the LanguageProvider trait and LanguageRegistry
// in engine::language. Language-specific highlighting is now handled by
// provider structs in svp::{rust,python,rune}_provider.
//
// This module is intentionally left minimal. The remaining public symbols
// are preserved only to ease migration of downstream test code.
